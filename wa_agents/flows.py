"""
WhatsApp Flows endpoint service, encryption, and key provisioning helpers.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import inspect
import json
import logging
import time

from dotenv import (
    find_dotenv,
    load_dotenv,
)
from datetime import (
    UTC,
    datetime,
)
from typing import (
    Any,
    Literal,
)

import httpx

from cryptography.hazmat.primitives import (
    hashes,
    serialization,
)
from cryptography.hazmat.primitives.asymmetric import (
    padding,
    rsa,
)
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import status
from fastapi.responses import PlainTextResponse
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
)
from sofia_utils.pydantic import (
    NO_WS_str,
    NumericID,
)

from .io_functions import (
    API_URL,
    write_headers,
)
from .io_models import (
    WhatsApp_IB_Encrypted_FlowRequest,
    WhatsApp_IB_Decrypted_FlowRequest,
    WhatsApp_OB_FlowResponse,
)
from .supabase import (
    AsyncSupabaseStorage,
    SyncSupabaseStorage,
)


# =========================================================================================
# INTERNAL HANDLER MODELS

class WhatsApp_Local_FlowContext (BaseModel) :
    """
    Persisted routing context supplied to an application Flow handler
        `flow_waba_id` : Database ID of the Flow WABA configuration
        `session_id`   : Database ID of the active Flow session
        `flow_id`      : Meta Flow ID
        `flow_key`     : Stable application Flow key
        `business_id`  : Database ID of the WhatsApp business
        `contact_id`   : Database ID of the WhatsApp contact
        `state`        : Decrypted application state from the previous exchange
    """
    model_config = ConfigDict( frozen = True)
    
    flow_waba_id : int
    session_id   : int
    flow_id      : NumericID
    flow_key     : NO_WS_str
    business_id  : int
    contact_id   : int
    state        : dict[ str, Any] = Field( default_factory = dict)


class WhatsApp_Local_FlowHandlerResult (BaseModel) :
    """
    Result returned by an application Flow handler
        `response`       : Logical response for the Flow client
        `state`          : Application state to encrypt for the next exchange | null
        `session_status` : Resulting Flow session status
    """
    model_config = ConfigDict( frozen = True)
    
    response       : WhatsApp_OB_FlowResponse
    state          : dict[str, Any] | None = None
    session_status : Literal[ "active", "completed", "failed"] = "active"


def flow_token_hash( flow_token : NO_WS_str) -> str :
    """
    Return the SHA-256 digest used to look up a Flow session token
    """
    return hashlib.sha256(flow_token.encode("utf-8")).hexdigest()


def canonical_flow_json( value : dict[str, Any] | BaseModel) -> bytes :
    """
    Serialize a Flow payload to deterministic UTF-8 JSON bytes
    """
    if isinstance( value, BaseModel) :
        value = value.model_dump( mode = "json", exclude_none = True)
    return json.dumps(
        value,
        ensure_ascii = False,
        separators   = ( ",", ":"),
        sort_keys    = True,
    ).encode("utf-8")


def decrypt_flow_request(
    envelope       : WhatsApp_IB_Encrypted_FlowRequest,
    private_key_pem: str | bytes,
) -> tuple[ WhatsApp_IB_Decrypted_FlowRequest, bytes, bytes] :
    """
    Decrypt and validate Meta's RSA-OAEP/AES-GCM request envelope \\
    Args:
        envelope        : Encrypted transport fields received from Meta
        private_key_pem : WABA private key used to decrypt the ephemeral AES key
    Returns:
        Decrypted request, ephemeral AES key, and request initialization vector
    """
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode("utf-8")
        if isinstance( private_key_pem, str) else private_key_pem,
        password = None,
    )
    aes_key = private_key.decrypt(
        base64.b64decode(envelope.encrypted_aes_key),
        padding.OAEP(
            mgf       = padding.MGF1( algorithm = hashes.SHA256()),
            algorithm = hashes.SHA256(),
            label     = None,
        ),
    )
    iv         = base64.b64decode(envelope.initial_vector)
    ciphertext = base64.b64decode(envelope.encrypted_flow_data)
    plaintext  = AESGCM(aes_key).decrypt( iv, ciphertext, None)
    request    = WhatsApp_IB_Decrypted_FlowRequest.model_validate_json(plaintext)
    
    return request, aes_key, iv


def encrypt_flow_response(
    response : WhatsApp_OB_FlowResponse | dict[str, Any],
    aes_key  : bytes,
    request_iv: bytes,
) -> str :
    """
    Encrypt a logical response using Meta's bitwise-inverted request IV \\
    Args:
        response   : Logical response for the Flow client
        aes_key    : Ephemeral AES key recovered from the request
        request_iv : Initialization vector recovered from the request
    Returns:
        Base64-encoded AES-GCM ciphertext and authentication tag
    """
    response_iv = bytes( byte ^ 0xFF for byte in request_iv)
    ciphertext  = AESGCM(aes_key).encrypt(
        response_iv,
        canonical_flow_json(response),
        None,
    )
    return base64.b64encode(ciphertext).decode("ascii")


# =========================================================================================
# DATA ENDPOINT SERVICE

class WhatsAppFlowEndpoint :
    """
    Decrypt, route, audit, and encrypt Flow data-exchange requests
    """
    
    def __init__(
        self,
        storage         : AsyncSupabaseStorage,
        handler_classes : dict[str, type[Any]],
    ) -> None :
        """
        Initialize the Flow endpoint service \\
        Args:
            storage         : Async persistence gateway
            handler_classes : Case handlers keyed by their registered handler key
        """
        self.storage         = storage
        self.handler_classes = handler_classes
    
    @staticmethod
    def _plain( body : str, status_code : int = 200) -> PlainTextResponse :
        """
        Return a plain-text Flow endpoint response
        """
        return PlainTextResponse(
            content     = body,
            status_code = status_code,
            media_type  = "text/plain",
        )
    
    async def handle(
        self,
        waba_id : str,
        payload : dict[str, Any],
    ) -> PlainTextResponse :
        """
        Process one encrypted Flow data-exchange request \\
        Args:
            waba_id : WABA whose private key decrypts the request
            payload : Encrypted request envelope received from Meta
        Returns:
            Encrypted success response or plain-text transport error
        """
        started   = time.perf_counter()
        flow_waba = await self.storage.get_flow_waba(waba_id)
        if not flow_waba :
            return self._plain( "Unknown WABA", status.HTTP_404_NOT_FOUND)
        
        try :
            envelope = WhatsApp_IB_Encrypted_FlowRequest.model_validate(payload)
            request, aes_key, request_iv = decrypt_flow_request(
                envelope,
                flow_waba["private_key"],
            )
        
        except Exception :
            logging.exception("Unable to decrypt WhatsApp Flow request")
            return self._plain(
                "Unable to decrypt request",
                status.HTTP_421_MISDIRECTED_REQUEST,
            )
        
        if request.action == "ping" :
            response     = WhatsApp_OB_FlowResponse.health_check()
            request_data = request.model_dump( mode = "json", exclude_none = True)
            exchange = await self.storage.insert_flow_exchange(
                flow_waba      = flow_waba["id"],
                request_hash   = flow_token_hash(
                    canonical_flow_json(request_data).decode("utf-8")
                ),
                request_action = request.action,
                request_data   = request_data,
            )
            if exchange :
                await self.storage.finish_flow_exchange(
                    exchange["id"],
                    response_data = response.model_dump(
                        mode         = "json",
                        exclude_none = True,
                    ),
                    exchange_status = "succeeded",
                    http_status     = 200,
                    latency_ms      = round(
                        ( time.perf_counter() - started) * 1000
                    ),
                )
            return self._plain(
                encrypt_flow_response( response, aes_key, request_iv)
            )
        
        if not request.flow_token :
            return self._plain( "Missing flow token", 432)
        
        token_hash = flow_token_hash(request.flow_token)
        session    = await self.storage.get_flow_session(token_hash)
        if not session :
            return self._plain( "Invalid flow token", 432)
        if (
            session["session_status"] in { "completed", "failed", "expired",
                                           "superseded" } or
            session["expires_at"] <= datetime.now(UTC)
        ) :
            if session["expires_at"] <= datetime.now(UTC) :
                await self.storage.update_flow_session( session["id"], "expired")
            return self._plain( "Expired flow token", 432)
        
        request_data = request.model_dump( mode = "json", exclude_none = True)
        request_hash = flow_token_hash(
            canonical_flow_json(request_data).decode("utf-8")
        )
        cached = await self.storage.get_flow_exchange(
            session["id"],
            request_hash,
        )
        if cached and cached.get("response_data_decrypted") :
            response = WhatsApp_OB_FlowResponse.model_validate(
                cached["response_data_decrypted"]
            )
            return self._plain(
                encrypt_flow_response( response, aes_key, request_iv)
            )
        
        exchange = await self.storage.insert_flow_exchange(
            flow_waba      = flow_waba["id"],
            session_id     = session["id"],
            request_hash   = request_hash,
            request_action = request.action,
            request_screen = request.screen,
            request_data   = request_data,
        )
        if not exchange :
            return self._plain( "Request already processing", 409)
        
        try :
            if request.is_client_error :
                response = WhatsApp_OB_FlowResponse.acknowledge_error()
                result   = WhatsApp_Local_FlowHandlerResult(response = response)
            else :
                Handler  = self.handler_classes.get(session["handler_key"])
                callback = getattr( Handler, "handle_flow_request", None)
                if not callable(callback) :
                    raise RuntimeError(
                        f"No Flow handler for '{session['handler_key']}'"
                    )
                context = WhatsApp_Local_FlowContext(
                    flow_waba_id = flow_waba["id"],
                    session_id   = session["id"],
                    flow_id      = session["flow_id"],
                    flow_key     = session["flow_key"],
                    business_id  = session["business"],
                    contact_id   = session["contact"],
                    state        = session.get("session_state_data") or {},
                )
                result = callback( context, request)
                if inspect.isawaitable(result) :
                    result = await result
                result = WhatsApp_Local_FlowHandlerResult.model_validate(result)
            
            latency_ms = round( ( time.perf_counter() - started) * 1000)
            response_data = result.response.model_dump(
                mode         = "json",
                exclude_none = True,
            )
            await self.storage.finish_flow_exchange(
                exchange["id"],
                response_data   = response_data,
                exchange_status = "succeeded",
                http_status     = 200,
                latency_ms      = latency_ms,
            )
            await self.storage.update_flow_session(
                session["id"],
                result.session_status,
                session_state = result.state,
            )
            return self._plain(
                encrypt_flow_response( result.response, aes_key, request_iv)
            )
        
        except ( ValidationError, ValueError) as ex :
            error_code = "invalid_flow_response"
            error_name = type(ex).__name__
            logging.exception("Invalid WhatsApp Flow handler response")
        
        except Exception as ex :
            error_code = "flow_handler_error"
            error_name = type(ex).__name__
            logging.exception("WhatsApp Flow handler failed")
        
        latency_ms = round( ( time.perf_counter() - started) * 1000)
        await self.storage.finish_flow_exchange(
            exchange["id"],
            response_data   = None,
            exchange_status = "failed",
            http_status     = 500,
            latency_ms      = latency_ms,
            error_code      = error_code,
            error_message   = error_name,
        )
        await self.storage.update_flow_session(
            session["id"],
            "failed",
            last_error = { "code" : error_code },
        )
        return self._plain( "Flow handler failed", 500)


# =========================================================================================
# KEY PROVISIONING

def generate_flow_key_pair() -> tuple[ str, str, str] :
    """
    Generate one RSA-2048 key pair and its public-key SHA-256 fingerprint
    """
    private_key = rsa.generate_private_key(
        public_exponent = 65537,
        key_size        = 2048,
    )
    private_pem = private_key.private_bytes(
        encoding           = serialization.Encoding.PEM,
        format             = serialization.PrivateFormat.PKCS8,
        encryption_algorithm = serialization.NoEncryption(),
    ).decode("ascii")
    public_pem = private_key.public_key().public_bytes(
        encoding = serialization.Encoding.PEM,
        format   = serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")
    fingerprint = hashlib.sha256(public_pem.encode("ascii")).hexdigest()
    return private_pem, public_pem, fingerprint


def upload_flow_public_key( phone_number_id : str, public_key : str) -> None :
    """
    Upload a Flow endpoint public key to one WhatsApp phone number \\
    Args:
        phone_number_id : Meta WhatsApp phone-number ID
        public_key      : PEM-encoded RSA public key
    """
    response = httpx.post(
        f"{API_URL}{phone_number_id}/whatsapp_business_encryption",
        headers = write_headers(),
        data    = { "business_public_key" : public_key },
        timeout = 30,
    )
    response.raise_for_status()


def provision_flow_waba(
    waba_id         : str,
    phone_number_ids: list[str],
    *,
    database_url    : str | None = None,
) -> dict[str, Any] :
    """
    Generate, persist, upload, and verify one WABA's Flow key pair \\
    Args:
        waba_id          : Meta WhatsApp Business Account ID
        phone_number_ids : Phone-number IDs that belong to the WABA
        database_url     : Optional PostgreSQL connection URL override
    Returns:
        Provisioned WABA, phone-number IDs, key fingerprint, and timestamp
    """
    private_key, public_key, fingerprint = generate_flow_key_pair()
    storage = SyncSupabaseStorage(database_url)
    record  = storage.upsert_flow_waba(
        waba_id,
        public_key,
        fingerprint,
        private_key,
    )
    if not record :
        raise RuntimeError("Unable to persist WhatsApp Flow WABA key")
    
    for phone_number_id in phone_number_ids :
        upload_flow_public_key( phone_number_id, public_key)
        response = httpx.get(
            f"{API_URL}{phone_number_id}/whatsapp_business_encryption",
            headers = write_headers(),
            timeout = 30,
        )
        response.raise_for_status()
        data = response.json().get( "data", [])
        if not data or data[0].get("business_public_key_signature_status") != "VALID" :
            raise RuntimeError(
                f"Meta did not validate the Flow key for phone '{phone_number_id}'"
            )
    
    return {
        "waba_id"                : waba_id,
        "phone_number_ids"       : phone_number_ids,
        "public_key_fingerprint" : fingerprint,
        "provisioned_at"         : datetime.now(UTC).isoformat(),
    }


def main() -> None :
    """
    Provision a WABA Flow key pair from command-line arguments
    """
    parser = argparse.ArgumentParser(
        description = "Provision a per-WABA WhatsApp Flows endpoint key",
    )
    parser.add_argument( "--waba-id", required = True)
    parser.add_argument(
        "--phone-number-id",
        action   = "append",
        dest     = "phone_number_ids",
        required = True,
    )
    args   = parser.parse_args()
    result = provision_flow_waba( args.waba_id, args.phone_number_ids)
    print(json.dumps( result, indent = 2))


if __name__ == "__main__" :
    load_dotenv(find_dotenv(usecwd = True))
    main()
