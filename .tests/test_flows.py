import base64
import json
import os

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from wa_agents.case_handler_models import ServerFlowMsg
from wa_agents.flows import (
    decrypt_flow_request,
    encrypt_flow_response,
    generate_flow_key_pair,
)
from wa_agents.io_functions import (
    _collect_send_results,
    write_payload,
)
from wa_agents.io_models import (
    WhatsApp_IB_Encrypted_FlowRequest,
    WhatsApp_OB_FlowResponse,
    WhatsApp_IB_Message,
)
from wa_agents.supabase import _message_data


def test_flow_endpoint_encryption_round_trip() -> None :
    private_pem, _public_pem, _fingerprint = generate_flow_key_pair()
    from cryptography.hazmat.primitives import serialization
    
    private_key = serialization.load_pem_private_key(
        private_pem.encode(),
        password = None,
    )
    aes_key = os.urandom(16)
    iv      = os.urandom(12)
    data    = {
        "version"    : "3.0",
        "action"     : "INIT",
        "flow_token" : "test-token",
        "data"       : {},
    }
    envelope = WhatsApp_IB_Encrypted_FlowRequest(
        encrypted_aes_key = base64.b64encode(
            private_key.public_key().encrypt(
                aes_key,
                padding.OAEP(
                    mgf       = padding.MGF1( algorithm = hashes.SHA256()),
                    algorithm = hashes.SHA256(),
                    label     = None,
                ),
            )
        ).decode(),
        initial_vector = base64.b64encode(iv).decode(),
        encrypted_flow_data = base64.b64encode(
            AESGCM(aes_key).encrypt(
                iv,
                json.dumps(data).encode(),
                None,
            )
        ).decode(),
    )
    request, decrypted_key, request_iv = decrypt_flow_request(
        envelope,
        private_pem,
    )
    assert request.action == "INIT"
    assert request.flow_token == "test-token"
    assert decrypted_key == aes_key
    
    encrypted = encrypt_flow_response(
        WhatsApp_OB_FlowResponse( screen = "NEXT", data = { "ok" : True }),
        aes_key,
        request_iv,
    )
    plaintext = AESGCM(aes_key).decrypt(
        bytes( byte ^ 0xFF for byte in iv),
        base64.b64decode(encrypted),
        None,
    )
    assert json.loads(plaintext) == {
        "data"    : { "ok" : True },
        "screen"  : "NEXT",
        "version" : "3.0",
    }


def test_flow_launch_payload_and_persisted_token_redaction() -> None :
    message = ServerFlowMsg(
        flow_id    = "123456",
        flow_token = "secret-token",
        flow_cta   = "Open",
        body       = "Start here",
        screen     = "START",
    )
    payload = write_payload( "593999999999", message)
    parameters = payload["interactive"]["action"]["parameters"]
    assert parameters["flow_message_version"] == "3"
    assert parameters["flow_token"] == "secret-token"
    assert parameters["flow_action_payload"] == { "screen" : "START" }
    
    class Response :
        def json(self) :
            return { "messages" : [ { "id" : "wamid.dGVzdA==" } ] }
        def raise_for_status(self) :
            return None
    
    results = _collect_send_results( payload, Response())
    stored  = results[0]["msg_data"]["interactive"]
    assert stored["action"]["parameters"]["flow_token"] == "[REDACTED]"
    assert _message_data(message).obj["flow_token"] == "[REDACTED]"


def test_nfm_reply_parses_safe_completion_payload() -> None :
    message = WhatsApp_IB_Message.model_validate(
        {
            "from"      : "593999999999",
            "id"        : "wamid.dGVzdA==",
            "timestamp" : "1700000000",
            "type"      : "interactive",
            "interactive" : {
                "type"      : "nfm_reply",
                "nfm_reply" : {
                    "name"          : "flow",
                    "body"          : "Sent",
                    "response_json" : json.dumps(
                        {
                            "flow_token" : "opaque",
                            "service"    : "credit",
                            "outcome"    : "success",
                        }
                    ),
                },
            },
        }
    )
    assert message.interactive is not None
    assert message.interactive.nfm_reply is not None
    assert message.interactive.nfm_reply.response["service"] == "credit"
