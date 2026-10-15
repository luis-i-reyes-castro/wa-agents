import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os

import httpx
import pytest

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers import (
    Cipher,
    algorithms,
    modes,
)
from cryptography.hazmat.primitives import padding as symmetric_padding
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from wa_agents import flows
from wa_agents.case_handler_models import ServerFlowMsg
from wa_agents.flows import (
    async_fetch_flow_cdn_media,
    decrypt_flow_request,
    encrypt_flow_response,
    fetch_flow_cdn_media,
    generate_flow_key_pair,
)
from wa_agents.io_functions import (
    _collect_send_results,
    write_payload,
)
from wa_agents.io_models import (
    WhatsAppFlowMediaError,
    WhatsApp_Flow_CDNMedia,
    WhatsApp_Flow_EncryptionMetadata,
    WhatsApp_IB_Decrypted_FlowRequest,
    WhatsApp_IB_Encrypted_FlowRequest,
    WhatsApp_IB_FlowCDNMedia,
    WhatsApp_IB_FlowEncryptionMetadata,
    WhatsApp_IB_FlowReply,
    WhatsApp_OB_FlowResponse,
    WhatsApp_IB_Message,
)
from wa_agents.supabase import _message_data


FLOW_MEDIA_URL = "https://mmg.whatsapp.net/v/signed-secret"


def test_flow_media_model_compatibility_aliases() -> None :
    assert WhatsApp_Flow_CDNMedia is WhatsApp_IB_FlowCDNMedia
    assert (
        WhatsApp_Flow_EncryptionMetadata is
        WhatsApp_IB_FlowEncryptionMetadata
    )


def _b64( value : bytes) -> str :
    return base64.b64encode(value).decode()


def _flow_media_fixture() -> tuple[ WhatsApp_IB_FlowCDNMedia, bytes, bytes] :
    plaintext      = b"deterministic Flow media fixture"
    iv             = bytes(range(16))
    encryption_key = bytes(range(32))
    hmac_key       = bytes(range(32, 64))
    padder         = symmetric_padding.PKCS7(128).padder()
    padded         = padder.update(plaintext) + padder.finalize()
    encryptor      = Cipher(
        algorithms.AES(encryption_key),
        modes.CBC(iv),
    ).encryptor()
    ciphertext = encryptor.update(padded) + encryptor.finalize()
    hmac_tag   = hmac.new(
        hmac_key,
        iv + ciphertext,
        hashlib.sha256,
    ).digest()[:10]
    cdn_file = ciphertext + hmac_tag
    media    = WhatsApp_IB_FlowCDNMedia(
        media_id  = "790aba14-5f4a-4dbd-aa9e-0d75401da14b",
        cdn_url   = FLOW_MEDIA_URL,
        file_name = "fixture.jpg",
        encryption_metadata = WhatsApp_IB_FlowEncryptionMetadata(
            encrypted_hash = _b64(hashlib.sha256(cdn_file).digest()),
            iv             = _b64(iv),
            encryption_key = _b64(encryption_key),
            hmac_key       = _b64(hmac_key),
            plaintext_hash = _b64(hashlib.sha256(plaintext).digest()),
        ),
    )

    return media, cdn_file, plaintext


def _replace_cdn_file(
    media      : WhatsApp_IB_FlowCDNMedia,
    ciphertext : bytes,
    hmac_tag   : bytes | None = None,
) -> tuple[ WhatsApp_IB_FlowCDNMedia, bytes] :
    metadata = media.encryption_metadata
    iv       = base64.b64decode(metadata.iv)
    hmac_key = base64.b64decode(metadata.hmac_key)
    hmac_tag = hmac_tag or hmac.new(
        hmac_key,
        iv + ciphertext,
        hashlib.sha256,
    ).digest()[:10]
    cdn_file = ciphertext + hmac_tag
    metadata = metadata.model_copy(
        update = {
            "encrypted_hash" : _b64(hashlib.sha256(cdn_file).digest()),
        }
    )

    return media.model_copy( update = { "encryption_metadata" : metadata }), cdn_file


def _response( status_code : int, content : bytes = b"") -> httpx.Response :
    return httpx.Response(
        status_code,
        request = httpx.Request( "GET", FLOW_MEDIA_URL),
        content = content,
    )


def _run_flow_media_fetch(
    monkeypatch,
    async_mode : bool,
    media      : WhatsApp_IB_FlowCDNMedia,
    response   : httpx.Response,
) -> bytes :
    if not async_mode :
        monkeypatch.setattr(
            flows.httpx,
            "get",
            lambda *_args, **_kwargs: response,
        )
        return fetch_flow_cdn_media(media)

    class AsyncClient :

        async def __aenter__(self) :
            return self

        async def __aexit__( self, *_args) -> None :
            return

        async def get( self, *_args, **_kwargs) -> httpx.Response :
            return response

    monkeypatch.setattr( flows.httpx, "AsyncClient", AsyncClient)
    return asyncio.run(async_fetch_flow_cdn_media(media))


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


def test_flow_launch_payload_and_persisted_token_redaction(capsys) -> None :
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
    assert capsys.readouterr().out == ""

    _collect_send_results( payload, Response(), debug = True)
    assert "WhatsApp API response to sent message" in capsys.readouterr().out

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


def test_flow_reply_parses_completion_media_by_component() -> None :
    digest = _b64(hashlib.sha256(b"media").digest())
    reply  = WhatsApp_IB_FlowReply(
        response_json = json.dumps(
            {
                "photo_picker" : [
                    {
                        "id"        : "123456789",
                        "mime_type" : "image/jpeg",
                        "sha256"    : digest,
                        "file_name" : "first.jpg",
                    },
                    {
                        "id"        : "987654321",
                        "mime_type" : "image/png",
                        "sha256"    : digest,
                        "file_name" : "second.png",
                    },
                ],
                "document_picker" : [],
                "unrelated"       : [ { "value" : "not media" } ],
                "flow_token"      : "opaque",
            }
        )
    )

    assert set(reply.media) == { "photo_picker", "document_picker" }
    assert [ item.filename for item in reply.media["photo_picker"] ] == [
        "first.jpg",
        "second.png",
    ]
    assert reply.media["document_picker"] == []


def test_flow_reply_media_handles_missing_and_invalid_picker() -> None :
    assert WhatsApp_IB_FlowReply(
        response_json = '{"flow_token":"opaque"}'
    ).media == {}

    reply = WhatsApp_IB_FlowReply(
        response_json = json.dumps(
            {
                "photo_picker" : [
                    {
                        "id"        : "not-numeric",
                        "mime_type" : "image/jpeg",
                        "sha256"    : _b64(hashlib.sha256(b"media").digest()),
                    }
                ]
            }
        )
    )
    with pytest.raises( WhatsAppFlowMediaError, match = "completion") :
        _ = reply.media


def test_flow_request_parses_cdn_media_by_component() -> None :
    media, _cdn_file, _plaintext = _flow_media_fixture()
    request = WhatsApp_IB_Decrypted_FlowRequest(
        version = "3.0",
        action  = "data_exchange",
        data    = {
            "photo_picker"    : [ media.model_dump( mode = "json") ],
            "document_picker" : [],
            "unrelated"       : [ { "value" : "not media" } ],
        },
    )

    assert request.media == {
        "photo_picker"    : [ media ],
        "document_picker" : [],
    }


@pytest.mark.parametrize(
    "field",
    [
        "encrypted_hash",
        "iv",
        "encryption_key",
        "hmac_key",
        "plaintext_hash",
    ],
)
def test_flow_request_rejects_invalid_cdn_metadata_lengths( field : str) -> None :
    media, _cdn_file, _plaintext = _flow_media_fixture()
    data                         = media.model_dump( mode = "json")
    data["encryption_metadata"][field] = _b64(b"short")
    request = WhatsApp_IB_Decrypted_FlowRequest(
        version = "3.0",
        action  = "data_exchange",
        data    = { "photo_picker" : [ data ] },
    )

    with pytest.raises( WhatsAppFlowMediaError, match = "CDN media descriptor") as ex :
        _ = request.media

    assert FLOW_MEDIA_URL not in str(ex.value)
    assert data["encryption_metadata"][field] not in str(ex.value)


@pytest.mark.parametrize(
    ( "field", "value"),
    [
        ( "media_id",  "not-a-uuid"            ),
        ( "cdn_url",   "http://example.test/a" ),
        ( "file_name", ""                      ),
    ],
)
def test_flow_request_rejects_invalid_cdn_descriptor_fields(
    field : str,
    value : str,
) -> None :
    media, _cdn_file, _plaintext = _flow_media_fixture()
    data                         = media.model_dump( mode = "json")
    data[field]                  = value
    request                      = WhatsApp_IB_Decrypted_FlowRequest(
        version = "3.0",
        action  = "data_exchange",
        data    = { "photo_picker" : [ data ] },
    )

    with pytest.raises( WhatsAppFlowMediaError, match = "CDN media descriptor") :
        _ = request.media


def test_flow_request_rejects_noncanonical_base64_metadata() -> None :
    media, _cdn_file, _plaintext = _flow_media_fixture()
    data                         = media.model_dump( mode = "json")
    data["encryption_metadata"]["hmac_key"] = "not canonical base64"
    request = WhatsApp_IB_Decrypted_FlowRequest(
        version = "3.0",
        action  = "data_exchange",
        data    = { "photo_picker" : [ data ] },
    )

    with pytest.raises( WhatsAppFlowMediaError, match = "CDN media descriptor") as ex :
        _ = request.media

    assert "not canonical base64" not in str(ex.value)


@pytest.mark.parametrize( "async_mode", [ False, True ] )
def test_fetch_flow_cdn_media_decrypts_verified_content(
    monkeypatch,
    async_mode : bool,
) -> None :
    media, cdn_file, plaintext = _flow_media_fixture()

    assert _run_flow_media_fetch(
        monkeypatch,
        async_mode,
        media,
        _response( 200, cdn_file),
    ) == plaintext


@pytest.mark.parametrize(
    ( "failure", "error"),
    [
        ( "encrypted_hash", "encrypted hash"       ),
        ( "hmac",           "HMAC"                 ),
        ( "truncated",      "truncated"            ),
        ( "misaligned",     "not block aligned"    ),
        ( "padding",        "padding"              ),
        ( "plaintext_hash", "plaintext hash"       ),
    ],
)
def test_fetch_flow_cdn_media_rejects_invalid_content(
    monkeypatch,
    failure : str,
    error   : str,
) -> None :
    media, cdn_file, _plaintext = _flow_media_fixture()

    if failure == "encrypted_hash" :
        metadata = media.encryption_metadata.model_copy(
            update = { "encrypted_hash" : _b64(bytes(32)) }
        )
        media = media.model_copy( update = { "encryption_metadata" : metadata })

    elif failure == "hmac" :
        cdn_file = cdn_file[:-1] + bytes([ cdn_file[-1] ^ 1 ])
        metadata = media.encryption_metadata.model_copy(
            update = {
                "encrypted_hash" : _b64(hashlib.sha256(cdn_file).digest())
            }
        )
        media = media.model_copy( update = { "encryption_metadata" : metadata })

    elif failure == "truncated" :
        cdn_file = b"short"
        metadata = media.encryption_metadata.model_copy(
            update = {
                "encrypted_hash" : _b64(hashlib.sha256(cdn_file).digest())
            }
        )
        media = media.model_copy( update = { "encryption_metadata" : metadata })

    elif failure == "misaligned" :
        media, cdn_file = _replace_cdn_file( media, b"misaligned-data")

    elif failure == "padding" :
        metadata       = media.encryption_metadata
        iv             = base64.b64decode(metadata.iv)
        encryption_key = base64.b64decode(metadata.encryption_key)
        encryptor      = Cipher(
            algorithms.AES(encryption_key),
            modes.CBC(iv),
        ).encryptor()
        ciphertext     = encryptor.update(b"A" * 16) + encryptor.finalize()
        media, cdn_file = _replace_cdn_file( media, ciphertext)

    elif failure == "plaintext_hash" :
        metadata = media.encryption_metadata.model_copy(
            update = { "plaintext_hash" : _b64(bytes(32)) }
        )
        media = media.model_copy( update = { "encryption_metadata" : metadata })

    monkeypatch.setattr(
        flows.httpx,
        "get",
        lambda *_args, **_kwargs: _response( 200, cdn_file),
    )
    with pytest.raises( WhatsAppFlowMediaError, match = error) as ex :
        fetch_flow_cdn_media(media)

    assert FLOW_MEDIA_URL not in str(ex.value)
    assert media.encryption_metadata.encryption_key not in str(ex.value)
    assert media.encryption_metadata.hmac_key not in str(ex.value)


@pytest.mark.parametrize( "async_mode", [ False, True ] )
def test_flow_cdn_download_errors_do_not_log_secrets(
    monkeypatch,
    caplog,
    async_mode : bool,
) -> None :
    media, _cdn_file, _plaintext = _flow_media_fixture()

    with caplog.at_level(logging.ERROR) :
        try :
            _run_flow_media_fetch(
                monkeypatch,
                async_mode,
                media,
                _response(403),
            )
        except WhatsAppFlowMediaError :
            logging.exception("Flow media failed")

    assert "HTTP status 403" in caplog.text
    assert FLOW_MEDIA_URL not in caplog.text
    assert media.encryption_metadata.encryption_key not in caplog.text
    assert media.encryption_metadata.hmac_key not in caplog.text
