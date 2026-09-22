import asyncio
import hashlib
import hmac
import json

from typing import Any

from pydantic import ValidationError
import pytest

from wa_agents.whatsapp_api_server import WhatsAppAPIServer
from wa_agents.whatsapp_models import WhatsAppPayload


class StubRequest :
    def __init__(
        self,
        data    : dict[str, Any],
        headers : dict[str, str] | None = None,
    ) -> None :
        self.data    = data
        self.headers = headers or {}
        self.payload = json.dumps(data).encode("utf-8")

    async def body(self) -> bytes :
        return self.payload


class StubQueue :
    def __init__( self, error : Exception | None = None) -> None :
        self.error   = error
        self.payload : dict[str, Any] | None = None

    async def enqueue( self, payload : dict[str, Any]) -> dict[str, bool] :
        self.payload = payload
        if self.error :
            raise self.error
        return { "stored" : True, "enqueued" : True }


class StubHandler :
    pass


class RegistryHandler :
    HANDLER_KEY = "registry"


def response_data(response) -> dict[str, Any] :
    return json.loads(response.body)


def signed_request(
    data   : dict[str, Any],
    secret : str,
) -> StubRequest :
    request   = StubRequest(data)
    signature = hmac.new(
        secret.encode("utf-8"),
        request.payload,
        hashlib.sha256,
    ).hexdigest()
    request.headers["x-hub-signature-256"] = f"sha256={signature}"
    return request


def test_webhook_passes_payload_dict_to_queue() -> None :
    data   = { "object" : "whatsapp_business_account", "entry" : [] }
    queue  = StubQueue()
    server = WhatsAppAPIServer( StubHandler, queue)

    response = asyncio.run(server.webhook(StubRequest(data)))

    assert queue.payload == data
    assert response_data(response) == {
        "status"   : "ok",
        "stored"   : True,
        "enqueued" : True,
    }


def test_webhook_handles_payload_validation_error_from_queue() -> None :
    data = { "object" : "whatsapp_business_account", "entry" : [] }
    with pytest.raises(ValidationError) as exc_info :
        WhatsAppPayload.model_validate(data)

    queue    = StubQueue(exc_info.value)
    server   = WhatsAppAPIServer( StubHandler, queue)
    response = asyncio.run(server.webhook(StubRequest(data)))

    assert queue.payload == data
    assert response.status_code == 200
    assert response_data(response)["status"] == "error"


def test_server_accepts_handler_registry() -> None :
    server = WhatsAppAPIServer(
        queue_db        = StubQueue(),
        handler_classes = { "registry" : RegistryHandler },
    )

    assert server.queue_worker.handler_keys == ( "registry", )


def test_webhook_accepts_any_configured_meta_app_secret( monkeypatch) -> None :
    data    = { "object" : "whatsapp_business_account", "entry" : [] }
    queue   = StubQueue()
    secrets = ( "first-test-secret", "second-test-secret" )
    monkeypatch.setenv(
        "WA_APPS",
        json.dumps(
            [
                { "id" : "1001", "secret" : secrets[0] },
                { "id" : "1002", "secret" : secrets[1] },
            ]
        ),
    )
    server = WhatsAppAPIServer(
        StubHandler,
        queue,
        verify_app_secret = True,
    )

    response = asyncio.run(server.webhook(signed_request( data, secrets[1])))

    assert response.status_code == 200
    assert queue.payload == data


def test_webhook_rejects_unknown_meta_app_secret( monkeypatch) -> None :
    data  = { "object" : "whatsapp_business_account", "entry" : [] }
    queue = StubQueue()
    monkeypatch.setenv(
        "WA_APPS",
        json.dumps([ { "id" : "1001", "secret" : "configured-secret" } ]),
    )
    server = WhatsAppAPIServer(
        StubHandler,
        queue,
        verify_app_secret = True,
    )

    response = asyncio.run(
        server.webhook(signed_request( data, "unknown-secret"))
    )

    assert response.status_code == 401
    assert queue.payload is None


def test_server_rejects_malformed_meta_apps_configuration( monkeypatch) -> None :
    monkeypatch.setenv( "WA_APPS", "not-json")

    with pytest.raises( RuntimeError, match = "invalid JSON") :
        WhatsAppAPIServer(
            StubHandler,
            StubQueue(),
            verify_app_secret = True,
        )


def test_webhook_falls_back_to_legacy_app_secret( monkeypatch) -> None :
    data   = { "object" : "whatsapp_business_account", "entry" : [] }
    queue  = StubQueue()
    secret = "legacy-test-secret"
    monkeypatch.delenv( "WA_APPS", raising = False)
    monkeypatch.setenv( "WA_APP_SECRET", secret)
    server = WhatsAppAPIServer(
        StubHandler,
        queue,
        verify_app_secret = True,
    )

    response = asyncio.run(server.webhook(signed_request( data, secret)))

    assert response.status_code == 200
    assert queue.payload == data
