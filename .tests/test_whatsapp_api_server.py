import asyncio
import json

from typing import Any

from pydantic import ValidationError
import pytest

from wa_agents.whatsapp_api_server import WhatsAppAPIServer
from wa_agents.whatsapp_models import WhatsAppPayload


class StubRequest :
    headers : dict[str, str] = {}

    def __init__( self, data : dict[str, Any]) -> None :
        self.data = data

    async def body(self) -> bytes :
        return json.dumps(self.data).encode("utf-8")


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
