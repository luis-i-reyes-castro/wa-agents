import asyncio
import hashlib
import hmac
import json

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from pydantic import ValidationError
import pytest

from wa_agents.api_listener import WhatsAppAPIListener
from wa_agents.api_server import WhatsAppAPIServer
from wa_agents.api_worker import WhatsAppAPIWorker
from wa_agents.io_models import WhatsApp_IB_Payload


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


class LifespanQueue (StubQueue) :
    def __init__(self) -> None :
        super().__init__()
        self.events = []

    @asynccontextmanager
    async def connection_pool(self) :
        self.events.append("pool_open")
        try :
            yield
        finally :
            self.events.append("pool_close")


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
    server = WhatsAppAPIListener(
        queue             = queue,
        verify_app_secret = False,
    )

    response = asyncio.run(server.webhook(StubRequest(data)))

    assert queue.payload == data
    assert response_data(response) == {
        "status"   : "ok",
        "stored"   : True,
        "enqueued" : True,
    }


def test_combined_server_preserves_webhook_behavior() -> None :
    data   = { "object" : "whatsapp_business_account", "entry" : [] }
    queue  = StubQueue()
    server = WhatsAppAPIServer(
        handler_cls = StubHandler,
        queue       = queue,
    )

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
        WhatsApp_IB_Payload.model_validate(data)

    queue    = StubQueue(exc_info.value)
    server   = WhatsAppAPIListener(
        queue             = queue,
        verify_app_secret = False,
    )
    response = asyncio.run(server.webhook(StubRequest(data)))

    assert queue.payload == data
    assert response.status_code == 200
    assert response_data(response)["status"] == "error"


def test_server_accepts_handler_registry() -> None :
    queue  = StubQueue()
    server = WhatsAppAPIServer(
        queue           = queue,
        handler_classes = { "registry" : RegistryHandler },
    )

    assert server.queue is queue
    assert server.handler_keys == ( "registry", )


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
    server = WhatsAppAPIListener(
        queue             = queue,
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
    server = WhatsAppAPIListener(
        queue             = queue,
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
            handler_cls       = StubHandler,
            queue             = StubQueue(),
            verify_app_secret = True,
        )


def test_webhook_falls_back_to_legacy_app_secret( monkeypatch) -> None :
    data   = { "object" : "whatsapp_business_account", "entry" : [] }
    queue  = StubQueue()
    secret = "legacy-test-secret"
    monkeypatch.delenv( "WA_APPS", raising = False)
    monkeypatch.setenv( "WA_APP_SECRET", secret)
    server = WhatsAppAPIListener(
        queue             = queue,
        verify_app_secret = True,
    )

    response = asyncio.run(server.webhook(signed_request( data, secret)))

    assert response.status_code == 200
    assert queue.payload == data


def test_server_inherits_listener_and_worker_apps() -> None :
    assert issubclass( WhatsAppAPIListener, FastAPI)
    assert issubclass( WhatsAppAPIWorker, FastAPI)
    assert issubclass( WhatsAppAPIServer, WhatsAppAPIListener)
    assert issubclass( WhatsAppAPIServer, WhatsAppAPIWorker)


def test_apps_register_role_specific_routes_once() -> None :
    listener = WhatsAppAPIListener(
        queue             = StubQueue(),
        verify_app_secret = False,
    )
    worker = WhatsAppAPIWorker(
        handler_cls = StubHandler,
        queue       = StubQueue(),
    )
    server = WhatsAppAPIServer(
        handler_cls = StubHandler,
        queue       = StubQueue(),
    )

    listener_paths = [ route.path for route in listener.routes ]
    worker_paths   = [ route.path for route in worker.routes ]
    server_paths   = [ route.path for route in server.routes ]

    assert listener_paths.count("/webhook") == 2
    assert "/webhook" not in worker_paths
    assert server_paths.count("/webhook") == 2
    for path in ( "/", "/healthz", "/debugz" ) :
        assert listener_paths.count(path) == 1
        assert worker_paths.count(path) == 1
        assert server_paths.count(path) == 1


def test_listener_lifespan_owns_queue_pool() -> None :
    queue    = LifespanQueue()
    listener = WhatsAppAPIListener(
        queue             = queue,
        verify_app_secret = False,
    )

    async def run() -> None :
        async with listener.router.lifespan_context(listener) :
            assert queue.events == [ "pool_open" ]

    asyncio.run(run())

    assert queue.events == [ "pool_open", "pool_close" ]


@pytest.mark.parametrize(
    "App",
    [ WhatsAppAPIWorker, WhatsAppAPIServer ],
)
def test_worker_lifespan_owns_one_pool_and_worker_task(
    monkeypatch,
    App,
) -> None :
    queue  = LifespanQueue()
    server = App(
        handler_cls = StubHandler,
        queue       = queue,
    )
    events = queue.events

    async def serve_forever() -> None :
        events.append("worker_start")
        await asyncio.Event().wait()

    monkeypatch.setattr( server, "serve_forever", serve_forever)

    async def run() -> None :
        async with server.router.lifespan_context(server) :
            await asyncio.sleep(0)
            assert server.worker_task
            assert events == [ "pool_open", "worker_start" ]

    asyncio.run(run())

    assert server.worker_task is None
    assert server._stop_flag is True
    assert events == [ "pool_open", "worker_start", "pool_close" ]


def test_apps_expose_role_specific_debug_data( monkeypatch) -> None :
    monkeypatch.setenv( "WA_VERIFY_TOKEN", "test-token")
    listener = WhatsAppAPIListener(
        queue             = StubQueue(),
        verify_app_secret = False,
    )
    worker = WhatsAppAPIWorker(
        handler_cls = StubHandler,
        queue       = StubQueue(),
    )
    server = WhatsAppAPIServer(
        handler_cls = StubHandler,
        queue       = StubQueue(),
    )

    listener_data = response_data(asyncio.run(listener.listener_debugz()))
    worker_data   = response_data(asyncio.run(worker.worker_debugz()))
    server_data   = response_data(asyncio.run(server.debugz()))

    assert set(listener_data) == {
        "verify_token_set",
        "verify_token_tail",
        "verify_meta_app_secret",
    }
    assert set(worker_data) == {
        "worker_handler_keys",
        "worker_task_created",
        "worker_task_done",
        "worker_task_cancelled",
        "worker_task_exception",
        "worker_stop_flag",
    }
    assert server_data == { **listener_data, **worker_data }
