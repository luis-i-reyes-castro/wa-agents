import asyncio
import hashlib
import hmac
import json

from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import patch

import wa_agents.api_listener as listener_module
from pydantic import ValidationError
import pytest

from wa_agents.api_listener import WhatsAppAPIListener
from wa_agents.api_server import WhatsAppAPIServer
from wa_agents.api_worker import WhatsAppAPIWorker
from wa_agents.io_models import WhatsApp_IB_Payload


TEST_APPS = (
    { "app" : "first", "id" : "1001", "secret" : "first-test-secret" },
    { "app" : "second", "id" : "1002", "secret" : "second-test-secret" },
)


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

    async def json(self) -> dict[str, Any] :
        return self.data


class StubQueue :
    def __init__(
        self,
        error  : Exception | None = None,
        result : dict[str, bool] | None = None,
    ) -> None :
        self.error   = error
        self.result  = (
            result
            if result is not None else
            { "stored" : True, "enqueued" : True }
        )
        self.payload : dict[str, Any] | None = None
        self.storage = object()

    async def enqueue( self, payload : dict[str, Any]) -> dict[str, bool] :
        self.payload = payload
        if self.error :
            raise self.error
        return self.result


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
        apps = TEST_APPS,
        queue             = queue,
        verify_signatures = False,
    )

    response = asyncio.run(server.webhook(StubRequest(data)))

    assert queue.payload == data
    assert response_data(response) == {
        "status"   : "ok",
        "stored"   : True,
        "enqueued" : True,
    }


def test_webhook_acknowledges_duplicate_payload() -> None :
    data = { "object" : "whatsapp_business_account", "entry" : [] }
    queue = StubQueue(
        result = { "stored" : False, "enqueued" : False },
    )
    server = WhatsAppAPIListener(
        apps = TEST_APPS,
        queue             = queue,
        verify_signatures = False,
    )

    response = asyncio.run(server.webhook(StubRequest(data)))

    assert response.status_code == 200
    assert response_data(response) == {
        "status"   : "ok",
        "stored"   : False,
        "enqueued" : False,
    }


def test_webhook_prints_unicode_payload_only_in_debug_mode(capsys) -> None :
    data = { "name" : "José" }

    quiet_server = WhatsAppAPIListener(
        apps = TEST_APPS,
        queue             = StubQueue(),
        verify_signatures = False,
    )
    asyncio.run(quiet_server.webhook(StubRequest(data)))

    assert capsys.readouterr().out == ""

    debug_server = WhatsAppAPIListener(
        apps = TEST_APPS,
        debug             = True,
        queue             = StubQueue(),
        verify_signatures = False,
    )
    asyncio.run(debug_server.webhook(StubRequest(data)))
    output = capsys.readouterr().out

    assert "WhatsApp API incoming payload" in output
    assert "José" in output
    assert "\\u00e9" not in output


@pytest.mark.parametrize(
    "app",
    [
        WhatsAppAPIListener(
            apps = TEST_APPS,
            debug             = True,
            queue             = StubQueue(),
            verify_signatures = False,
        ),
        WhatsAppAPIWorker(
            debug       = True,
            handler_cls = StubHandler,
            queue       = StubQueue(),
        ),
        WhatsAppAPIServer(
            apps = TEST_APPS,
            debug       = True,
            handler_cls = StubHandler,
            queue       = StubQueue(),
        ),
    ],
)
def test_apps_preserve_debug_flag( app : FastAPI) -> None :
    assert app.debug is True


def test_combined_server_preserves_webhook_behavior() -> None :
    data   = { "object" : "whatsapp_business_account", "entry" : [] }
    queue  = StubQueue()
    server = WhatsAppAPIServer(
        apps = TEST_APPS,
        verify_signatures = False,
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
        apps = TEST_APPS,
        queue             = queue,
        verify_signatures = False,
    )
    response = asyncio.run(server.webhook(StubRequest(data)))

    assert queue.payload == data
    assert response.status_code == 200
    assert response_data(response)["status"] == "error"


def test_server_accepts_handler_registry() -> None :
    queue  = StubQueue()
    server = WhatsAppAPIServer(
        apps = TEST_APPS,
        queue           = queue,
        handler_classes = { "registry" : RegistryHandler },
    )

    assert server.queue is queue
    assert server.handler_keys == ( "registry", )


def test_server_forwards_worker_timing() -> None :
    server = WhatsAppAPIServer(
        apps = TEST_APPS,
        handler_cls        = StubHandler,
        queue              = StubQueue(),
        poll_interval_busy = 0.1,
        poll_interval_idle = 2.0,
        response_delay     = 3.0,
        silence_timeout    = timedelta( minutes = 45),
    )

    assert server.poll_interval_busy == 0.1
    assert server.poll_interval_idle == 2.0
    assert server.response_delay == 3.0
    assert server.silence_timeout == timedelta( minutes = 45)


@pytest.mark.parametrize( "app", TEST_APPS)
def test_webhook_checks_only_the_addressed_app_secret(app) -> None :
    queue = StubQueue()
    server = WhatsAppAPIListener( apps = TEST_APPS, queue = queue)
    client = TestClient(server)
    data = { "object" : "whatsapp_business_account", "entry" : [] }
    request = signed_request( data, app["secret"])
    
    with patch.object(
        listener_module, "verify_payload_signature",
        wraps = listener_module.verify_payload_signature,
    ) as verify :
        response = client.post(
            f"/{app['app']}", content = request.payload, headers = request.headers,
        )
    
    assert response.status_code == 200
    assert queue.payload == data
    verify.assert_called_once_with(
        app_secret = app["secret"],
        payload    = request.payload,
        signature  = request.headers["x-hub-signature-256"],
    )


@pytest.mark.parametrize( "app", TEST_APPS)
@pytest.mark.parametrize( "signature_kind", [ "other-app", "unknown", "missing" ])
def test_webhook_rejects_other_secrets_without_enqueueing( app, signature_kind) -> None :
    queue = StubQueue()
    server = WhatsAppAPIListener( apps = TEST_APPS, queue = queue)
    other = next(item for item in TEST_APPS if item["app"] != app["app"])
    secret = other["secret"] if signature_kind == "other-app" else "unknown-secret"
    request = signed_request( { "entry" : [] }, secret)
    if signature_kind == "missing" :
        request.headers.clear()
    
    with patch.object(
        listener_module, "verify_payload_signature",
        wraps = listener_module.verify_payload_signature,
    ) as verify :
        response = TestClient(server).post(
            f"/{app['app']}", content = request.payload, headers = request.headers,
        )
    
    assert response.status_code == 401
    assert queue.payload is None
    assert verify.call_count == 1


@pytest.mark.parametrize( "signature", [ b"sha1=invalid", b"sha256=\xff" ])
def test_malformed_signatures_are_rejected_without_enqueueing(signature) -> None :
    
    queue = StubQueue()
    server = WhatsAppAPIListener( apps = TEST_APPS, queue = queue)
    
    response = TestClient(server).post(
        "/first",
        content = b"{}",
        headers = { b"x-hub-signature-256" : signature },
    )
    
    assert response.status_code == 401
    assert queue.payload is None


@pytest.mark.parametrize( "app", TEST_APPS)
def test_each_callback_verifies_with_shared_token( monkeypatch, app) -> None :
    monkeypatch.setenv( "WA_VERIFY_TOKEN", "shared-token")
    client = TestClient(WhatsAppAPIListener( apps = TEST_APPS, queue = StubQueue()))
    params = {
        "hub.mode" : "subscribe", "hub.verify_token" : "shared-token",
        "hub.challenge" : "challenge",
    }
    response = client.get( f"/{app['app']}", params = params)
    assert response.status_code == 200
    assert response.text == "challenge"
    params["hub.verify_token"] = "wrong"
    assert client.get( f"/{app['app']}", params = params).status_code == 403


def test_unknown_and_removed_webhooks_do_not_enqueue() -> None :
    queue = StubQueue()
    server = WhatsAppAPIListener( apps = TEST_APPS, queue = queue)
    client = TestClient(server)
    
    with patch.object( listener_module, "verify_payload_signature") as verify :
        for path in [ "/unknown", "/whatsapp", "/webhook", "/first/nested" ] :
            assert client.post( path, content = b"{}").status_code == 404
            assert client.get(path).status_code == 404
    verify.assert_not_called()
    assert queue.payload is None
    
    schema = json.dumps(server.openapi())
    for app in TEST_APPS :
        assert app["secret"] not in schema
        assert app["id"] not in schema


def test_combined_server_verifies_signatures_by_default() -> None :
    queue = StubQueue()
    server = WhatsAppAPIServer(
        apps = TEST_APPS, queue = queue, handler_cls = StubHandler,
    )
    request = signed_request( { "entry" : [] }, TEST_APPS[0]["secret"])
    client = TestClient(server)
    assert client.post(
        "/second", content = request.payload, headers = request.headers,
    ).status_code == 401
    assert queue.payload is None
    assert client.post(
        "/first", content = request.payload, headers = request.headers,
    ).status_code == 200


@pytest.mark.parametrize( "name", [ "docs", "redoc", "healthz", "debugz" ])
def test_callback_cannot_shadow_framework_routes(name) -> None :
    apps = ( { **TEST_APPS[0], "app" : name }, )
    with pytest.raises( RuntimeError, match = "collides") :
        WhatsAppAPIListener( apps = apps, queue = StubQueue())


def test_callback_cannot_shadow_later_business_or_dynamic_routes() -> None :
    server = WhatsAppAPIListener( apps = TEST_APPS, queue = StubQueue())
    server.add_api_route( "/{business}", lambda : None, methods = [ "POST" ])
    with pytest.raises( RuntimeError, match = "collides") :
        server.validate_listener_routes()


def test_server_inherits_listener_and_worker_apps() -> None :
    assert issubclass( WhatsAppAPIListener, FastAPI)
    assert issubclass( WhatsAppAPIWorker, FastAPI)
    assert issubclass( WhatsAppAPIServer, WhatsAppAPIListener)
    assert issubclass( WhatsAppAPIServer, WhatsAppAPIWorker)


def test_apps_register_role_specific_routes_once() -> None :
    listener = WhatsAppAPIListener(
        apps = TEST_APPS,
        queue             = StubQueue(),
        verify_signatures = False,
    )
    worker = WhatsAppAPIWorker(
        handler_cls = StubHandler,
        queue       = StubQueue(),
    )
    server = WhatsAppAPIServer(
        apps = TEST_APPS,
        handler_cls = StubHandler,
        queue       = StubQueue(),
    )

    listener_paths = [ route.path for route in listener.routes ]
    worker_paths   = [ route.path for route in worker.routes ]
    server_paths   = [ route.path for route in server.routes ]

    assert listener_paths.count("/first") == 2
    assert "/webhook/flows/{waba_id}" not in listener_paths
    assert "/webhook" not in worker_paths
    assert worker_paths.count("/webhook/flows/{waba_id}") == 1
    assert server_paths.count("/first") == 2
    assert server_paths.count("/flows/{waba_id}") == 1
    for path in ( "/", "/healthz", "/debugz" ) :
        assert listener_paths.count(path) == 1
        assert worker_paths.count(path) == 1
        assert server_paths.count(path) == 1


def test_listener_lifespan_owns_queue_pool() -> None :
    queue    = LifespanQueue()
    listener = WhatsAppAPIListener(
        apps = TEST_APPS,
        queue             = queue,
        verify_signatures = False,
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
        **({ "apps" : TEST_APPS } if App is WhatsAppAPIServer else {}),
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
        apps = TEST_APPS,
        queue             = StubQueue(),
    )
    worker = WhatsAppAPIWorker(
        handler_cls = StubHandler,
        queue       = StubQueue(),
    )
    server = WhatsAppAPIServer(
        apps = TEST_APPS,
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
