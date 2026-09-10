import asyncio
import json

from typing import Any

from wa_agents.whatsapp_api_server import WhatsAppAPIServer
from wa_agents.whatsapp_models import WhatsAppPayload


VALID_PAYLOAD = {
    "object" : "whatsapp_business_account",
    "entry"  : [
        {
            "id"      : "123456789",
            "time"    : 1788724265,
            "changes" : [
                {
                    "field" : "messages",
                    "value" : {
                        "messaging_product" : "whatsapp",
                        "metadata" : {
                            "display_phone_number" : "15551234567",
                            "phone_number_id"      : "987654321",
                        },
                        "contacts" : [
                            {
                                "profile" : { "name" : "Luis" },
                                "wa_id"   : "593995341161",
                            }
                        ],
                        "messages" : [
                            {
                                "from"      : "593995341161",
                                "id"        : "wamid.dGVzdA==",
                                "timestamp" : "1788724265",
                                "type"      : "text",
                                "text"      : { "body" : "Hello" },
                            }
                        ],
                    },
                }
            ],
        }
    ],
}


class StubRequest :
    headers : dict[str, str] = {}

    def __init__( self, data : Any) -> None :
        self.data = data

    async def body(self) -> bytes :
        return json.dumps(self.data).encode("utf-8")


class StubStorage :
    def __init__( self, events : list[str], *, inserted : bool = True) -> None :
        self.events   = events
        self.inserted = inserted
        self.errors   : list[dict[str, Any]] | None = None
        self.message_data : dict[str, Any] | None = None

    async def insert_inbound_payload(self, _data : Any) -> dict[str, Any] :
        self.events.append("insert_raw")
        return { "id" : 7, "inserted" : self.inserted }

    async def mark_inbound_payload_invalid(
        self,
        _payload_id : int,
        errors      : list[dict[str, Any]],
    ) -> dict[str, Any] :
        self.events.append("mark_invalid")
        self.errors = errors
        return { "id" : 7 }

    async def mark_inbound_payload_valid(self, _payload_id : int) -> dict[str, Any] :
        self.events.append("mark_valid")
        return { "id" : 7 }

    async def upsert_business(self, **_kwargs : Any) -> dict[str, Any] :
        self.events.append("upsert_business")
        return { "id" : 11 }

    async def upsert_contact(self, **_kwargs : Any) -> dict[str, Any] :
        self.events.append("upsert_contact")
        return { "id" : 13 }

    async def insert_contact_profile(self, **_kwargs : Any) -> dict[str, Any] :
        self.events.append("insert_profile")
        return { "id" : 17 }

    async def insert_inbound_payload_metadata(
        self,
        **_kwargs : Any,
    ) -> dict[str, Any] :
        self.events.append("insert_metadata")
        return { "id" : 19 }

    async def insert_inbound_message(self, **kwargs : Any) -> dict[str, Any] :
        self.events.append("insert_message")
        self.message_data = kwargs["msg_data"]
        return { "id" : 23 }


class StubQueue :
    def __init__( self, storage : StubStorage, events : list[str]) -> None :
        self.storage = storage
        self.events  = events
        self.msg_ids : list[str] = []

    async def enqueue( self, msg_id : str) -> bool :
        self.events.append("enqueue")
        self.msg_ids.append(msg_id)
        return True


class StubHandler :
    pass


def response_data(response) -> dict[str, Any] :
    return json.loads(response.body)


def track_validation( monkeypatch, events : list[str]) -> None :
    model_validate = WhatsAppPayload.model_validate

    def validate( _cls, data) :
        events.append("validate")
        return model_validate(data)

    monkeypatch.setattr( WhatsAppPayload, "model_validate", classmethod(validate))


def test_invalid_payload_is_stored_before_validation( monkeypatch) -> None :
    events  = []
    storage = StubStorage(events)
    server  = WhatsAppAPIServer( StubHandler, StubQueue( storage, events))
    track_validation( monkeypatch, events)

    response = asyncio.run(
        server.webhook(
            StubRequest({ "object" : "whatsapp_business_account", "entry" : [] })
        )
    )

    assert events == [ "insert_raw", "validate", "mark_invalid" ]
    assert storage.errors
    assert response.status_code == 200
    assert response_data(response)["stored"] is True


def test_valid_payload_is_marked_before_normalization_and_queueing(
    monkeypatch,
) -> None :
    events  = []
    storage = StubStorage(events)
    queue   = StubQueue( storage, events)
    server  = WhatsAppAPIServer( StubHandler, queue)
    track_validation( monkeypatch, events)

    response = asyncio.run(server.webhook(StubRequest(VALID_PAYLOAD)))

    assert events == [
        "insert_raw",
        "validate",
        "mark_valid",
        "upsert_business",
        "upsert_contact",
        "insert_profile",
        "insert_metadata",
        "insert_message",
        "enqueue",
    ]
    assert queue.msg_ids == [ "wamid.dGVzdA==" ]
    assert storage.message_data == { "text" : { "body" : "Hello" } }
    assert response_data(response) == {
        "status"   : "ok",
        "enqueued" : True,
        "stored"   : True,
    }


def test_duplicate_payload_is_validated_but_not_normalized( monkeypatch) -> None :
    events  = []
    storage = StubStorage( events, inserted = False)
    server  = WhatsAppAPIServer( StubHandler, StubQueue( storage, events))
    track_validation( monkeypatch, events)

    response = asyncio.run(server.webhook(StubRequest(VALID_PAYLOAD)))

    assert events == [ "insert_raw", "validate", "mark_valid" ]
    assert response_data(response) == {
        "status"   : "ok",
        "enqueued" : False,
        "stored"   : False,
    }
