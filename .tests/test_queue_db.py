import asyncio

from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from wa_agents import queue_db
from wa_agents.whatsapp_models import WhatsAppPayload


class _StorageStub :

    def __init__(self) -> None :
        self.released = []

    def get_inbound_message( self, msg_id : str) -> dict :
        return {
            "id"       : 21,
            "msg_id"   : msg_id,
            "contact"  : 31,
            "business" : 41,
        }

    def release_contact_lease( self, contact, owner_token) -> bool :
        self.released.append(( contact, owner_token))
        return True


def test_queue_enqueues_persisted_message_id( monkeypatch) -> None :
    queue = queue_db.QueueDB("postgresql://test")
    calls = []

    def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return { "id" : 1 }

    monkeypatch.setattr( queue, "_fetch_one", fake_fetch_one)

    assert queue._enqueue_message("wamid.ABC123=") is True
    assert calls == [
        ( queue_db.SQL_ENQUEUE, { "msg_id" : "wamid.ABC123=" } ),
    ]


def test_queue_claim_returns_message_and_lease_identity( monkeypatch) -> None :
    queue         = queue_db.QueueDB("postgresql://test")
    queue.storage = _StorageStub()
    calls         = []

    def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return {
            "row_id"     : 1,
            "msg_id"     : "wamid.ABC123=",
            "msg_status" : "processing",
            "contact"    : 31,
        }

    monkeypatch.setattr( queue, "_fetch_one", fake_fetch_one)
    item = queue.claim_next()

    assert item["id"] == 21
    assert item["row_id"] == 1
    assert item["contact"] == 31
    assert isinstance( item["owner_token"], UUID)
    assert calls[0][0] == queue_db.SQL_CLAIM_NEXT
    assert calls[0][1]["owner_token"] == item["owner_token"]


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


class _PayloadStorageStub :
    def __init__( self, events : list[str], *, inserted : bool = True) -> None :
        self.events   = events
        self.inserted = inserted
        self.errors   : list[dict[str, Any]] | None = None

    def insert_inbound_payload(self, _data : Any) -> dict[str, Any] :
        self.events.append("insert_raw")
        return { "id" : 7, "inserted" : self.inserted }

    def mark_inbound_payload_invalid(
        self,
        _payload_id : int,
        errors      : list[dict[str, Any]],
    ) -> dict[str, Any] :
        self.events.append("mark_invalid")
        self.errors = errors
        return { "id" : 7 }


class _AsyncPayloadStorageStub :
    def __init__( self, events : list[str], *, inserted : bool = True) -> None :
        self.events   = events
        self.inserted = inserted
        self.message_data : dict[str, Any] | None = None

    async def insert_inbound_payload(self, _data : Any) -> dict[str, Any] :
        self.events.append("insert_raw")
        return { "id" : 7, "inserted" : self.inserted }

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


def _track_validation( monkeypatch, events : list[str]) -> None :
    model_validate = WhatsAppPayload.model_validate

    def validate( _cls, data) :
        events.append("validate")
        return model_validate(data)

    monkeypatch.setattr( WhatsAppPayload, "model_validate", classmethod(validate))


def test_queue_stores_invalid_payload_before_validation( monkeypatch) -> None :
    events        = []
    storage       = _PayloadStorageStub(events)
    queue         = queue_db.QueueDB("postgresql://test")
    queue.storage = storage
    _track_validation( monkeypatch, events)

    with pytest.raises(ValidationError) :
        queue.enqueue({ "object" : "whatsapp_business_account", "entry" : [] })

    assert events == [ "insert_raw", "validate", "mark_invalid" ]
    assert storage.errors


def test_async_queue_validates_normalizes_and_enqueues_payload(
    monkeypatch,
) -> None :
    events        = []
    storage       = _AsyncPayloadStorageStub(events)
    queue         = queue_db.AsyncQueueDB("postgresql://test")
    queue.storage = storage
    _track_validation( monkeypatch, events)

    async def enqueue_message( msg_id : str) -> bool :
        assert msg_id == "wamid.dGVzdA=="
        events.append("enqueue_message")
        return True

    monkeypatch.setattr( queue, "_enqueue_message", enqueue_message)
    result = asyncio.run(queue.enqueue(VALID_PAYLOAD))

    assert events == [
        "insert_raw",
        "validate",
        "mark_valid",
        "upsert_business",
        "upsert_contact",
        "insert_profile",
        "insert_metadata",
        "insert_message",
        "enqueue_message",
    ]
    assert storage.message_data == { "text" : { "body" : "Hello" } }
    assert result == { "stored" : True, "enqueued" : True }


def test_async_queue_does_not_normalize_duplicate_payload( monkeypatch) -> None :
    events        = []
    storage       = _AsyncPayloadStorageStub( events, inserted = False)
    queue         = queue_db.AsyncQueueDB("postgresql://test")
    queue.storage = storage
    _track_validation( monkeypatch, events)

    result = asyncio.run(queue.enqueue(VALID_PAYLOAD))

    assert events == [ "insert_raw", "validate", "mark_valid" ]
    assert result == { "stored" : False, "enqueued" : False }
