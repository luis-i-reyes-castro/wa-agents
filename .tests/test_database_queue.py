import asyncio

from copy import deepcopy
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from wa_agents import database_queue
from wa_agents.io_models import WhatsApp_IB_Payload


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


class _AsyncStorageStub :

    def __init__(self) -> None :
        self.released = []

    async def get_inbound_message( self, msg_id : str) -> dict :
        return {
            "id"       : 22,
            "msg_id"   : msg_id,
            "contact"  : 31,
            "business" : 41,
        }

    async def release_contact_lease( self, contact, owner_token) -> bool :
        self.released.append(( contact, owner_token))
        return True


def test_legacy_queue_class_names_are_removed() -> None :
    assert not hasattr( database_queue, "QueueDB")
    assert not hasattr( database_queue, "AsyncQueueDB")


@pytest.mark.parametrize( "raise_inside", [ False, True ] )
def test_async_queue_owns_connection_pool_lifetime(
    monkeypatch,
    raise_inside : bool,
) -> None :
    events = []
    queue  = database_queue.AsyncWhatsAppDatabaseQueue("postgresql://test")

    async def open_pool( database_url : str) -> None :
        events.append(f"open:{database_url}")

    async def close_pool() -> None :
        events.append("close")

    monkeypatch.setattr(
        database_queue,
        "open_async_database_connection_pool",
        open_pool,
    )
    monkeypatch.setattr(
        database_queue,
        "close_async_database_connection_pool",
        close_pool,
    )

    async def run() -> None :
        async with queue.connection_pool() :
            events.append("yield")
            if raise_inside :
                raise RuntimeError("test")

    if raise_inside :
        with pytest.raises( RuntimeError, match = "test") :
            asyncio.run(run())
    else :
        asyncio.run(run())

    assert events == [ "open:postgresql://test", "yield", "close" ]


def test_queue_enqueues_persisted_message_id( monkeypatch) -> None :
    queue = database_queue.WhatsAppDatabaseQueue(
        "postgresql://test",
        fallback_handler_key = "fallback",
    )
    calls = []

    def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return { "id" : 1 }

    monkeypatch.setattr( queue, "_fetch_one", fake_fetch_one)

    assert queue._enqueue_message("wamid.ABC123=") is True
    assert calls == [
        (
            database_queue.SQL_ENQUEUE,
            { "msg_id" : "wamid.ABC123=" },
        ),
    ]


def test_queue_claim_returns_message_and_lease_identity( monkeypatch) -> None :
    queue         = database_queue.WhatsAppDatabaseQueue("postgresql://test")
    queue.storage = _StorageStub()
    calls         = []

    def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return {
            "row_id"     : 1,
            "msg_id"     : "wamid.ABC123=",
            "msg_status" : "processing",
            "contact"    : 31,
            "handler_id" : 7,
            "handler_key": "default",
        }

    monkeypatch.setattr( queue, "_fetch_one", fake_fetch_one)
    item = queue.claim_next()

    assert item["id"] == 21
    assert item["row_id"] == 1
    assert item["contact"] == 31
    assert item["handler_id"] == 7
    assert item["handler_key"] == "default"
    assert isinstance( item["owner_token"], UUID)
    assert calls[0][0] == database_queue.SQL_CLAIM_NEXT
    assert calls[0][1]["owner_token"] == item["owner_token"]
    assert calls[0][1]["handler_keys"] == [ "default" ]
    assert calls[0][1]["fallback_handler_key"] == "default"


def test_queue_claims_next_contact_message_with_existing_lease(monkeypatch) -> None :
    queue         = database_queue.WhatsAppDatabaseQueue("postgresql://test")
    queue.storage = _StorageStub()
    owner_token   = UUID("12345678-1234-5678-1234-567812345678")
    calls         = []

    def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return {
            "row_id"     : 2,
            "msg_id"     : "wamid.SECOND=",
            "msg_status" : "processing",
            "contact"    : 31,
            "handler_id" : 7,
            "handler_key": "default",
        }

    monkeypatch.setattr( queue, "_fetch_one", fake_fetch_one)
    item = queue.claim_next_for_contact(
        31,
        owner_token,
        1,
        [ "retail", "default" ],
    )

    assert item and item["id"] == 21
    assert item["row_id"] == 2
    assert item["owner_token"] == owner_token
    assert calls == [
        (
            database_queue.SQL_CLAIM_NEXT_FOR_CONTACT,
            {
                "contact"              : 31,
                "owner_token"          : owner_token,
                "after_row_id"         : 1,
                "handler_keys"         : [ "default", "retail" ],
                "fallback_handler_key" : "default",
            },
        )
    ]


def test_async_queue_claims_next_contact_message_with_existing_lease(
    monkeypatch,
) -> None :
    queue         = database_queue.AsyncWhatsAppDatabaseQueue("postgresql://test")
    queue.storage = _AsyncStorageStub()
    owner_token   = UUID("12345678-1234-5678-1234-567812345678")
    calls         = []

    async def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return {
            "row_id"     : 2,
            "msg_id"     : "wamid.SECOND=",
            "msg_status" : "processing",
            "contact"    : 31,
            "handler_id" : 7,
            "handler_key": "default",
        }

    monkeypatch.setattr( queue, "_fetch_one", fake_fetch_one)

    async def run() -> dict | None :
        return await queue.claim_next_for_contact(
            31,
            owner_token,
            1,
            [ "default" ],
        )

    item = asyncio.run(run())

    assert item and item["id"] == 22
    assert item["owner_token"] == owner_token
    assert calls[0][0] == database_queue.SQL_CLAIM_NEXT_FOR_CONTACT
    assert calls[0][1]["owner_token"] == owner_token


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

HISTORY_PAYLOAD = {
    "object" : "whatsapp_business_account",
    "entry"  : [
        {
            "id"      : "123456789",
            "changes" : [
                {
                    "field" : "history",
                    "value" : {
                        "messaging_product" : "whatsapp",
                        "metadata" : {
                            "display_phone_number" : "15551234567",
                            "phone_number_id"      : "987654321",
                        },
                        "history" : [
                            {
                                "metadata" : {
                                    "phase"       : 1,
                                    "chunk_order" : 4,
                                    "progress"    : 60,
                                },
                                "threads" : [
                                    {
                                        "id"       : "593995341161",
                                        "messages" : [
                                            {
                                                "from" : "593995341161",
                                                "id"   : "wamid.history-in",
                                                "timestamp" : "1788724265",
                                                "type" : "text",
                                                "text" : {
                                                    "body" : "Previous message"
                                                },
                                                "history_context" : {
                                                    "status" : "read"
                                                },
                                            },
                                            {
                                                "from" : "15551234567",
                                                "to"   : "593995341161",
                                                "id"   : "wamid.history-out",
                                                "timestamp" : "1788724270",
                                                "type" : "media_placeholder",
                                                "history_context" : {
                                                    "status" : "delivered"
                                                },
                                            },
                                            {
                                                "from" : "593995341161",
                                                "id"   : "wamid.history-error",
                                                "timestamp" : "1788724271",
                                                "errors" : [
                                                    {
                                                        "code" : 131051,
                                                        "title" : (
                                                            "Message type unknown"
                                                        ),
                                                    },
                                                ],
                                                "type" : "errors",
                                                "history_context" : {
                                                    "status" : "pending"
                                                },
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                },
            ],
        },
    ],
}

HISTORY_MEDIA_PAYLOAD = {
    "object" : "whatsapp_business_account",
    "entry"  : [
        {
            "id"      : "123456789",
            "changes" : [
                {
                    "field" : "history",
                    "value" : {
                        "messaging_product" : "whatsapp",
                        "metadata" : {
                            "display_phone_number" : "15551234567",
                            "phone_number_id"      : "987654321",
                        },
                        "messages" : [
                            {
                                "from"      : "593995341161",
                                "id"        : "wamid.history-out",
                                "timestamp" : "1788724271",
                                "type"      : "image",
                                "image" : {
                                    "id"        : "24230790383178626",
                                    "mime_type" : "image/jpeg",
                                    "sha256" : (
                                        "P52U05n6YckRvBvEccgTpdA1zbG5tR8J"
                                        "6WOkFcbLDQw="
                                    ),
                                },
                            },
                        ],
                    },
                },
            ],
        },
    ],
}

SMB_APP_STATE_SYNC_PAYLOAD = {
    "object" : "whatsapp_business_account",
    "entry"  : [
        {
            "id"      : "123456789",
            "changes" : [
                {
                    "field" : "smb_app_state_sync",
                    "value" : {
                        "messaging_product" : "whatsapp",
                        "metadata" : {
                            "display_phone_number" : "15551234567",
                            "phone_number_id"      : "987654321",
                        },
                        "state_sync" : [
                            {
                                "type"    : "contact",
                                "action"  : "add",
                                "contact" : {
                                    "full_name"    : "Pablo Morales",
                                    "first_name"   : "Pablo",
                                    "phone_number" : "593995341161",
                                },
                                "metadata" : { "timestamp" : "1738346006000" },
                            },
                            {
                                "type"    : "contact",
                                "action"  : "remove",
                                "contact" : {
                                    "phone_number" : "593995341162",
                                },
                                "metadata" : { "timestamp" : "1738346010000" },
                            },
                        ],
                    },
                },
            ],
        },
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

    def mark_inbound_payload_valid(self, _payload_id : int) -> dict[str, Any] :
        self.events.append("mark_valid")
        return { "id" : 7 }


class _AsyncPayloadStorageStub :
    def __init__( self, events : list[str], *, inserted : bool = True) -> None :
        self.events   = events
        self.inserted = inserted
        self.message_data : dict[str, Any] | None = None
        self.metadata     : dict[str, Any] | None = None
        self.history_chunk : dict[str, Any] | None = None
        self.messages      : list[dict[str, Any]] = []
        self.smb_contacts  : list[dict[str, Any]] = []

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

    async def upsert_contact_profile(self, **_kwargs : Any) -> dict[str, Any] :
        self.events.append("upsert_profile")
        return { "id" : 17 }

    async def upsert_smb_app_contact(self, **kwargs : Any) -> dict[str, Any] :
        self.events.append("upsert_smb_app_contact")
        self.smb_contacts.append(kwargs)
        return { "contact" : kwargs["contact"] }

    async def complete_smb_app_contact_sync(
        self,
        _phone_number : int,
    ) -> dict[str, Any] :
        self.events.append("complete_smb_app_contact_sync")
        return { "id" : 31 }

    async def update_inbound_payload_metadata(
        self,
        **kwargs : Any,
    ) -> dict[str, Any] :
        self.events.append("update_metadata")
        self.metadata = kwargs
        return { "id" : 19 }

    async def insert_inbound_message(self, **kwargs : Any) -> dict[str, Any] :
        self.events.append("insert_message")
        self.message_data = kwargs["msg_data"]
        self.messages.append(kwargs)
        return { "id" : 23 }

    async def insert_history_chunk(self, **kwargs : Any) -> dict[str, Any] :
        self.events.append("insert_history_chunk")
        self.history_chunk = kwargs
        return { "id" : 29 }

    async def get_inbound_message( self, msg_id : str) -> dict[str, Any] :
        self.events.append("get_message")
        return {
            "id"              : 23,
            "contact"         : 13,
            "payload"         : 5,
            "is_echo"         : True,
            "msg_id"          : msg_id,
            "msg_type"        : "media_placeholder",
            "history_chunk"   : 29,
            "history_context" : { "status" : "played" },
        }


def _track_validation( monkeypatch, events : list[str]) -> None :
    model_validate = WhatsApp_IB_Payload.model_validate

    def validate( _cls, data) :
        events.append("validate")
        return model_validate(data)

    monkeypatch.setattr( WhatsApp_IB_Payload, "model_validate", classmethod(validate))


def test_queue_stores_invalid_payload_before_validation( monkeypatch) -> None :
    events        = []
    storage       = _PayloadStorageStub(events)
    queue         = database_queue.WhatsAppDatabaseQueue("postgresql://test")
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
    queue         = database_queue.AsyncWhatsAppDatabaseQueue("postgresql://test")
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
        "upsert_business",
        "upsert_contact",
        "upsert_profile",
        "update_metadata",
        "insert_message",
        "enqueue_message",
        "mark_valid",
    ]
    assert storage.metadata and storage.metadata["item_index"] == 0
    assert storage.metadata["change_index"] == 0
    assert storage.message_data == { "text" : { "body" : "Hello" } }
    assert result == { "stored" : True, "enqueued" : True }


def test_async_queue_does_not_normalize_duplicate_payload( monkeypatch) -> None :
    events        = []
    storage       = _AsyncPayloadStorageStub( events, inserted = False)
    queue         = database_queue.AsyncWhatsAppDatabaseQueue("postgresql://test")
    queue.storage = storage
    _track_validation( monkeypatch, events)

    result = asyncio.run(queue.enqueue(VALID_PAYLOAD))

    assert events == [ "insert_raw", "validate" ]
    assert result == { "stored" : False, "enqueued" : False }


def test_sync_queue_reprocesses_duplicate_embedded_sync( monkeypatch) -> None :
    events        = []
    storage       = _PayloadStorageStub( events, inserted = False)
    queue         = database_queue.WhatsAppDatabaseQueue("postgresql://test")
    queue.storage = storage
    _track_validation( monkeypatch, events)

    def persist( _payload_id : int, _payload : WhatsApp_IB_Payload) -> bool :
        events.append("normalize")
        return False

    monkeypatch.setattr( queue, "_persist_valid_payload", persist)
    result = queue.enqueue(SMB_APP_STATE_SYNC_PAYLOAD)

    assert events == [ "insert_raw", "validate", "normalize", "mark_valid" ]
    assert result == { "stored" : False, "enqueued" : False }


def test_sync_queue_does_not_mark_payload_valid_when_normalization_fails(
    monkeypatch,
) -> None :
    events        = []
    storage       = _PayloadStorageStub(events)
    queue         = database_queue.WhatsAppDatabaseQueue("postgresql://test")
    queue.storage = storage
    _track_validation( monkeypatch, events)

    def persist( _payload_id : int, _payload : WhatsApp_IB_Payload) -> bool :
        events.append("normalize")
        raise RuntimeError("persistence failed")

    monkeypatch.setattr( queue, "_persist_valid_payload", persist)
    with pytest.raises( RuntimeError, match = "persistence failed") :
        queue.enqueue(VALID_PAYLOAD)

    assert events == [ "insert_raw", "validate", "normalize" ]


def test_async_queue_persists_history_without_enqueuing( monkeypatch) -> None :
    events        = []
    storage       = _AsyncPayloadStorageStub(events)
    queue         = database_queue.AsyncWhatsAppDatabaseQueue("postgresql://test")
    queue.storage = storage

    async def enqueue_message( _msg_id : str) -> bool :
        raise AssertionError("History messages must not enter the live queue")

    monkeypatch.setattr( queue, "_enqueue_message", enqueue_message)
    result = asyncio.run(queue.enqueue(HISTORY_PAYLOAD))

    assert events == [
        "insert_raw",
        "upsert_business",
        "insert_history_chunk",
        "upsert_contact",
        "insert_message",
        "insert_message",
        "insert_message",
        "mark_valid",
    ]
    assert storage.history_chunk == {
        "phone_number" : 11,
        "payload"      : 7,
        "history_index" : 0,
        "phase"         : 1,
        "chunk_order"   : 4,
        "progress"      : 60,
        "errors"        : None,
    }
    assert [ message["is_echo"] for message in storage.messages ] == [
        False,
        True,
        False,
    ]
    assert storage.messages[0]["history_chunk"] == 29
    assert storage.messages[0]["history_context"] == { "status" : "read" }
    assert "history_context" not in storage.messages[0]["msg_data"]
    assert storage.messages[2]["msg_type"] == "errors"
    assert storage.messages[2]["msg_data"] == {
        "errors" : [
            {
                "code"    : 131051,
                "title"   : "Message type unknown",
            },
        ],
    }
    assert result == { "stored" : True, "enqueued" : False }


def test_async_queue_persists_history_completion_marker( monkeypatch) -> None :
    events        = []
    storage       = _AsyncPayloadStorageStub( events, inserted = False)
    queue         = database_queue.AsyncWhatsAppDatabaseQueue("postgresql://test")
    queue.storage = storage
    payload       = deepcopy(HISTORY_PAYLOAD)
    payload["entry"][0]["changes"][0]["value"]["history"] = [
        {
            "metadata" : {
                "phase"       : 2,
                "chunk_order" : 1,
                "progress"    : 100,
            },
        },
    ]

    async def enqueue_message( _msg_id : str) -> bool :
        raise AssertionError("History messages must not enter the live queue")

    monkeypatch.setattr( queue, "_enqueue_message", enqueue_message)
    result = asyncio.run(queue.enqueue(payload))

    assert events == [
        "insert_raw",
        "upsert_business",
        "insert_history_chunk",
        "mark_valid",
    ]
    assert storage.history_chunk == {
        "phone_number"  : 11,
        "payload"       : 7,
        "history_index" : 0,
        "phase"         : 2,
        "chunk_order"   : 1,
        "progress"      : 100,
        "errors"        : None,
    }
    assert storage.messages == []
    assert result == { "stored" : False, "enqueued" : False }


def test_async_queue_persists_smb_app_contacts_without_enqueuing(
    monkeypatch,
) -> None :
    events        = []
    storage       = _AsyncPayloadStorageStub(events)
    queue         = database_queue.AsyncWhatsAppDatabaseQueue("postgresql://test")
    queue.storage = storage

    async def enqueue_message( _msg_id : str) -> bool :
        raise AssertionError("Contact synchronization must not enter the live queue")

    monkeypatch.setattr( queue, "_enqueue_message", enqueue_message)
    result = asyncio.run(queue.enqueue(SMB_APP_STATE_SYNC_PAYLOAD))

    assert events == [
        "insert_raw",
        "upsert_business",
        "upsert_contact",
        "upsert_smb_app_contact",
        "upsert_contact",
        "upsert_smb_app_contact",
        "complete_smb_app_contact_sync",
        "mark_valid",
    ]
    assert [ contact["is_active"] for contact in storage.smb_contacts ] == [
        True,
        False,
    ]
    assert storage.smb_contacts[0]["full_name"] == "Pablo Morales"
    assert storage.smb_contacts[1]["full_name"] is None
    assert storage.smb_contacts[0]["changed_at"] == database_queue._timestamp(
        "1738346006"
    )
    assert result == { "stored" : True, "enqueued" : False }


def test_async_queue_reprocesses_duplicate_smb_app_contacts(
    monkeypatch,
) -> None :
    events        = []
    storage       = _AsyncPayloadStorageStub( events, inserted = False)
    queue         = database_queue.AsyncWhatsAppDatabaseQueue("postgresql://test")
    queue.storage = storage

    async def enqueue_message( _msg_id : str) -> bool :
        raise AssertionError("Contact synchronization must not enter the live queue")

    monkeypatch.setattr( queue, "_enqueue_message", enqueue_message)
    result = asyncio.run(queue.enqueue(SMB_APP_STATE_SYNC_PAYLOAD))

    assert events == [
        "insert_raw",
        "upsert_business",
        "upsert_contact",
        "upsert_smb_app_contact",
        "upsert_contact",
        "upsert_smb_app_contact",
        "complete_smb_app_contact_sync",
        "mark_valid",
    ]
    assert result == { "stored" : False, "enqueued" : False }


def test_async_queue_upgrades_history_media_placeholder( monkeypatch) -> None :
    events        = []
    storage       = _AsyncPayloadStorageStub(events)
    queue         = database_queue.AsyncWhatsAppDatabaseQueue("postgresql://test")
    queue.storage = storage

    async def enqueue_message( _msg_id : str) -> bool :
        raise AssertionError("History messages must not enter the live queue")

    monkeypatch.setattr( queue, "_enqueue_message", enqueue_message)
    result = asyncio.run(queue.enqueue(HISTORY_MEDIA_PAYLOAD))

    assert events == [
        "insert_raw",
        "upsert_business",
        "get_message",
        "insert_message",
        "mark_valid",
    ]
    assert storage.messages == [
        {
            "contact"         : 13,
            "payload"         : 5,
            "is_echo"         : True,
            "msg_id"          : "wamid.history-out",
            "msg_ts"          : database_queue._timestamp("1788724271"),
            "msg_type"        : "image",
            "msg_data" : {
                "image" : {
                    "id"        : "24230790383178626",
                    "mime_type" : "image/jpeg",
                    "sha256" : (
                        "P52U05n6YckRvBvEccgTpdA1zbG5tR8J6WOkFcbLDQw="
                    ),
                },
            },
            "history_chunk"   : 29,
            "history_context" : { "status" : "played" },
        }
    ]
    assert result == { "stored" : True, "enqueued" : False }
