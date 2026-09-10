import asyncio
import os

from datetime import (
    UTC,
    datetime,
)

import pytest


os.environ.setdefault( "BUCKET_REGION", "test-region")
os.environ.setdefault( "BUCKET_KEY_ID", "test-key")
os.environ.setdefault( "BUCKET_KEY_SECRET", "test-secret")
os.environ.setdefault( "BUCKET_NAME", "test-bucket")
os.environ.setdefault( "SUPABASE_DB_CONNECTION_URL_IPv4", "postgresql://test")


from wa_agents import S3_bucket_storage
from wa_agents import supabase
from wa_agents.S3_bucket_storage import (
    AsyncS3BucketStorage,
    S3BucketStorage,
    media_object_key,
)
from wa_agents.case_handler_models import (
    AssistantMsg,
    CaseManifest,
    MediaObject,
    ServerTextMsg,
)


def test_database_url_prefers_ipv4( monkeypatch) -> None :
    monkeypatch.setenv( "SUPABASE_DB_CONNECTION_URL_IPv4", "postgresql://ipv4")
    monkeypatch.setenv( "SUPABASE_DB_CONNECTION_URL_IPv6", "postgresql://ipv6")

    assert supabase.get_database_url() == "postgresql://ipv4"


def test_payload_hash_is_canonical() -> None :
    payload_1 = { "entry" : [ { "id" : "123" } ], "object" : "whatsapp" }
    payload_2 = { "object" : "whatsapp", "entry" : [ { "id" : "123" } ] }

    assert supabase._payload_hash(payload_1) == supabase._payload_hash(payload_2)


def test_inbound_payload_is_bound_as_jsonb( monkeypatch) -> None :
    storage = supabase.SyncSupabaseStorage("postgresql://test")
    calls   = []

    def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return { "id" : 7, "inserted" : True }

    monkeypatch.setattr( storage, "_fetch_one", fake_fetch_one)
    row = storage.insert_inbound_payload({ "hello" : "world" })

    assert row == { "id" : 7, "inserted" : True }
    assert calls[0][0] == supabase.SQL_INSERT_INBOUND_PAYLOAD
    assert calls[0][1]["data_raw"].obj == { "hello" : "world" }
    assert calls[0][1]["data_hash"] == supabase._payload_hash(
        { "hello" : "world" }
    )


def test_case_handler_message_round_trip_and_state_update( monkeypatch) -> None :
    storage = supabase.SyncSupabaseStorage("postgresql://test")
    message = ServerTextMsg( text = "hello", origin = "test")
    calls   = []

    def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return {
            "id"            : 41,
            "ts"            : message.ts,
            "case_id"       : 9,
            "basemodel"     : message.basemodel,
            "origin"        : message.origin,
            "data"          : params["data"].obj,
            "machine_state" : "awaiting_photo",
        }

    monkeypatch.setattr( storage, "_fetch_one", fake_fetch_one)
    stored = storage.insert_case_handler_message(
        case_id       = 9,
        message       = message,
        machine_state = "awaiting_photo",
    )

    assert isinstance( stored, ServerTextMsg)
    assert stored.id == 41
    assert stored.text == "hello"
    assert calls[0][0] == supabase.SQL_INSERT_CASE_HANDLER_MESSAGE
    assert calls[0][1]["machine_state"] == "awaiting_photo"


def test_llm_model_is_stored_with_assistant_message( monkeypatch) -> None :
    storage = supabase.SyncSupabaseStorage("postgresql://test")
    message = AssistantMsg( text = "hello", model = "gpt-test")
    calls   = []

    def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return None

    monkeypatch.setattr( storage, "_fetch_one", fake_fetch_one)
    storage.insert_case_handler_message(
        case_id       = 9,
        message       = message,
        machine_state = "confirm_drone_model",
    )

    assert calls[0][1]["data"].obj["model"] == "gpt-test"
    assert calls[0][1]["machine_state"] == "confirm_drone_model"


def test_manifest_loads_ordered_message_ids( monkeypatch) -> None :
    storage = supabase.SyncSupabaseStorage("postgresql://test")
    now     = datetime.now(UTC)

    monkeypatch.setattr(
        storage,
        "_fetch_one",
        lambda _sql, _params : {
            "id"            : 5,
            "contact"       : 3,
            "created_at"    : now,
            "updated_at"    : now,
            "is_open"       : True,
            "machine_state" : "confirm_model",
        },
    )
    monkeypatch.setattr(
        storage,
        "_fetch_all",
        lambda _sql, _params : [ { "id" : 11 }, { "id" : 12 } ],
    )

    manifest = storage.get_case_manifest( case_id = 5, contact = 3)

    assert isinstance( manifest, CaseManifest)
    assert manifest.machine_state == "confirm_model"
    assert manifest.message_ids == [ 11, 12 ]


def test_contact_leases_are_explicit_operations( monkeypatch) -> None :
    storage = supabase.SyncSupabaseStorage("postgresql://test")
    calls   = []

    def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return { "contact" : params["contact"] }

    monkeypatch.setattr( storage, "_fetch_one", fake_fetch_one)

    assert storage.acquire_contact_lease( 3, "token")
    assert storage.renew_contact_lease( 3, "token")
    assert storage.release_contact_lease( 3, "token") is True
    assert [ sql for sql, _params in calls ] == [
        supabase.SQL_ACQUIRE_CONTACT_LEASE,
        supabase.SQL_RENEW_CONTACT_LEASE,
        supabase.SQL_RELEASE_CONTACT_LEASE,
    ]
    assert not hasattr( storage, "__enter__")


def test_async_payload_insert_uses_same_contract( monkeypatch) -> None :
    storage = supabase.AsyncSupabaseStorage("postgresql://test")
    calls   = []

    async def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return { "id" : 8, "inserted" : True }

    monkeypatch.setattr( storage, "_fetch_one", fake_fetch_one)
    row = asyncio.run(storage.insert_inbound_payload({ "hello" : "async" }))

    assert row == { "id" : 8, "inserted" : True }
    assert calls[0][0] == supabase.SQL_INSERT_INBOUND_PAYLOAD
    assert calls[0][1]["data_raw"].obj == { "hello" : "async" }


def test_media_object_key_uses_business_contact_case_and_filename() -> None :
    assert media_object_key( 11, 17, 29, "31.jpeg") == "11/17/29/31.jpeg"


@pytest.mark.parametrize(
    "business_id, contact_id, case_id, filename",
    [
        ( 0, 1, 2, "file.jpeg"),
        ( 1, 0, 2, "file.jpeg"),
        ( 1, 1, -1, "file.jpeg"),
        ( 1, 1, 2, "nested/file.jpeg"),
        ( 1, 1, 2, ".."),
    ],
)
def test_media_object_key_rejects_invalid_components(
    business_id : int,
    contact_id : int,
    case_id    : int,
    filename   : str,
) -> None :
    with pytest.raises(ValueError) :
        media_object_key( business_id, contact_id, case_id, filename)


def test_s3_media_write_returns_database_object_key( monkeypatch) -> None :
    calls = []
    media = MediaObject(
        mime    = "image/jpeg",
        name    = "31.jpeg",
        content = b"image bytes",
    )

    monkeypatch.setattr(
        S3_bucket_storage,
        "b3_put_media",
        lambda *args : calls.append(args),
    )

    object_key = S3BucketStorage().media_write( 11, 17, 29, media)

    assert object_key == "11/17/29/31.jpeg"
    assert calls == [ ( "11/17/29/31.jpeg", b"image bytes", "image/jpeg") ]


def test_async_s3_media_write_returns_database_object_key( monkeypatch) -> None :
    calls = []
    media = MediaObject(
        mime    = "image/jpeg",
        name    = "31.jpeg",
        content = b"image bytes",
    )

    async def fake_put_media( *args) :
        calls.append(args)

    monkeypatch.setattr( S3_bucket_storage, "async_b3_put_media", fake_put_media)
    object_key = asyncio.run(AsyncS3BucketStorage().media_write( 11, 17, 29, media))

    assert object_key == "11/17/29/31.jpeg"
    assert calls == [ ( "11/17/29/31.jpeg", b"image bytes", "image/jpeg") ]
