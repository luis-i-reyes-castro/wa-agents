import asyncio
import os

from datetime import (
    UTC,
    datetime,
)

import pytest

from cachetools import LRUCache


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


def _use_media_cache(
    monkeypatch,
    max_bytes : int,
) -> LRUCache[ str, bytes] :
    cache = LRUCache(
        maxsize   = max_bytes,
        getsizeof = len,
    )
    monkeypatch.setattr( S3_bucket_storage, "_media_cache", cache)
    
    return cache


def test_database_url_prefers_ipv4( monkeypatch) -> None :
    monkeypatch.setenv( "SUPABASE_DB_CONNECTION_URL_IPv4", "postgresql://ipv4")
    monkeypatch.setenv( "SUPABASE_DB_CONNECTION_URL_IPv6", "postgresql://ipv6")

    assert supabase.get_database_url() == "postgresql://ipv4"


def test_payload_hash_is_canonical() -> None :
    payload_1 = { "entry" : [ { "id" : "123" } ], "object" : "whatsapp" }
    payload_2 = { "object" : "whatsapp", "entry" : [ { "id" : "123" } ] }

    assert supabase._payload_hash(payload_1) == supabase._payload_hash(payload_2)


def test_get_business_uses_business_row_id( monkeypatch) -> None :
    storage = supabase.SyncSupabaseStorage("postgresql://test")
    calls   = []

    def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return { "id" : params["business"] }

    monkeypatch.setattr( storage, "_fetch_one", fake_fetch_one)

    assert storage.get_business(41) == { "id" : 41 }
    assert calls == [ ( supabase.SQL_GET_BUSINESS, { "business" : 41 } ) ]


def test_async_get_business_uses_business_row_id( monkeypatch) -> None :
    storage = supabase.AsyncSupabaseStorage("postgresql://test")
    calls   = []

    async def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return { "id" : params["business"] }

    monkeypatch.setattr( storage, "_fetch_one", fake_fetch_one)

    assert asyncio.run(storage.get_business(41)) == { "id" : 41 }
    assert calls == [ ( supabase.SQL_GET_BUSINESS, { "business" : 41 } ) ]


def test_resolve_business_and_contact( monkeypatch) -> None :
    storage = supabase.SyncSupabaseStorage("postgresql://test")

    monkeypatch.setattr(
        storage,
        "get_business",
        lambda _business : {
            "id"                   : 41,
            "waba_id"              : "123456789012345",
            "phone_number_id"      : "1234567890",
            "display_phone_number" : "15551234567",
        },
    )
    monkeypatch.setattr(
        storage,
        "get_contact",
        lambda _contact : {
            "id"               : 31,
            "business"         : 41,
            "wa_id"            : "593995341161",
            "user_id"          : None,
            "profile_name"     : "Test User",
            "profile_username" : None,
        },
    )

    business, contact = storage.resolve_business_and_contact( 41, 31)

    assert business.row_id == 41
    assert business.api_id == "1234567890"
    assert contact.row_id == 31
    assert contact.api_id == "593995341161"
    assert contact.profile and contact.profile.name == "Test User"


def test_async_resolve_business_and_contact( monkeypatch) -> None :
    storage = supabase.AsyncSupabaseStorage("postgresql://test")

    async def get_business( _business) :
        return {
            "id"                   : 41,
            "waba_id"              : "123456789012345",
            "phone_number_id"      : "1234567890",
            "display_phone_number" : "15551234567",
        }

    async def get_contact( _contact) :
        return {
            "id"               : 31,
            "business"         : 41,
            "wa_id"            : "593995341161",
            "user_id"          : None,
            "profile_name"     : "Test User",
            "profile_username" : None,
        }

    monkeypatch.setattr( storage, "get_business", get_business)
    monkeypatch.setattr( storage, "get_contact", get_contact)

    business, contact = asyncio.run(storage.resolve_business_and_contact( 41, 31))

    assert business.row_id == 41
    assert business.api_id == "1234567890"
    assert contact.row_id == 31
    assert contact.api_id == "593995341161"
    assert contact.profile and contact.profile.name == "Test User"


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
            "message_index" : 0,
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
    assert stored.message_index == 0
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
            "case_index"    : 2,
            "created_at"    : now,
            "updated_at"    : now,
            "is_open"       : True,
            "handler_id"    : 7,
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
    assert manifest.handler_id == 7
    assert manifest.case_index == 2
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
    assert media_object_key( 11, 17, 29, "31.jpeg") == "11/17/29_31.jpeg"


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


def test_media_cache_configuration( monkeypatch) -> None :
    monkeypatch.delenv( "WA_AGENTS_MEDIA_CACHE_MB", raising = False)
    assert (
        S3_bucket_storage._media_cache_size() ==
        S3_bucket_storage.MEDIA_CACHE_DEFAULT_MB * 1024 * 1024
    )
    
    monkeypatch.setenv( "WA_AGENTS_MEDIA_CACHE_MB", "0")
    assert S3_bucket_storage._media_cache_size() == 0


@pytest.mark.parametrize(
    "configured",
    [ "invalid", "-1" ],
)
def test_media_cache_configuration_rejects_invalid_limits(
    monkeypatch,
    configured : str,
) -> None :
    monkeypatch.setenv( "WA_AGENTS_MEDIA_CACHE_MB", configured)
    
    with pytest.raises(ValueError) :
        S3_bucket_storage._media_cache_size()


def test_media_cache_evicts_least_recently_used_content( monkeypatch) -> None :
    cache = _use_media_cache( monkeypatch, 4)
    S3_bucket_storage._media_cache_set( "first",  b"aa")
    S3_bucket_storage._media_cache_set( "second", b"bb")
    
    assert S3_bucket_storage._media_cache_get("first") == b"aa"
    
    S3_bucket_storage._media_cache_set( "third", b"cc")
    
    assert S3_bucket_storage._media_cache_get("second") is None
    assert S3_bucket_storage._media_cache_get("first") == b"aa"
    assert S3_bucket_storage._media_cache_get("third") == b"cc"
    assert cache.currsize == 4


def test_media_cache_replaces_and_bypasses_content_by_size( monkeypatch) -> None :
    cache = _use_media_cache( monkeypatch, 4)
    S3_bucket_storage._media_cache_set( "media", b"abc")
    S3_bucket_storage._media_cache_set( "media", b"de")
    
    assert S3_bucket_storage._media_cache_get("media") == b"de"
    assert cache.currsize == 2
    
    S3_bucket_storage._media_cache_set( "media", b"oversized")
    
    assert S3_bucket_storage._media_cache_get("media") is None
    assert cache.currsize == 0


def test_media_cache_can_be_disabled( monkeypatch) -> None :
    cache = _use_media_cache( monkeypatch, 0)
    S3_bucket_storage._media_cache_set( "media", b"content")
    
    assert S3_bucket_storage._media_cache_get("media") is None
    assert cache.currsize == 0


def test_s3_media_write_returns_database_object_key( monkeypatch) -> None :
    calls = []
    _use_media_cache( monkeypatch, 100)
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

    assert object_key == "11/17/29_31.jpeg"
    assert calls == [ ( "11/17/29_31.jpeg", b"image bytes", "image/jpeg") ]
    assert S3_bucket_storage._media_cache_get(object_key) == b"image bytes"


def test_s3_media_write_failure_does_not_populate_cache( monkeypatch) -> None :
    _use_media_cache( monkeypatch, 100)
    media = MediaObject(
        mime    = "image/jpeg",
        name    = "31.jpeg",
        content = b"image bytes",
    )
    
    def fail_write( *_args) -> None :
        raise RuntimeError("write failed")
    
    monkeypatch.setattr( S3_bucket_storage, "b3_put_media", fail_write)
    
    with pytest.raises( RuntimeError, match = "write failed") :
        S3BucketStorage().media_write( 11, 17, 29, media)
    
    assert S3_bucket_storage._media_cache_get("11/17/29_31.jpeg") is None


def test_s3_media_read_uses_read_through_cache( monkeypatch) -> None :
    _use_media_cache( monkeypatch, 100)
    calls = []
    
    def read_media( object_key : str) -> bytes :
        calls.append(object_key)
        return b"image bytes"
    
    monkeypatch.setattr( S3_bucket_storage, "b3_get_file", read_media)
    storage = S3BucketStorage()
    
    assert storage.media_read("11/17/29_31.jpeg") == b"image bytes"
    assert storage.media_read("11/17/29_31.jpeg") == b"image bytes"
    assert calls == [ "11/17/29_31.jpeg" ]


def test_s3_media_read_failure_does_not_populate_cache( monkeypatch) -> None :
    _use_media_cache( monkeypatch, 100)
    
    def fail_read( _object_key : str) -> bytes :
        raise RuntimeError("read failed")
    
    monkeypatch.setattr( S3_bucket_storage, "b3_get_file", fail_read)
    
    with pytest.raises( RuntimeError, match = "read failed") :
        S3BucketStorage().media_read("11/17/29_31.jpeg")
    
    assert S3_bucket_storage._media_cache_get("11/17/29_31.jpeg") is None


def test_s3_media_delete_invalidates_cache_after_success( monkeypatch) -> None :
    _use_media_cache( monkeypatch, 100)
    S3_bucket_storage._media_cache_set( "11/17/29_31.jpeg", b"image bytes")
    calls = []
    
    monkeypatch.setattr(
        S3_bucket_storage,
        "b3_delete",
        lambda object_key : calls.append(object_key),
    )
    S3BucketStorage().media_delete("11/17/29_31.jpeg")
    
    assert calls == [ "11/17/29_31.jpeg" ]
    assert S3_bucket_storage._media_cache_get("11/17/29_31.jpeg") is None


def test_s3_media_delete_failure_keeps_cached_content( monkeypatch) -> None :
    _use_media_cache( monkeypatch, 100)
    S3_bucket_storage._media_cache_set( "11/17/29_31.jpeg", b"image bytes")
    
    def fail_delete( _object_key : str) -> None :
        raise RuntimeError("delete failed")
    
    monkeypatch.setattr( S3_bucket_storage, "b3_delete", fail_delete)
    
    with pytest.raises( RuntimeError, match = "delete failed") :
        S3BucketStorage().media_delete("11/17/29_31.jpeg")
    
    assert (
        S3_bucket_storage._media_cache_get("11/17/29_31.jpeg") == b"image bytes"
    )


def test_async_s3_media_write_returns_database_object_key( monkeypatch) -> None :
    calls = []
    _use_media_cache( monkeypatch, 100)
    media = MediaObject(
        mime    = "image/jpeg",
        name    = "31.jpeg",
        content = b"image bytes",
    )

    async def fake_put_media( *args) :
        calls.append(args)

    monkeypatch.setattr( S3_bucket_storage, "async_b3_put_media", fake_put_media)
    object_key = asyncio.run(AsyncS3BucketStorage().media_write( 11, 17, 29, media))

    assert object_key == "11/17/29_31.jpeg"
    assert calls == [ ( "11/17/29_31.jpeg", b"image bytes", "image/jpeg") ]
    assert S3_bucket_storage._media_cache_get(object_key) == b"image bytes"


def test_async_s3_media_read_and_delete_use_cache( monkeypatch) -> None :
    _use_media_cache( monkeypatch, 100)
    read_calls   = []
    delete_calls = []
    
    async def read_media( object_key : str) -> bytes :
        read_calls.append(object_key)
        return b"image bytes"
    
    async def delete_media( object_key : str) -> None :
        delete_calls.append(object_key)
        return
    
    monkeypatch.setattr( S3_bucket_storage, "async_b3_get_file", read_media)
    monkeypatch.setattr( S3_bucket_storage, "async_b3_delete", delete_media)
    storage = AsyncS3BucketStorage()
    
    async def exercise() -> None :
        assert await storage.media_read("11/17/29_31.jpeg") == b"image bytes"
        assert await storage.media_read("11/17/29_31.jpeg") == b"image bytes"
        await storage.media_delete("11/17/29_31.jpeg")
    
    asyncio.run(exercise())
    
    assert read_calls == [ "11/17/29_31.jpeg" ]
    assert delete_calls == [ "11/17/29_31.jpeg" ]
    assert S3_bucket_storage._media_cache_get("11/17/29_31.jpeg") is None


def test_sync_and_async_s3_storage_share_media_cache( monkeypatch) -> None :
    _use_media_cache( monkeypatch, 100)
    media = MediaObject(
        mime    = "image/jpeg",
        name    = "31.jpeg",
        content = b"image bytes",
    )
    
    monkeypatch.setattr( S3_bucket_storage, "b3_put_media", lambda *_args : None)
    object_key = S3BucketStorage().media_write( 11, 17, 29, media)
    
    async def fail_read( _object_key : str) -> bytes :
        raise AssertionError("cached media should not be read from S3")
    
    monkeypatch.setattr( S3_bucket_storage, "async_b3_get_file", fail_read)
    content = asyncio.run(AsyncS3BucketStorage().media_read(object_key))
    
    assert content == b"image bytes"
