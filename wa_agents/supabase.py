"""
Supabase PostgreSQL persistence.

All structured WhatsApp and case-handler data lives here. S3 is deliberately not
imported by this module; it stores media bytes only.
"""

from __future__ import annotations

import json
import os

from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from sofia_utils.psycopg import (
    Jsonb,
    async_pooled_connection,
    load_sql_script,
    sync_pooled_conection,
)
from sofia_utils.printing import get_qualname as here

from .case_handler_models import (
    CaseManifest,
    Message,
)


# =========================================================================================
# CONFIGURATION

DB_POOL_MIN_SIZE = 1
"""Database pool minimum size."""
DB_POOL_MAX_SIZE = 5
"""Database pool maximum size."""
DB_POOL_TIMEOUT  = 30
"""Database pool timeout in seconds."""


def get_database_url() -> str :
    """
    Return the configured Supabase PostgreSQL connection URL.
    """
    ipv4 = "SUPABASE_DB_CONNECTION_URL_IPv4"
    if ( db_url := os.getenv(ipv4) ) :
        return db_url
    
    ipv6 = ipv4.replace( "4", "6")
    if ( db_url := os.getenv(ipv6) ) :
        return db_url
    
    raise RuntimeError(
        f"In {here()}: Both env vars '{ipv4}' and '{ipv6}' are unset"
    )


# =========================================================================================
# SQL

SQL_DIR = Path(__file__).parent / "sql"


def _load_sql( filename : str) -> str :
    return load_sql_script( SQL_DIR / filename)


SQL_ACQUIRE_CONTACT_LEASE           = _load_sql("acquire_contact_lease.sql")
SQL_CASE_HANDLER_MESSAGE_EXISTS     = _load_sql("case_handler_message_exists.sql")
SQL_GET_CASE_HANDLER_MEDIA          = _load_sql("get_case_handler_media.sql")
SQL_GET_CASE_HANDLER_MESSAGE        = _load_sql("get_case_handler_message.sql")
SQL_GET_CASE_HANDLER_MESSAGES       = _load_sql("get_case_handler_messages.sql")
SQL_GET_CASE_MANIFEST               = _load_sql("get_case_manifest.sql")
SQL_GET_CASE_MESSAGE_IDS            = _load_sql("get_case_message_ids.sql")
SQL_GET_CONTACT                     = _load_sql("get_contact.sql")
SQL_GET_INBOUND_MESSAGE             = _load_sql("get_inbound_message.sql")
SQL_GET_INBOUND_PAYLOAD             = _load_sql("get_inbound_payload.sql")
SQL_GET_OPEN_CASE_MANIFEST          = _load_sql("get_open_case_manifest.sql")
SQL_INSERT_CASE_HANDLER_MESSAGE     = _load_sql("insert_case_handler_message.sql")
SQL_INSERT_CASE_MANIFEST            = _load_sql("insert_case_manifest.sql")
SQL_INSERT_CONTACT_PROFILE          = _load_sql("insert_contact_profile.sql")
SQL_INSERT_INBOUND_MESSAGE          = _load_sql("insert_inbound_message.sql")
SQL_INSERT_INBOUND_PAYLOAD          = _load_sql("insert_inbound_payload.sql")
SQL_INSERT_INBOUND_PAYLOAD_METADATA = _load_sql(
    "insert_inbound_payload_metadata.sql"
)
SQL_INSERT_MEDIA                    = _load_sql("insert_media.sql")
SQL_INSERT_OUTBOUND_MESSAGE         = _load_sql("insert_outbound_message.sql")
SQL_INSERT_STATUS                   = _load_sql("insert_status.sql")
SQL_LINK_CASE_HANDLER_TO_API        = _load_sql("link_case_handler_to_api.sql")
SQL_MARK_INBOUND_PAYLOAD_INVALID    = _load_sql("mark_inbound_payload_invalid.sql")
SQL_MARK_INBOUND_PAYLOAD_VALID      = _load_sql("mark_inbound_payload_valid.sql")
SQL_RELEASE_CONTACT_LEASE           = _load_sql("release_contact_lease.sql")
SQL_RENEW_CONTACT_LEASE             = _load_sql("renew_contact_lease.sql")
SQL_UPDATE_CASE_MANIFEST            = _load_sql("update_case_manifest.sql")
SQL_UPSERT_BUSINESS                 = _load_sql("upsert_business.sql")
SQL_UPSERT_CONTACT                  = _load_sql("upsert_contact.sql")


# =========================================================================================
# HELPERS

def _json_param( value : Any) -> Jsonb | None :
    
    return None if value is None else Jsonb(value)


def _json_compatible( value : dict[str, Any] | BaseModel) -> dict[str, Any] :
    
    if isinstance( value, BaseModel) :
        return value.model_dump( mode = "json", by_alias = True)
    
    return value


def _payload_hash( payload : dict[str, Any] | BaseModel) -> str :
    """
    Hash a canonical JSON representation of a raw webhook payload.
    """
    data      = _json_compatible(payload)
    canonical = json.dumps(
        data,
        ensure_ascii = False,
        separators   = ( ",", ":"),
        sort_keys    = True,
    )
    
    return sha256(canonical.encode("utf-8")).hexdigest()


def _message_data( message : Message) -> dict[str, Any] :
    
    return message.model_dump(
        mode    = "json",
        exclude = { "id", "ts", "basemodel", "origin" },
    )


def _message_from_row( row : dict[str, Any] | None) -> Message | None :
    
    if not row :
        return None
    
    from . import case_handler_models
    
    basemodel = row.get("basemodel")
    if not isinstance( basemodel, str) :
        raise ValueError(f"Invalid case-handler message model '{basemodel}'")
    
    MsgBM     = getattr( case_handler_models, basemodel, None)
    if not isinstance( MsgBM, type) or not issubclass( MsgBM, Message) :
        raise ValueError(f"Unknown case-handler message model '{basemodel}'")
    
    payload = dict(row.get("data") or {})
    payload.update(
        {
            "id"        : row["id"],
            "ts"        : row["ts"],
            "basemodel" : basemodel,
            "origin"    : row.get("origin"),
        }
    )
    
    return MsgBM.model_validate(payload)


def _manifest_from_row(
    row         : dict[str, Any] | None,
    message_ids : list[int] | None = None,
) -> CaseManifest | None :
    
    if not row :
        return None
    
    return CaseManifest(
        id            = row["id"],
        contact       = row["contact"],
        created_at    = row["created_at"],
        updated_at    = row.get("updated_at"),
        is_open       = row["is_open"],
        machine_state = row.get("machine_state"),
        message_ids   = message_ids or [],
    )


# =========================================================================================
# SEQUENTIAL ADAPTER

class SyncSupabaseStorage :
    """
    Sequential gateway for the normalized persistence SQL.
    """
    
    def __init__(
        self,
        database_url : str | None = None,
    ) -> None :
        
        self.database_url = database_url or get_database_url()
        return
    
    def _fetch_one(
        self,
        sql    : str,
        params : dict[str, Any],
    ) -> dict[str, Any] | None :
        
        with sync_pooled_conection(
            database_url = self.database_url,
            min_size     = DB_POOL_MIN_SIZE,
            max_size     = DB_POOL_MAX_SIZE,
            timeout      = DB_POOL_TIMEOUT,
        ) as conn :
            row = conn.execute( sql, params).fetchone()
        
        return dict(row) if row else None
    
    def _fetch_all(
        self,
        sql    : str,
        params : dict[str, Any],
    ) -> list[dict[str, Any]] :
        
        with sync_pooled_conection(
            database_url = self.database_url,
            min_size     = DB_POOL_MIN_SIZE,
            max_size     = DB_POOL_MAX_SIZE,
            timeout      = DB_POOL_TIMEOUT,
        ) as conn :
            rows = conn.execute( sql, params).fetchall()
        
        return [ dict(row) for row in rows ]
    
    # -------------------------------------------------------------------------------------
    # WHATSAPP API DATA
    
    def upsert_business(
        self,
        waba_id              : str,
        phone_number_id      : str,
        display_phone_number : str,
    ) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_UPSERT_BUSINESS,
            {
                "waba_id"              : waba_id,
                "phone_number_id"      : phone_number_id,
                "display_phone_number" : display_phone_number,
            },
        )
    
    def upsert_contact(
        self,
        business : int,
        wa_id    : str | None,
        user_id  : str | None,
    ) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_UPSERT_CONTACT,
            { "business" : business, "wa_id" : wa_id, "user_id" : user_id },
        )
    
    def get_contact( self, contact : int) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_GET_CONTACT,
            { "contact" : contact },
        )
    
    def insert_contact_profile(
        self,
        contact          : int,
        profile_name     : str,
        profile_username : str | None,
    ) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_INSERT_CONTACT_PROFILE,
            {
                "contact"          : contact,
                "profile_name"     : profile_name,
                "profile_username" : profile_username,
            },
        )
    
    def insert_inbound_payload(
        self,
        data_raw  : dict[str, Any] | BaseModel,
        data_hash : str | None = None,
    ) -> dict[str, Any] | None :
        
        raw = _json_compatible(data_raw)
        
        return self._fetch_one(
            SQL_INSERT_INBOUND_PAYLOAD,
            {
                "data_raw"  : Jsonb(raw),
                "data_hash" : data_hash or _payload_hash(raw),
            },
        )
    
    def get_inbound_payload(
        self,
        payload_id : int,
    ) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_GET_INBOUND_PAYLOAD,
            { "payload_id" : payload_id },
        )
    
    def mark_inbound_payload_valid(
        self,
        payload_id : int,
    ) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_MARK_INBOUND_PAYLOAD_VALID,
            { "payload_id" : payload_id },
        )
    
    def mark_inbound_payload_invalid(
        self,
        payload_id : int,
        errors     : list[dict[str, Any]] | dict[str, Any],
    ) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_MARK_INBOUND_PAYLOAD_INVALID,
            { "payload_id" : payload_id, "errors" : Jsonb(errors) },
        )
    
    def insert_inbound_payload_metadata(
        self,
        payload_id : int,
        contact    : int,
        item_idx   : int | None      = None,
        item_ts    : datetime | None = None,
        change_idx : int | None      = None,
    ) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_INSERT_INBOUND_PAYLOAD_METADATA,
            {
                "item_idx"   : item_idx,
                "item_ts"    : item_ts,
                "change_idx" : change_idx,
                "payload_id" : payload_id,
                "contact"    : contact,
            },
        )
    
    def insert_inbound_message(
        self,
        payload  : int,
        msg_id   : str,
        msg_ts   : datetime,
        msg_type : str,
        msg_data : dict[str, Any],
        is_echo  : bool | None = None,
    ) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_INSERT_INBOUND_MESSAGE,
            {
                "payload"  : payload,
                "is_echo"  : is_echo,
                "msg_id"   : msg_id,
                "msg_ts"   : msg_ts,
                "msg_type" : msg_type,
                "msg_data" : Jsonb(msg_data),
            },
        )
    
    def get_inbound_message(
        self,
        msg_id : str,
    ) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_GET_INBOUND_MESSAGE,
            { "msg_id" : msg_id },
        )
    
    def insert_outbound_message(
        self,
        contact  : int,
        msg_id   : str,
        msg_type : str,
        msg_data : dict[str, Any],
    ) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_INSERT_OUTBOUND_MESSAGE,
            {
                "contact"  : contact,
                "msg_id"   : msg_id,
                "msg_type" : msg_type,
                "msg_data" : Jsonb(msg_data),
            },
        )
    
    def insert_status(
        self,
        payload      : int,
        msg_id       : str,
        msg_status   : str,
        status_ts    : datetime,
        conversation : dict[str, Any] | None       = None,
        pricing      : dict[str, Any] | None       = None,
        errors       : list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_INSERT_STATUS,
            {
                "payload"      : payload,
                "msg_id"       : msg_id,
                "msg_status"   : msg_status,
                "status_ts"    : status_ts,
                "conversation" : _json_param(conversation),
                "pricing"      : _json_param(pricing),
                "errors"       : _json_param(errors),
            },
        )
    
    def insert_media(
        self,
        mime_type       : str,
        size            : int,
        object_key      : str,
        inbound_msg_id  : int | None = None,
        outbound_msg_id : int | None = None,
        caption         : str | None = None,
        filename        : str | None = None,
    ) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_INSERT_MEDIA,
            {
                "inbound_msg_id"  : inbound_msg_id,
                "outbound_msg_id" : outbound_msg_id,
                "mime_type"       : mime_type,
                "size"            : size,
                "object_key"      : object_key,
                "caption"         : caption,
                "filename"        : filename,
            },
        )
    
    # -------------------------------------------------------------------------------------
    # CONTACT LEASES
    
    def acquire_contact_lease(
        self,
        contact     : int,
        owner_token : UUID | str,
    ) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_ACQUIRE_CONTACT_LEASE,
            { "contact" : contact, "owner_token" : owner_token },
        )
    
    def renew_contact_lease(
        self,
        contact     : int,
        owner_token : UUID | str,
    ) -> dict[str, Any] | None :
        
        return self._fetch_one(
            SQL_RENEW_CONTACT_LEASE,
            { "contact" : contact, "owner_token" : owner_token },
        )
    
    def release_contact_lease(
        self,
        contact     : int,
        owner_token : UUID | str,
    ) -> bool :
        
        row = self._fetch_one(
            SQL_RELEASE_CONTACT_LEASE,
            { "contact" : contact, "owner_token" : owner_token },
        )
        return bool(row)
    
    # -------------------------------------------------------------------------------------
    # CASE HANDLER DATA
    
    def _message_ids( self, case_id : int) -> list[int] :
        
        rows = self._fetch_all(
            SQL_GET_CASE_MESSAGE_IDS,
            { "case_id" : case_id },
        )
        return [ row["id"] for row in rows ]
    
    def insert_case_manifest(
        self,
        contact       : int,
        machine_state : str | None,
    ) -> CaseManifest | None :
        
        row = self._fetch_one(
            SQL_INSERT_CASE_MANIFEST,
            { "contact" : contact, "machine_state" : machine_state },
        )
        return _manifest_from_row(row)
    
    def get_case_manifest(
        self,
        case_id : int,
        contact : int,
    ) -> CaseManifest | None :
        
        row = self._fetch_one(
            SQL_GET_CASE_MANIFEST,
            { "case_id" : case_id, "contact" : contact },
        )
        ids = self._message_ids(case_id) if row else []
        
        return _manifest_from_row( row, ids)
    
    def get_open_case_manifest(
        self,
        contact : int,
    ) -> CaseManifest | None :
        
        row = self._fetch_one(
            SQL_GET_OPEN_CASE_MANIFEST,
            { "contact" : contact },
        )
        ids = self._message_ids(row["id"]) if row else []
        
        return _manifest_from_row( row, ids)
    
    def update_case_manifest(
        self,
        manifest : CaseManifest,
    ) -> CaseManifest | None :
        
        row = self._fetch_one(
            SQL_UPDATE_CASE_MANIFEST,
            {
                "case_id"       : manifest.id,
                "contact"       : manifest.contact,
                "is_open"       : manifest.is_open,
                "machine_state" : manifest.machine_state,
            },
        )
        return _manifest_from_row( row, manifest.message_ids)
    
    def case_handler_message_exists(
        self,
        api_inbound_msg_id : int,
    ) -> bool :
        
        row = self._fetch_one(
            SQL_CASE_HANDLER_MESSAGE_EXISTS,
            { "api_inbound_msg_id" : api_inbound_msg_id },
        )
        return bool(row)
    
    def insert_case_handler_message(
        self,
        case_id       : int,
        message       : Message,
        machine_state : str | None,
    ) -> Message | None :
        
        row = self._fetch_one(
            SQL_INSERT_CASE_HANDLER_MESSAGE,
            {
                "case_id"       : case_id,
                "ts"            : message.ts,
                "basemodel"     : message.basemodel,
                "origin"        : message.origin,
                "data"          : Jsonb(_message_data(message)),
                "machine_state" : machine_state,
            },
        )
        return _message_from_row(row)
    
    def get_case_handler_message(
        self,
        case_id    : int,
        message_id : int,
    ) -> Message | None :
        
        row = self._fetch_one(
            SQL_GET_CASE_HANDLER_MESSAGE,
            { "case_id" : case_id, "message_id" : message_id },
        )
        return _message_from_row(row)
    
    def get_case_handler_messages(
        self,
        case_id : int,
    ) -> list[Message] :
        
        rows = self._fetch_all(
            SQL_GET_CASE_HANDLER_MESSAGES,
            { "case_id" : case_id },
        )
        return [
            message for row in rows if ( message := _message_from_row(row) )
        ]
    
    def link_case_handler_to_api(
        self,
        case_handler_msg_id : int,
        api_inbound_msg_id  : int | None = None,
        api_outbound_msg_id : int | None = None,
    ) -> dict[str, Any] | None :
        """
        Link one side of the API boundary.
        
        None can mean an idempotent duplicate or a rejected conflicting mapping;
        callers that need to distinguish those cases must inspect existing links.
        """
        return self._fetch_one(
            SQL_LINK_CASE_HANDLER_TO_API,
            {
                "case_handler_msg_id" : case_handler_msg_id,
                "api_inbound_msg_id"  : api_inbound_msg_id,
                "api_outbound_msg_id" : api_outbound_msg_id,
            },
        )
    
    def get_case_handler_media(
        self,
        case_handler_msg_id : int,
    ) -> list[dict[str, Any]] :
        
        return self._fetch_all(
            SQL_GET_CASE_HANDLER_MEDIA,
            { "case_handler_msg_id" : case_handler_msg_id },
        )


# =========================================================================================
# ASYNCHRONOUS ADAPTER

class AsyncSupabaseStorage :
    """
    Asynchronous gateway for the normalized persistence SQL.
    """

    def __init__(
        self,
        database_url : str | None = None,
    ) -> None :
        
        self.database_url = database_url or get_database_url()
        return
    
    async def _fetch_one(
        self,
        sql    : str,
        params : dict[str, Any],
    ) -> dict[str, Any] | None :
        
        async with async_pooled_connection(
            database_url = self.database_url,
            min_size     = DB_POOL_MIN_SIZE,
            max_size     = DB_POOL_MAX_SIZE,
            timeout      = DB_POOL_TIMEOUT,
        ) as conn :
            cursor = await conn.execute( sql, params)
            row    = await cursor.fetchone()
        
        return dict(row) if row else None
    
    async def _fetch_all(
        self,
        sql    : str,
        params : dict[str, Any],
    ) -> list[dict[str, Any]] :
        
        async with async_pooled_connection(
            database_url = self.database_url,
            min_size     = DB_POOL_MIN_SIZE,
            max_size     = DB_POOL_MAX_SIZE,
            timeout      = DB_POOL_TIMEOUT,
        ) as conn :
            cursor = await conn.execute( sql, params)
            rows   = await cursor.fetchall()
        
        return [ dict(row) for row in rows ]
    
    # -------------------------------------------------------------------------------------
    # WHATSAPP API DATA
    
    async def upsert_business(
        self,
        waba_id              : str,
        phone_number_id      : str,
        display_phone_number : str,
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_UPSERT_BUSINESS,
            {
                "waba_id"              : waba_id,
                "phone_number_id"      : phone_number_id,
                "display_phone_number" : display_phone_number,
            },
        )
    
    async def upsert_contact(
        self,
        business : int,
        wa_id    : str | None,
        user_id  : str | None,
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_UPSERT_CONTACT,
            { "business" : business, "wa_id" : wa_id, "user_id" : user_id },
        )
    
    async def get_contact(
        self,
        contact : int,
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_GET_CONTACT,
            { "contact" : contact },
        )
    
    async def insert_contact_profile(
        self,
        contact          : int,
        profile_name     : str,
        profile_username : str | None,
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_INSERT_CONTACT_PROFILE,
            {
                "contact"          : contact,
                "profile_name"     : profile_name,
                "profile_username" : profile_username,
            },
        )
    
    async def insert_inbound_payload(
        self,
        data_raw  : dict[str, Any] | BaseModel,
        data_hash : str | None = None,
    ) -> dict[str, Any] | None :
        
        raw = _json_compatible(data_raw)
        
        return await self._fetch_one(
            SQL_INSERT_INBOUND_PAYLOAD,
            {
                "data_raw"  : Jsonb(raw),
                "data_hash" : data_hash or _payload_hash(raw),
            },
        )
    
    async def get_inbound_payload(
        self,
        payload_id : int,
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_GET_INBOUND_PAYLOAD,
            { "payload_id" : payload_id },
        )
    
    async def mark_inbound_payload_valid(
        self,
        payload_id : int,
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_MARK_INBOUND_PAYLOAD_VALID,
            { "payload_id" : payload_id },
        )
    
    async def mark_inbound_payload_invalid(
        self,
        payload_id : int,
        errors     : list[dict[str, Any]] | dict[str, Any],
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_MARK_INBOUND_PAYLOAD_INVALID,
            { "payload_id" : payload_id, "errors" : Jsonb(errors) },
        )
    
    async def insert_inbound_payload_metadata(
        self,
        payload_id : int,
        contact    : int,
        item_idx   : int | None      = None,
        item_ts    : datetime | None = None,
        change_idx : int | None      = None,
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_INSERT_INBOUND_PAYLOAD_METADATA,
            {
                "item_idx"   : item_idx,
                "item_ts"    : item_ts,
                "change_idx" : change_idx,
                "payload_id" : payload_id,
                "contact"    : contact,
            },
        )
    
    async def insert_inbound_message(
        self,
        payload  : int,
        msg_id   : str,
        msg_ts   : datetime,
        msg_type : str,
        msg_data : dict[str, Any],
        is_echo  : bool | None = None,
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_INSERT_INBOUND_MESSAGE,
            {
                "payload"  : payload,
                "is_echo"  : is_echo,
                "msg_id"   : msg_id,
                "msg_ts"   : msg_ts,
                "msg_type" : msg_type,
                "msg_data" : Jsonb(msg_data),
            },
        )
    
    async def get_inbound_message(
        self,
        msg_id : str,
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_GET_INBOUND_MESSAGE,
            { "msg_id" : msg_id },
        )
    
    async def insert_outbound_message(
        self,
        contact  : int,
        msg_id   : str,
        msg_type : str,
        msg_data : dict[str, Any],
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_INSERT_OUTBOUND_MESSAGE,
            {
                "contact"  : contact,
                "msg_id"   : msg_id,
                "msg_type" : msg_type,
                "msg_data" : Jsonb(msg_data),
            },
        )
    
    async def insert_status(
        self,
        payload      : int,
        msg_id       : str,
        msg_status   : str,
        status_ts    : datetime,
        conversation : dict[str, Any] | None       = None,
        pricing      : dict[str, Any] | None       = None,
        errors       : list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_INSERT_STATUS,
            {
                "payload"      : payload,
                "msg_id"       : msg_id,
                "msg_status"   : msg_status,
                "status_ts"    : status_ts,
                "conversation" : _json_param(conversation),
                "pricing"      : _json_param(pricing),
                "errors"       : _json_param(errors),
            },
        )
    
    async def insert_media(
        self,
        mime_type       : str,
        size            : int,
        object_key      : str,
        inbound_msg_id  : int | None = None,
        outbound_msg_id : int | None = None,
        caption         : str | None = None,
        filename        : str | None = None,
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_INSERT_MEDIA,
            {
                "inbound_msg_id"  : inbound_msg_id,
                "outbound_msg_id" : outbound_msg_id,
                "mime_type"       : mime_type,
                "size"            : size,
                "object_key"      : object_key,
                "caption"         : caption,
                "filename"        : filename,
            },
        )
    
    # -------------------------------------------------------------------------------------
    # CONTACT LEASES
    
    async def acquire_contact_lease(
        self,
        contact     : int,
        owner_token : UUID | str,
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_ACQUIRE_CONTACT_LEASE,
            { "contact" : contact, "owner_token" : owner_token },
        )
    
    async def renew_contact_lease(
        self,
        contact     : int,
        owner_token : UUID | str,
    ) -> dict[str, Any] | None :
        
        return await self._fetch_one(
            SQL_RENEW_CONTACT_LEASE,
            { "contact" : contact, "owner_token" : owner_token },
        )
    
    async def release_contact_lease(
        self,
        contact     : int,
        owner_token : UUID | str,
    ) -> bool :
        
        row = await self._fetch_one(
            SQL_RELEASE_CONTACT_LEASE,
            { "contact" : contact, "owner_token" : owner_token },
        )
        return bool(row)
    
    # -------------------------------------------------------------------------------------
    # CASE HANDLER DATA
    
    async def _message_ids(
        self,
        case_id : int,
    ) -> list[int] :
        
        rows = await self._fetch_all(
            SQL_GET_CASE_MESSAGE_IDS,
            { "case_id" : case_id },
        )
        return [ row["id"] for row in rows ]
    
    async def insert_case_manifest(
        self,
        contact       : int,
        machine_state : str | None,
    ) -> CaseManifest | None :
        
        row = await self._fetch_one(
            SQL_INSERT_CASE_MANIFEST,
            { "contact" : contact, "machine_state" : machine_state },
        )
        return _manifest_from_row(row)
    
    async def get_case_manifest(
        self,
        case_id : int,
        contact : int,
    ) -> CaseManifest | None :
        
        row = await self._fetch_one(
            SQL_GET_CASE_MANIFEST,
            { "case_id" : case_id, "contact" : contact },
        )
        ids = await self._message_ids(case_id) if row else []
        
        return _manifest_from_row( row, ids)
    
    async def get_open_case_manifest(
        self,
        contact : int,
    ) -> CaseManifest | None :
        
        row = await self._fetch_one(
            SQL_GET_OPEN_CASE_MANIFEST,
            { "contact" : contact },
        )
        ids = await self._message_ids(row["id"]) if row else []
        
        return _manifest_from_row( row, ids)
    
    async def update_case_manifest(
        self,
        manifest : CaseManifest,
    ) -> CaseManifest | None :
        
        row = await self._fetch_one(
            SQL_UPDATE_CASE_MANIFEST,
            {
                "case_id"       : manifest.id,
                "contact"       : manifest.contact,
                "is_open"       : manifest.is_open,
                "machine_state" : manifest.machine_state,
            },
        )
        return _manifest_from_row( row, manifest.message_ids)
    
    async def case_handler_message_exists(
        self,
        api_inbound_msg_id : int,
    ) -> bool :
        
        row = await self._fetch_one(
            SQL_CASE_HANDLER_MESSAGE_EXISTS,
            { "api_inbound_msg_id" : api_inbound_msg_id },
        )
        return bool(row)
    
    async def insert_case_handler_message(
        self,
        case_id       : int,
        message       : Message,
        machine_state : str | None,
    ) -> Message | None :
        
        row = await self._fetch_one(
            SQL_INSERT_CASE_HANDLER_MESSAGE,
            {
                "case_id"       : case_id,
                "ts"            : message.ts,
                "basemodel"     : message.basemodel,
                "origin"        : message.origin,
                "data"          : Jsonb(_message_data(message)),
                "machine_state" : machine_state,
            },
        )
        return _message_from_row(row)
    
    async def get_case_handler_message(
        self,
        case_id    : int,
        message_id : int,
    ) -> Message | None :
        
        row = await self._fetch_one(
            SQL_GET_CASE_HANDLER_MESSAGE,
            { "case_id" : case_id, "message_id" : message_id },
        )
        return _message_from_row(row)
    
    async def get_case_handler_messages(
        self,
        case_id : int,
    ) -> list[Message] :
        
        rows = await self._fetch_all(
            SQL_GET_CASE_HANDLER_MESSAGES,
            { "case_id" : case_id },
        )
        return [
            message for row in rows if ( message := _message_from_row(row) )
        ]
    
    async def link_case_handler_to_api(
        self,
        case_handler_msg_id : int,
        api_inbound_msg_id  : int | None = None,
        api_outbound_msg_id : int | None = None,
    ) -> dict[str, Any] | None :
        """
        Link one side of the API boundary.
        
        None can mean an idempotent duplicate or a rejected conflicting mapping;
        callers that need to distinguish those cases must inspect existing links.
        """
        return await self._fetch_one(
            SQL_LINK_CASE_HANDLER_TO_API,
            {
                "case_handler_msg_id" : case_handler_msg_id,
                "api_inbound_msg_id"  : api_inbound_msg_id,
                "api_outbound_msg_id" : api_outbound_msg_id,
            },
        )
    
    async def get_case_handler_media(
        self,
        case_handler_msg_id : int,
    ) -> list[dict[str, Any]] :
        
        return await self._fetch_all(
            SQL_GET_CASE_HANDLER_MEDIA,
            { "case_handler_msg_id" : case_handler_msg_id },
        )
