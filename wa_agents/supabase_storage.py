"""
Supabase PostgreSQL storage for WhatsApp case data.
"""

from __future__ import annotations

import os

from datetime import (
    datetime,
    timezone,
)
from hashlib import sha256
from inspect import currentframe
from pathlib import Path
from pydantic import BaseModel
from types import TracebackType
from typing import Any

from sofia_utils.psycopg import (
    Jsonb,
    async_pooled_connection,
    load_sql_script,
    sync_pooled_conection,
)
from sofia_utils.stamps import utc_iso_to_dt

from .basemodels import (
    CaseManifest,
    MediaContent,
    Message,
    UserContentMsg,
    WhatsAppMsg,
    WhatsAppPayload,
    WhatsAppStatus,
    WhatsAppValue,
)
from .do_bucket_io import (
    b3_exists,
    b3_get_file,
    b3_put_media,
    async_b3_exists,
    async_b3_get_file,
    async_b3_put_media,
)


# =========================================================================================
# CONFIGURATION

DB_POOL_MIN_SIZE = 1
""" Database pool minimum size """
DB_POOL_MAX_SIZE = 5
""" Database pool maximum size """
DB_POOL_TIMEOUT  = 30
""" Database pool timeout """


def get_database_url() -> str :
    """
    Retrieve the Supabase database URL from the standard project environment. \\
    Returns:
        The IPv4 connection URL when available, else the IPv6 URL.
    """
    if ( database_url := os.getenv("SUPABASE_DB_CONNECTION_URL_IPv4") ) \
    or ( database_url := os.getenv("SUPABASE_DB_CONNECTION_URL_IPv6") ) :
        return database_url
    
    here  = currentframe().f_code.co_name
    e_msg = (
        "Environment variables 'SUPABASE_DB_CONNECTION_URL_IPv4' "
        "and 'SUPABASE_DB_CONNECTION_URL_IPv6' are both unset"
    )
    raise RuntimeError(f"In {here}: {e_msg}")


# =========================================================================================
# SQL

SQL_DIR                  = Path(__file__).parent / "sql"
SQL_ENSURE_USER          = load_sql_script( SQL_DIR / "ensure_user.sql" )
SQL_GET_USER_DATA        = load_sql_script( SQL_DIR / "get_user_data.sql" )
SQL_UPSERT_USER_DATA     = load_sql_script( SQL_DIR / "upsert_user_data.sql" )
SQL_GET_CASE_INDEX       = load_sql_script( SQL_DIR / "get_case_index.sql" )
SQL_SET_CASE_INDEX       = load_sql_script( SQL_DIR / "set_case_index.sql" )
SQL_GET_NEXT_CASE_ID     = load_sql_script( SQL_DIR / "get_next_case_id.sql" )
SQL_GET_CASE             = load_sql_script( SQL_DIR / "get_case.sql" )
SQL_UPSERT_CASE          = load_sql_script( SQL_DIR / "upsert_case.sql" )
SQL_GET_CASE_MESSAGE_IDS = load_sql_script( SQL_DIR / "get_case_message_ids.sql" )
SQL_GET_MESSAGE          = load_sql_script( SQL_DIR / "get_message.sql" )
SQL_GET_MESSAGES         = load_sql_script( SQL_DIR / "get_messages.sql" )
SQL_INSERT_MESSAGE       = load_sql_script( SQL_DIR / "insert_message.sql" )
SQL_DEDUP_EXISTS         = load_sql_script( SQL_DIR / "dedup_exists.sql" )
SQL_INSERT_WEBHOOK_PAYLOAD = load_sql_script(
    SQL_DIR / "insert_webhook_payload.sql"
)
SQL_INSERT_WEBHOOK_MESSAGE = load_sql_script(
    SQL_DIR / "insert_webhook_message.sql"
)
SQL_INSERT_WEBHOOK_STATUS = load_sql_script(
    SQL_DIR / "insert_webhook_status.sql"
)


# =========================================================================================
# HELPERS

def _dt_to_utc_iso( value : Any) -> str | None :
    
    if value is None :
        return None
    
    if isinstance( value, datetime) :
        dt_obj = value
    elif isinstance( value, str) :
        dt_obj = utc_iso_to_dt(value)
    else :
        return str(value)
    
    if not dt_obj :
        return None
    
    if not dt_obj.tzinfo :
        dt_obj = dt_obj.replace( tzinfo = timezone.utc)
    
    return dt_obj.astimezone(timezone.utc).isoformat().replace( "+00:00", "Z")


def _dt_param( value : str | None) -> datetime | None :
    
    return utc_iso_to_dt(value) if value else None


def _unix_dt_param( value : str | None) -> datetime | None :
    
    if not value :
        return None
    
    try :
        return datetime.fromtimestamp( int(value), timezone.utc)
    except ( TypeError, ValueError, OSError ) :
        return None


def _message_from_payload( payload : dict[str, Any] | None) -> Message | None :
    
    if not payload :
        return None
    
    msg_bm = payload.get("basemodel")
    if not ( msg_bm and isinstance( msg_bm, str) ) :
        return None
    
    from . import basemodels
    
    MsgBM = getattr( basemodels, msg_bm, None)
    if MsgBM and issubclass( MsgBM, BaseModel) :
        return MsgBM.model_validate(payload)
    
    return None


def _default_user_data( user_id : str) -> dict[str, Any] :
    
    return { "user_id" : user_id, "names" : [] }


def _canonical_payload_json( payload : WhatsAppPayload) -> str :
    
    return payload.model_dump_json( by_alias = True)


def _payload_hash( payload : WhatsAppPayload) -> str :
    
    payload_json = _canonical_payload_json(payload)
    
    return sha256( payload_json.encode("utf-8")).hexdigest()


def _webhook_message_params(
    payload_id : int,
    waba_id    : str,
    value      : WhatsAppValue,
    message    : WhatsAppMsg,
) -> dict[str, Any] :
    
    media_data = message.media_data
    
    return {
        "payload_id"           : payload_id,
        "operator_id"          : value.metadata.phone_number_id,
        "display_phone_number" : value.metadata.display_phone_number,
        "waba_id"              : waba_id,
        "user_id"              : message.user,
        "message_id"           : message.id,
        "message_type"         : message.type,
        "timestamp"            : _unix_dt_param(message.timestamp),
        "media_id"             : media_data.id if media_data else None,
        "media_mime_type"      : media_data.mime_type if media_data else None,
        "context_message_id"   : message.context.id if message.context else None,
        "payload"              : Jsonb(message.model_dump( mode = "json",
                                                           by_alias = True)),
    }


def _webhook_status_params(
    payload_id : int,
    waba_id    : str,
    value      : WhatsAppValue,
    status     : WhatsAppStatus,
) -> dict[str, Any] :
    
    return {
        "payload_id"           : payload_id,
        "operator_id"          : value.metadata.phone_number_id,
        "display_phone_number" : value.metadata.display_phone_number,
        "waba_id"              : waba_id,
        "recipient_id"         : status.recipient_id,
        "message_id"           : status.id,
        "status"               : status.status,
        "timestamp"            : _unix_dt_param(status.timestamp),
        "conversation_id"      : (
            status.conversation.id if status.conversation else None
        ),
        "pricing_category"     : status.pricing.category if status.pricing else None,
        "payload"              : Jsonb(status.model_dump( mode = "json")),
    }


# =========================================================================================
# LOCKS (NO-OP)

class SyncSupabaseStorageLock :
    """
    No-op lock for SQL storage; consistency is handled by database constraints.
    """
    
    def __init__( self, *_args : Any, **_kwargs : Any) -> None :
        return
    
    def __enter__(self) -> "SyncSupabaseStorageLock" :
        return self
    
    def __exit__( self,
                  exc_type : type[BaseException] | None,
                  exc      : BaseException | None,
                  tb       : TracebackType | None ) -> None :
        return


class AsyncSupabaseStorageLock :
    """
    Async no-op lock for SQL storage.
    """
    
    def __init__( self, *_args : Any, **_kwargs : Any) -> None :
        return
    
    async def __aenter__(self) -> "AsyncSupabaseStorageLock" :
        return self
    
    async def __aexit__( self,
                         exc_type : type[BaseException] | None,
                         exc      : BaseException | None,
                         tb       : TracebackType | None ) -> None :
        return


# =========================================================================================
# WEBHOOK PAYLOAD STORAGE

def webhook_payload_write( payload : WhatsAppPayload) -> bool :
    """
    Persist a validated WhatsApp webhook payload and exploded message/status rows. \\
    Args:
        payload : Validated WhatsApp webhook payload
    Returns:
        True if a new payload was stored; False if it was already present.
    """
    payload_params = {
        "payload_hash" : _payload_hash(payload),
        "object_type"  : payload.title,
        "payload"      : Jsonb(payload.model_dump( mode = "json", by_alias = True)),
    }
    
    with sync_pooled_conection(get_database_url()) as conn :
        
        row = conn.execute( SQL_INSERT_WEBHOOK_PAYLOAD, payload_params).fetchone()
        if not ( row and row["inserted"] ) :
            return False
        
        payload_id = row["id"]
        for entry in payload.entry :
            for change in entry.changes :
                
                value = change.value
                for message in value.messages :
                    conn.execute(
                        SQL_INSERT_WEBHOOK_MESSAGE,
                        _webhook_message_params(
                            payload_id = payload_id,
                            waba_id    = entry.id,
                            value      = value,
                            message    = message,
                        ),
                    )
                
                for status in value.statuses :
                    conn.execute(
                        SQL_INSERT_WEBHOOK_STATUS,
                        _webhook_status_params(
                            payload_id = payload_id,
                            waba_id    = entry.id,
                            value      = value,
                            status     = status,
                        ),
                    )
    
    return True


async def async_webhook_payload_write( payload : WhatsAppPayload) -> bool :
    """
    Persist a validated WhatsApp webhook payload asynchronously. \\
    Args:
        payload : Validated WhatsApp webhook payload
    Returns:
        True if a new payload was stored; False if it was already present.
    """
    payload_params = {
        "payload_hash" : _payload_hash(payload),
        "object_type"  : payload.title,
        "payload"      : Jsonb(payload.model_dump( mode = "json", by_alias = True)),
    }
    
    async with async_pooled_connection(get_database_url()) as conn :
        
        row = await (
            await conn.execute( SQL_INSERT_WEBHOOK_PAYLOAD, payload_params)
        ).fetchone()
        if not ( row and row["inserted"] ) :
            return False
        
        payload_id = row["id"]
        for entry in payload.entry :
            for change in entry.changes :
                
                value = change.value
                for message in value.messages :
                    await conn.execute(
                        SQL_INSERT_WEBHOOK_MESSAGE,
                        _webhook_message_params(
                            payload_id = payload_id,
                            waba_id    = entry.id,
                            value      = value,
                            message    = message,
                        ),
                    )
                
                for status in value.statuses :
                    await conn.execute(
                        SQL_INSERT_WEBHOOK_STATUS,
                        _webhook_status_params(
                            payload_id = payload_id,
                            waba_id    = entry.id,
                            value      = value,
                            status     = status,
                        ),
                    )
    
    return True


# =========================================================================================
# SYNC STORAGE

class SyncSupabaseStorage :
    """
    PostgreSQL-backed storage helper for an operator/user pair.
    """
    
    def __init__( self,
                  operator_id : str | int,
                  user_id     : str | int ) -> None :
        
        self.operator_id  = str(operator_id)
        self.user_id      = str(user_id)
        self.case_id      = None
        self.database_url = get_database_url()
        
        return
    
    @staticmethod
    def webhook_payload_write( payload : WhatsAppPayload) -> bool :
        return webhook_payload_write(payload)
    
    # -------------------------------------------------------------------------------------
    # COMPATIBILITY PATHS
    
    def dir_user(self) -> Path :
        return Path(self.operator_id) / Path(self.user_id)
    
    def dir_case(self) -> Path :
        
        if self.case_id :
            return self.dir_user() / "cases" / str(self.case_id)
        
        here = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
        raise ValueError(f"In {here}: 'case_id' has not been initialized")
    
    def dir_media(self) -> Path :
        return self.dir_case() / "media"
    
    def dir_dedup(self) -> Path :
        return self.dir_user() / "dedup"
    
    def dir_messages(self) -> Path :
        return self.dir_case() / "messages"
    
    def path_user_data(self) -> Path :
        return Path("__supabase__") / "user_data"
    
    def path_case_index(self) -> Path :
        return Path("__supabase__") / "case_index"
    
    def path_manifest(self) -> Path :
        return Path("__supabase__") / "case_manifest"
    
    def path_message( self, message_id : str) -> Path :
        return Path("__supabase__") / "messages" / f"{message_id}.json"
    
    def set_case_id( self, case_id : str | int) -> None :
        
        if case_id and isinstance( case_id, int) :
            self.case_id = case_id
        
        elif case_id and isinstance( case_id, str) and case_id.isdigit() :
            self.case_id = int(case_id)
        
        else :
            here = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
            raise ValueError(f"In {here}: Invalid 'case_id' type {type(case_id)}")
        
        return
    
    # -------------------------------------------------------------------------------------
    # CONNECTION AND COMMON PARAMS
    
    def _params(self) -> dict[str, Any] :
        return {
            "operator_id" : self.operator_id,
            "user_id"     : self.user_id,
        }
    
    def _ensure_user_row(self) -> None :
        
        params         = self._params()
        params["data"] = Jsonb(_default_user_data(self.user_id))
        
        with sync_pooled_conection(self.database_url) as conn :
            conn.execute( SQL_ENSURE_USER, params)
        
        return
    
    # -------------------------------------------------------------------------------------
    # JSON I/O COMPATIBILITY
    
    def json_read( self, path : Path) -> Any | None :
        
        params = self._params()
        with sync_pooled_conection(self.database_url) as conn :
            
            if path == self.path_user_data() :
                row = conn.execute( SQL_GET_USER_DATA, params).fetchone()
                return row["data"] if row else None
            
            if path == self.path_case_index() :
                row = conn.execute( SQL_GET_CASE_INDEX, params).fetchone()
                return { "open_case_id" : row["open_case_id"] } if row else None
        
        here = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
        raise ValueError(f"In {here}: Unsupported JSON path '{path}'")
    
    def json_write( self, path : Path, obj : Any) -> None :
        
        params = self._params()
        with sync_pooled_conection(self.database_url) as conn :
            
            if path == self.path_user_data() :
                params["data"] = Jsonb(obj)
                conn.execute( SQL_UPSERT_USER_DATA, params)
                return
            
            if path == self.path_case_index() :
                params["data"]         = Jsonb(_default_user_data(self.user_id))
                params["open_case_id"] = obj.get("open_case_id")
                conn.execute( SQL_SET_CASE_INDEX, params)
                return
        
        here = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
        raise ValueError(f"In {here}: Unsupported JSON path '{path}'")
    
    # -------------------------------------------------------------------------------------
    # DEDUPLICATION
    
    def dedup_exists( self, idempotency_key : str) -> bool :
        
        params                    = self._params()
        params["idempotency_key"] = idempotency_key
        
        with sync_pooled_conection(self.database_url) as conn :
            row = conn.execute( SQL_DEDUP_EXISTS, params).fetchone()
        
        return bool(row)
    
    def dedup_write( self, _idempotency_key : str) -> None :
        return
    
    # -------------------------------------------------------------------------------------
    # MESSAGES AND MEDIA
    
    def message_read( self, message_id : str) -> Message | None :
        
        params               = self._params()
        params["case_id"]    = self.case_id
        params["message_id"] = message_id
        
        with sync_pooled_conection(self.database_url) as conn :
            row = conn.execute( SQL_GET_MESSAGE, params).fetchone()
        
        return _message_from_payload( row["payload"] if row else None)
    
    def messages_load(self) -> list[Message] :
        
        params            = self._params()
        params["case_id"] = self.case_id
        
        with sync_pooled_conection(self.database_url) as conn :
            rows = conn.execute( SQL_GET_MESSAGES, params).fetchall()
        
        return [
            msg for row in rows
            if ( msg := _message_from_payload(row["payload"]) )
        ]
    
    def message_write( self, message : Message) -> None :
        
        payload = message.model_dump( mode = "json")
        params  = self._params()
        params.update(
            { 
                "case_id"         : self.case_id,
                "message_id"      : message.id,
                "idempotency_key" : message.idempotency_key,
                "basemodel"       : message.basemodel,
                "role"            : message.role,
                "time_created"    : _dt_param(message.time_created),
                "time_received"   : _dt_param(message.time_received),
                "payload"         : Jsonb(payload),
            }
        )
        
        with sync_pooled_conection(self.database_url) as conn :
            conn.execute( SQL_INSERT_MESSAGE, params)
        
        return
    
    def media_get( self, filename : str) -> bytes | None :
        
        path = self.dir_media() / filename
        
        return b3_get_file(path) if b3_exists(path) else None
    
    def media_write( self,
                     message : UserContentMsg,
                     media   : MediaContent ) -> None :
        
        media_path = self.dir_media() / message.media.name
        
        if not b3_exists(media_path) :
            media_content = media.content if media.content else b""
            b3_put_media( media_path, media_content, media.mime)
        
        return
    
    # -------------------------------------------------------------------------------------
    # MANIFEST
    
    def get_next_case_id(self) -> int :
        
        self._ensure_user_row()
        params = self._params()
        
        with sync_pooled_conection(self.database_url) as conn :
            row = conn.execute( SQL_GET_NEXT_CASE_ID, params).fetchone()
        
        return int(row["case_id"])
    
    def manifest_append( self,
                         manifest : CaseManifest,
                         message  : Message ) -> None :
        
        if message.id not in manifest.message_ids :
            manifest.message_ids.append(message.id)
        
        existing_last = utc_iso_to_dt(manifest.time_last_message)
        msg_time      = utc_iso_to_dt(message.time_created)  or \
                        utc_iso_to_dt(message.time_received) or \
                        datetime.now(timezone.utc)
        
        if ( not existing_last ) or ( existing_last < msg_time ) :
            manifest.time_last_message = _dt_to_utc_iso(msg_time)
        
        self.manifest_write(manifest)
        
        return
    
    def manifest_load(self) -> CaseManifest | None :
        
        params            = self._params()
        params["case_id"] = self.case_id
        
        with sync_pooled_conection(self.database_url) as conn :
            case_row = conn.execute( SQL_GET_CASE, params).fetchone()
            msg_rows = conn.execute( SQL_GET_CASE_MESSAGE_IDS, params).fetchall()
        
        if not case_row :
            return None
        
        return CaseManifest(
            case_id           = case_row["case_id"],
            model             = case_row["model"],
            status            = case_row["status"],
            time_opened       = _dt_to_utc_iso(case_row["time_opened"]),
            time_last_message = _dt_to_utc_iso(case_row["time_last_message"]),
            time_closed       = _dt_to_utc_iso(case_row["time_closed"]),
            message_ids       = [ row["message_id"] for row in msg_rows ],
        )
    
    def manifest_write( self, manifest : CaseManifest) -> None :
        
        self._ensure_user_row()
        
        params = self._params()
        params.update(
            { "case_id"           : manifest.case_id,
              "model"             : manifest.model,
              "status"            : manifest.status,
              "time_opened"       : _dt_param(manifest.time_opened),
              "time_last_message" : _dt_param(manifest.time_last_message),
              "time_closed"       : _dt_param(manifest.time_closed) }
        )
        
        with sync_pooled_conection(self.database_url) as conn :
            conn.execute( SQL_UPSERT_CASE, params)
        
        return


# =========================================================================================
# ASYNC STORAGE

class AsyncSupabaseStorage (SyncSupabaseStorage) :
    """
    Async PostgreSQL-backed storage helper.
    """
    
    @staticmethod
    async def webhook_payload_write( payload : WhatsAppPayload) -> bool :
        return await async_webhook_payload_write(payload)
    
    async def _ensure_user_row(self) -> None :
        
        params         = self._params()
        params["data"] = Jsonb(_default_user_data(self.user_id))
        
        async with async_pooled_connection(self.database_url) as conn :
            await conn.execute( SQL_ENSURE_USER, params)
        
        return
    
    async def json_read( self, path : Path) -> Any | None :
        
        params = self._params()
        async with async_pooled_connection(self.database_url) as conn :
            
            if path == self.path_user_data() :
                row = await ( await conn.execute( SQL_GET_USER_DATA, params)).fetchone()
                return row["data"] if row else None
            
            if path == self.path_case_index() :
                row = await ( await conn.execute( SQL_GET_CASE_INDEX, params)).fetchone()
                return { "open_case_id" : row["open_case_id"] } if row else None
        
        here = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
        raise ValueError(f"In {here}: Unsupported JSON path '{path}'")
    
    async def json_write( self, path : Path, obj : Any) -> None :
        
        params = self._params()
        async with async_pooled_connection(self.database_url) as conn :
            
            if path == self.path_user_data() :
                params["data"] = Jsonb(obj)
                await conn.execute( SQL_UPSERT_USER_DATA, params)
                return
            
            if path == self.path_case_index() :
                params["data"]         = Jsonb(_default_user_data(self.user_id))
                params["open_case_id"] = obj.get("open_case_id")
                await conn.execute( SQL_SET_CASE_INDEX, params)
                return
        
        here = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
        raise ValueError(f"In {here}: Unsupported JSON path '{path}'")
    
    async def dedup_exists( self, idempotency_key : str) -> bool :
        
        params                    = self._params()
        params["idempotency_key"] = idempotency_key
        
        async with async_pooled_connection(self.database_url) as conn :
            row = await ( await conn.execute( SQL_DEDUP_EXISTS, params)).fetchone()
        
        return bool(row)
    
    async def dedup_write( self, _idempotency_key : str) -> None :
        return
    
    async def message_read( self, message_id : str) -> Message | None :
        
        params               = self._params()
        params["case_id"]    = self.case_id
        params["message_id"] = message_id
        
        async with async_pooled_connection(self.database_url) as conn :
            row = await ( await conn.execute( SQL_GET_MESSAGE, params)).fetchone()
        
        return _message_from_payload( row["payload"] if row else None)
    
    async def messages_load(self) -> list[Message] :
        
        params            = self._params()
        params["case_id"] = self.case_id
        
        async with async_pooled_connection(self.database_url) as conn :
            rows = await ( await conn.execute( SQL_GET_MESSAGES, params)).fetchall()
        
        return [
            msg for row in rows
            if ( msg := _message_from_payload(row["payload"]) )
        ]
    
    async def message_write( self, message : Message) -> None :
        
        payload = message.model_dump( mode = "json")
        params  = self._params()
        params.update(
            {
                "case_id"         : self.case_id,
                "message_id"      : message.id,
                "idempotency_key" : message.idempotency_key,
                "basemodel"       : message.basemodel,
                "role"            : message.role,
                "time_created"    : _dt_param(message.time_created),
                "time_received"   : _dt_param(message.time_received),
                "payload"         : Jsonb(payload),
            }
        )
        
        async with async_pooled_connection(self.database_url) as conn :
            await conn.execute( SQL_INSERT_MESSAGE, params)
        
        return
    
    async def media_get( self, filename : str) -> bytes | None :
        
        path = self.dir_media() / filename
        
        return await async_b3_get_file(path) if await async_b3_exists(path) else None
    
    async def media_write( self,
                           message : UserContentMsg,
                           media   : MediaContent ) -> None :
        
        media_path = self.dir_media() / message.media.name
        
        if not await async_b3_exists(media_path) :
            media_content = media.content if media.content else b""
            await async_b3_put_media( media_path, media_content, media.mime)
        
        return
    
    async def get_next_case_id(self) -> int :
        
        await self._ensure_user_row()
        params = self._params()
        
        async with async_pooled_connection(self.database_url) as conn :
            row = await ( await conn.execute( SQL_GET_NEXT_CASE_ID, params)).fetchone()
        
        return int(row["case_id"])
    
    async def manifest_append( self,
                               manifest : CaseManifest,
                               message  : Message ) -> None :
        
        if message.id not in manifest.message_ids :
            manifest.message_ids.append(message.id)
        
        existing_last = utc_iso_to_dt(manifest.time_last_message)
        msg_time      = utc_iso_to_dt(message.time_created)  or \
                        utc_iso_to_dt(message.time_received) or \
                        datetime.now(timezone.utc)
        
        if ( not existing_last ) or ( existing_last < msg_time ) :
            manifest.time_last_message = _dt_to_utc_iso(msg_time)
        
        await self.manifest_write(manifest)
        
        return
    
    async def manifest_load(self) -> CaseManifest | None :
        
        params            = self._params()
        params["case_id"] = self.case_id
        
        async with async_pooled_connection(self.database_url) as conn :
            case_row = await ( await conn.execute( SQL_GET_CASE, params)).fetchone()
            msg_rows = await (
                await conn.execute( SQL_GET_CASE_MESSAGE_IDS, params)
            ).fetchall()
        
        if not case_row :
            return None
        
        return CaseManifest(
            case_id           = case_row["case_id"],
            model             = case_row["model"],
            status            = case_row["status"],
            time_opened       = _dt_to_utc_iso(case_row["time_opened"]),
            time_last_message = _dt_to_utc_iso(case_row["time_last_message"]),
            time_closed       = _dt_to_utc_iso(case_row["time_closed"]),
            message_ids       = [ row["message_id"] for row in msg_rows ],
        )
    
    async def manifest_write( self, manifest : CaseManifest) -> None :
        
        await self._ensure_user_row()
        
        params = self._params()
        params.update(
            {
                "case_id"           : manifest.case_id,
                "model"             : manifest.model,
                "status"            : manifest.status,
                "time_opened"       : _dt_param(manifest.time_opened),
                "time_last_message" : _dt_param(manifest.time_last_message),
                "time_closed"       : _dt_param(manifest.time_closed),
            }
        )
        
        async with async_pooled_connection(self.database_url) as conn :
            await conn.execute( SQL_UPSERT_CASE, params)
        
        return
