"""
Supabase-backed queue for normalized inbound WhatsApp messages.
"""

import json

from datetime import (
    UTC,
    datetime,
)
from pathlib import Path
from pydantic import ValidationError
from typing import (
    Any,
    TypedDict,
)
from uuid import (
    UUID,
    uuid4,
)

from sofia_utils.printing import get_qualname as here
from sofia_utils.psycopg import (
    async_pooled_connection,
    load_sql_script,
    sync_pooled_conection,
)

from .supabase import (
    AsyncSupabaseStorage,
    SyncSupabaseStorage,
    get_database_url,
)
from .whatsapp_models import (
    WhatsAppContact,
    WhatsAppMessage,
    WhatsAppMessageEcho,
    WhatsAppPayload,
    WhatsAppValue,
)


SQL_DIR = Path(__file__).parent / "sql"

SQL_CLAIM_NEXT = load_sql_script( SQL_DIR / "claim_next_case_handler_message.sql")
SQL_ENQUEUE    = load_sql_script( SQL_DIR / "enqueue_case_handler_message.sql")
SQL_MARK_DONE  = load_sql_script( SQL_DIR / "mark_case_handler_message_done.sql")
SQL_MARK_ERROR = load_sql_script( SQL_DIR / "mark_case_handler_message_error.sql")


class PayloadEnqueueResult (TypedDict) :
    """
    Outcome of auditing and enqueueing one webhook payload.
    """
    stored   : bool
    enqueued : bool


def _message_data(
    message : WhatsAppMessage | WhatsAppMessageEcho,
) -> dict[ str, Any] :
    """
    Return message-specific data stored outside normalized columns.
    """
    return message.model_dump(
        mode         = "json",
        by_alias     = True,
        exclude_none = True,
        exclude      = { "user", "user_id", "id", "timestamp", "type" },
    )


def _value_contact( value : WhatsAppValue) -> WhatsAppContact :
    """
    Return the contact represented by one WhatsApp change value.
    """
    if value.contacts :
        return value.contacts[0]
    if value.messages :
        message = value.messages[0]
        return WhatsAppContact( wa_id = message.user, user_id = message.user_id)
    if value.message_echoes :
        message = value.message_echoes[0]
        return WhatsAppContact(
            wa_id   = message.to,
            user_id = message.to_user_id,
        )
    if value.statuses :
        message_status = value.statuses[0]
        return WhatsAppContact(
            wa_id   = message_status.recipient_id,
            user_id = message_status.recipient_user_id,
        )
    
    raise RuntimeError(f"In {here()}: WhatsApp value has no contact identity")


def _timestamp( value : int | str) -> datetime :
    
    return datetime.fromtimestamp( int(value), UTC)


def _validation_errors( ex : ValidationError) -> list[dict[str, Any]] :
    
    return json.loads(ex.json(include_url = False))


class QueueDB :
    """
    Sequential inbound-message queue.
    """
    
    def __init__( self, database_url : str | None = None) -> None :
        
        self.database_url = database_url or get_database_url()
        self.storage      = SyncSupabaseStorage(self.database_url)
        
        return
    
    def _fetch_one(
        self,
        sql    : str,
        params : dict[str, Any],
    ) -> dict[str, Any] | None :
        
        with sync_pooled_conection(self.database_url) as conn :
            row = conn.execute( sql, params).fetchone()
        
        return dict(row) if row else None
    
    def _enqueue_message( self, msg_id : str) -> bool :
        
        return bool( self._fetch_one( SQL_ENQUEUE, { "msg_id" : msg_id }) )
    
    def _persist_valid_payload(
        self,
        payload_id : int,
        payload    : WhatsAppPayload,
    ) -> bool :
        """
        Normalize a validated payload and enqueue newly persisted messages.
        """
        enqueued = False
        
        for item_idx, item in enumerate(payload.entry) :
            
            item_ts = _timestamp(item.time) if item.time is not None else None
            
            for change_idx, change in enumerate(item.changes) :
                
                value = change.value
                if not isinstance( value, WhatsAppValue) :
                    continue
                
                business = self.storage.upsert_business(
                    waba_id              = item.id,
                    phone_number_id      = value.metadata.phone_number_id,
                    display_phone_number = value.metadata.display_phone_number,
                )
                if not business :
                    raise RuntimeError(f"In {here()}: Unable to persist business")
                
                contact_model = _value_contact(value)
                contact       = self.storage.upsert_contact(
                    business = business["id"],
                    wa_id    = contact_model.wa_id,
                    user_id  = contact_model.user_id,
                )
                if not contact :
                    raise RuntimeError(f"In {here()}: Unable to persist contact")
                
                if contact_model.profile :
                    profile = self.storage.upsert_contact_profile(
                        contact          = contact["id"],
                        profile_name     = contact_model.profile.name,
                        profile_username = contact_model.profile.username,
                    )
                    if not profile :
                        raise RuntimeError(
                            f"In {here()}: Unable to persist contact profile"
                        )
                
                metadata = self.storage.insert_inbound_payload_metadata(
                    payload_id = payload_id,
                    contact    = contact["id"],
                    item_idx   = item_idx,
                    item_ts    = item_ts,
                    change_idx = change_idx,
                )
                if not metadata :
                    raise RuntimeError(
                        f"In {here()}: Unable to persist inbound payload metadata"
                    )
                
                messages = [ ( message, False) for message in value.messages ]
                messages.extend(
                    ( message, True) for message in value.message_echoes
                )
                for message, is_echo in messages :
                    message_row = self.storage.insert_inbound_message(
                        payload  = metadata["id"],
                        is_echo  = is_echo,
                        msg_id   = message.id,
                        msg_ts   = _timestamp(message.timestamp),
                        msg_type = message.type,
                        msg_data = _message_data(message),
                    )
                    if message_row :
                        enqueued = self._enqueue_message(message.id) or enqueued
                
                for message_status in value.statuses :
                    status_row = self.storage.insert_status(
                        payload      = metadata["id"],
                        msg_id       = message_status.id,
                        msg_status   = message_status.status,
                        status_ts    = _timestamp(message_status.timestamp),
                        conversation = (
                            message_status.conversation.model_dump( mode = "json")
                            if message_status.conversation else None
                        ),
                        pricing = (
                            message_status.pricing.model_dump( mode = "json")
                            if message_status.pricing else None
                        ),
                        errors = (
                            [
                                error.model_dump( mode = "json")
                                for error in message_status.errors
                            ]
                            if message_status.errors else None
                        ),
                    )
                    if not status_row :
                        raise RuntimeError(
                            f"In {here()}: Unable to persist message status"
                        )
        
        return enqueued
    
    def enqueue( self, data : dict[str, Any]) -> PayloadEnqueueResult :
        """
        Audit, validate, normalize, and enqueue one webhook payload.
        """
        raw_payload = self.storage.insert_inbound_payload(data)
        
        if not raw_payload or raw_payload.get("id") is None :
            raise RuntimeError(f"In {here()}: Unable to persist inbound payload")
        
        payload_id = int(raw_payload["id"])
        stored     = bool(raw_payload.get("inserted"))
        
        try :
            payload = WhatsAppPayload.model_validate(data)
        except ValidationError as ex :
            invalid = self.storage.mark_inbound_payload_invalid(
                payload_id,
                _validation_errors(ex),
            )
            if not invalid :
                raise RuntimeError(
                    f"In {here()}: Unable to persist payload validation errors"
                ) from ex
            raise
        
        valid = self.storage.mark_inbound_payload_valid(payload_id)
        if not valid :
            raise RuntimeError(
                f"In {here()}: Unable to mark inbound payload as valid"
            )
        
        return {
            "stored"   : stored,
            "enqueued" : (
                self._persist_valid_payload( payload_id, payload)
                if stored else False
            ),
        }
    
    def claim_next(self) -> dict[str, Any] | None :
        """
        Atomically claim one message and its contact lease.
        """
        owner_token = uuid4()
        queue_row   = self._fetch_one(
            SQL_CLAIM_NEXT,
            { "owner_token" : owner_token },
        )
        if not queue_row :
            return None
        
        message_row = self.storage.get_inbound_message(queue_row["msg_id"])
        if not message_row :
            self.mark_error(queue_row["row_id"])
            self.storage.release_contact_lease(
                queue_row["contact"],
                owner_token,
            )
            raise RuntimeError(
                f"Queued WhatsApp message '{queue_row['msg_id']}' was not found"
            )
        
        return {
            **message_row,
            "row_id"      : queue_row["row_id"],
            "msg_status"  : queue_row["msg_status"],
            "owner_token" : owner_token,
        }
    
    def mark_done( self, row_id : int) -> bool :
        """
        Mark a claimed queue row as successfully processed.
        """
        return bool( self._fetch_one( SQL_MARK_DONE, { "row_id" : row_id }) )
    
    def mark_error( self, row_id : int) -> bool :
        """
        Mark a claimed queue row as failed.
        """
        return bool( self._fetch_one( SQL_MARK_ERROR, { "row_id" : row_id }) )
    
    def release_contact_lease(
        self,
        contact     : int,
        owner_token : UUID | str,
    ) -> bool :
        """
        Release the lease associated with a claimed queue row.
        """
        return self.storage.release_contact_lease( contact, owner_token)


class AsyncQueueDB :
    """
    Asynchronous inbound-message queue.
    """
    
    def __init__( self, database_url : str | None = None) -> None :
        
        self.database_url = database_url or get_database_url()
        self.storage      = AsyncSupabaseStorage(self.database_url)
        
        return
    
    async def _fetch_one(
        self,
        sql    : str,
        params : dict[str, Any],
    ) -> dict[str, Any] | None :
        
        async with async_pooled_connection(self.database_url) as conn :
            cursor = await conn.execute( sql, params)
            row    = await cursor.fetchone()
        
        return dict(row) if row else None
    
    async def _enqueue_message( self, msg_id : str) -> bool :
        
        return bool( await self._fetch_one( SQL_ENQUEUE, { "msg_id" : msg_id }) )
    
    async def _persist_valid_payload(
        self,
        payload_id : int,
        payload    : WhatsAppPayload,
    ) -> bool :
        """
        Normalize a validated payload and enqueue newly persisted messages.
        """
        enqueued = False
        
        for item_idx, item in enumerate(payload.entry) :
            
            item_ts = _timestamp(item.time) if item.time is not None else None
            
            for change_idx, change in enumerate(item.changes) :
                
                value = change.value
                if not isinstance( value, WhatsAppValue) :
                    continue
                
                business = await self.storage.upsert_business(
                    waba_id              = item.id,
                    phone_number_id      = value.metadata.phone_number_id,
                    display_phone_number = value.metadata.display_phone_number,
                )
                if not business :
                    raise RuntimeError(f"In {here()}: Unable to persist business")
                
                contact_model = _value_contact(value)
                contact       = await self.storage.upsert_contact(
                    business = business["id"],
                    wa_id    = contact_model.wa_id,
                    user_id  = contact_model.user_id,
                )
                if not contact :
                    raise RuntimeError(f"In {here()}: Unable to persist contact")
                
                if contact_model.profile :
                    profile = await self.storage.upsert_contact_profile(
                        contact          = contact["id"],
                        profile_name     = contact_model.profile.name,
                        profile_username = contact_model.profile.username,
                    )
                    if not profile :
                        raise RuntimeError(
                            f"In {here()}: Unable to persist contact profile"
                        )
                
                metadata = await self.storage.insert_inbound_payload_metadata(
                    payload_id = payload_id,
                    contact    = contact["id"],
                    item_idx   = item_idx,
                    item_ts    = item_ts,
                    change_idx = change_idx,
                )
                if not metadata :
                    raise RuntimeError(
                        f"In {here()}: Unable to persist inbound payload metadata"
                    )
                
                messages = [ ( message, False) for message in value.messages ]
                messages.extend(
                    ( message, True) for message in value.message_echoes
                )
                for message, is_echo in messages :
                    message_row = await self.storage.insert_inbound_message(
                        payload  = metadata["id"],
                        is_echo  = is_echo,
                        msg_id   = message.id,
                        msg_ts   = _timestamp(message.timestamp),
                        msg_type = message.type,
                        msg_data = _message_data(message),
                    )
                    if message_row :
                        enqueued = await self._enqueue_message(message.id) or enqueued
                
                for message_status in value.statuses :
                    status_row = await self.storage.insert_status(
                        payload      = metadata["id"],
                        msg_id       = message_status.id,
                        msg_status   = message_status.status,
                        status_ts    = _timestamp(message_status.timestamp),
                        conversation = (
                            message_status.conversation.model_dump( mode = "json")
                            if message_status.conversation else None
                        ),
                        pricing = (
                            message_status.pricing.model_dump( mode = "json")
                            if message_status.pricing else None
                        ),
                        errors = (
                            [
                                error.model_dump( mode = "json")
                                for error in message_status.errors
                            ]
                            if message_status.errors else None
                        ),
                    )
                    if not status_row :
                        raise RuntimeError(
                            f"In {here()}: Unable to persist message status"
                        )
        
        return enqueued
    
    async def enqueue( self, data : dict[str, Any]) -> PayloadEnqueueResult :
        """
        Audit, validate, normalize, and enqueue one webhook payload.
        """
        raw_payload = await self.storage.insert_inbound_payload(data)
        
        if not raw_payload or raw_payload.get("id") is None :
            raise RuntimeError(f"In {here()}: Unable to persist inbound payload")
        
        payload_id = int(raw_payload["id"])
        stored     = bool(raw_payload.get("inserted"))
        
        try :
            payload = WhatsAppPayload.model_validate(data)
        except ValidationError as ex :
            invalid = await self.storage.mark_inbound_payload_invalid(
                payload_id,
                _validation_errors(ex),
            )
            if not invalid :
                raise RuntimeError(
                    f"In {here()}: Unable to persist payload validation errors"
                ) from ex
            raise
        
        valid = await self.storage.mark_inbound_payload_valid(payload_id)
        if not valid :
            raise RuntimeError(
                f"In {here()}: Unable to mark inbound payload as valid"
            )
        
        return {
            "stored"   : stored,
            "enqueued" : (
                await self._persist_valid_payload( payload_id, payload)
                if stored else False
            ),
        }
    
    async def claim_next(self) -> dict[str, Any] | None :
        """
        Atomically claim one message and its contact lease.
        """
        owner_token = uuid4()
        queue_row   = await self._fetch_one(
            SQL_CLAIM_NEXT,
            { "owner_token" : owner_token },
        )
        if not queue_row :
            return None
        
        message_row = await self.storage.get_inbound_message(queue_row["msg_id"])
        if not message_row :
            await self.mark_error(queue_row["row_id"])
            await self.storage.release_contact_lease(
                queue_row["contact"],
                owner_token,
            )
            raise RuntimeError(
                f"Queued WhatsApp message '{queue_row['msg_id']}' was not found"
            )
        
        return {
            **message_row,
            "row_id"      : queue_row["row_id"],
            "msg_status"  : queue_row["msg_status"],
            "owner_token" : owner_token,
        }
    
    async def mark_done( self, row_id : int) -> bool :
        """
        Mark a claimed queue row as successfully processed.
        """
        row = await self._fetch_one( SQL_MARK_DONE, { "row_id" : row_id })
        
        return bool(row)
    
    async def mark_error( self, row_id : int) -> bool :
        """
        Mark a claimed queue row as failed.
        """
        row = await self._fetch_one( SQL_MARK_ERROR, { "row_id" : row_id })
        
        return bool(row)

    async def release_contact_lease(
        self,
        contact     : int,
        owner_token : UUID | str,
    ) -> bool :
        """
        Release the lease associated with a claimed queue row.
        """
        return await self.storage.release_contact_lease( contact, owner_token)
