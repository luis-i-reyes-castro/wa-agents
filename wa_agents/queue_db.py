"""
Supabase-backed queue for normalized inbound WhatsApp messages.
"""

from pathlib import Path
from typing import Any
from uuid import (
    UUID,
    uuid4,
)

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


SQL_DIR = Path(__file__).parent / "sql"

SQL_CLAIM_NEXT = load_sql_script( SQL_DIR / "claim_next_case_handler_message.sql")
SQL_ENQUEUE    = load_sql_script( SQL_DIR / "enqueue_case_handler_message.sql")
SQL_MARK_DONE  = load_sql_script( SQL_DIR / "mark_case_handler_message_done.sql")
SQL_MARK_ERROR = load_sql_script( SQL_DIR / "mark_case_handler_message_error.sql")


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
    
    def enqueue( self, msg_id : str) -> bool :
        """
        Enqueue one persisted WhatsApp message ID unless already present.
        """
        return bool(self._fetch_one( SQL_ENQUEUE, { "msg_id" : msg_id }))
    
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
        return bool(self._fetch_one( SQL_MARK_DONE, { "row_id" : row_id }))
    
    def mark_error( self, row_id : int) -> bool :
        """
        Mark a claimed queue row as failed.
        """
        return bool(self._fetch_one( SQL_MARK_ERROR, { "row_id" : row_id }))
    
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
    
    async def enqueue( self, msg_id : str) -> bool :
        """
        Enqueue one persisted WhatsApp message ID unless already present.
        """
        row = await self._fetch_one( SQL_ENQUEUE, { "msg_id" : msg_id })
        
        return bool(row)
    
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
