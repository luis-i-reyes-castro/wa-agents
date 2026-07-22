"""
Supabase-backed queue for incoming WhatsApp payloads.
"""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any

from sofia_utils.psycopg import (
    Jsonb,
    async_pooled_connection,
    load_sql_script,
    sync_pooled_conection,
)

from .basemodels import WhatsAppPayload
from .supabase_storage import get_database_url


SQL_DIR               = Path(__file__).parent / "sql"
SQL_ENQUEUE_PAYLOAD   = load_sql_script( SQL_DIR / "enqueue_queue_payload.sql" )
SQL_CLAIM_NEXT        = load_sql_script( SQL_DIR / "claim_next_queue_payload.sql" )
SQL_MARK_QUEUE_DONE   = load_sql_script( SQL_DIR / "mark_queue_done.sql" )
SQL_MARK_QUEUE_ERROR  = load_sql_script( SQL_DIR / "mark_queue_error.sql" )


def _canonical_payload_json( payload : WhatsAppPayload) -> str :
    
    return payload.model_dump_json( by_alias = True)


def _payload_hash( payload : WhatsAppPayload) -> str :
    
    payload_json = _canonical_payload_json(payload)
    
    return sha256( payload_json.encode("utf-8")).hexdigest()


def _enqueue_params( payload : WhatsAppPayload) -> dict[str, Any] :
    
    return {
        "payload_hash" : _payload_hash(payload),
        "payload"      : Jsonb(payload.model_dump( mode = "json", by_alias = True)),
    }


class QueueDB :
    """
    Supabase-backed queue for incoming WhatsApp messages.
    """
    
    def __init__( self, db_path : str | Path | None = None) -> None :
        """
        Initialize the queue object. \\
        Args:
            db_path : Ignored; kept for compatibility with older local queues.
        """
        self._db_path      = Path(db_path) if db_path else None
        self.database_url  = get_database_url()
        
        return
    
    def enqueue( self, payload : WhatsAppPayload) -> bool :
        """
        Insert a payload only if it has not been seen before. \\
        Args:
            payload : WhatsAppPayload object to enqueue
        Returns:
            True if the payload was enqueued; False if it was a duplicate.
        """
        with sync_pooled_conection(self.database_url) as conn :
            row = conn.execute(
                SQL_ENQUEUE_PAYLOAD,
                _enqueue_params(payload),
            ).fetchone()
        
        return bool(row)
    
    def claim_next(self) -> dict[ str, int | WhatsAppPayload] | None :
        """
        Atomically claim the oldest pending payload. \\
        Returns:
            Dict with keys `row_id` and `payload`, or None if queue empty.
        """
        with sync_pooled_conection(self.database_url) as conn :
            row = conn.execute( SQL_CLAIM_NEXT, {}).fetchone()
        
        if not row :
            return None
        
        return {
            "row_id"  : row["row_id"],
            "payload" : WhatsAppPayload.model_validate(row["payload"]),
        }
    
    def mark_done( self, row_id : int) -> None :
        """
        Mark a queue row as processed successfully. \\
        Args:
            row_id : Queue row identifier
        """
        with sync_pooled_conection(self.database_url) as conn :
            conn.execute( SQL_MARK_QUEUE_DONE, { "row_id" : row_id })
        
        return
    
    def mark_error( self, row_id : int, error_msg : str) -> None :
        """
        Mark a queue row as failed and store the error message. \\
        Args:
            row_id    : Queue row identifier
            error_msg : Error details to persist
        """
        with sync_pooled_conection(self.database_url) as conn :
            conn.execute(
                SQL_MARK_QUEUE_ERROR,
                { "row_id" : row_id, "last_error" : error_msg },
            )
        
        return


class AsyncQueueDB :
    """
    Async Supabase-backed queue for incoming WhatsApp messages.
    """
    
    def __init__( self, db_path : str | Path | None = None) -> None :
        """
        Initialize the queue object. \\
        Args:
            db_path : Ignored; kept for compatibility with older local queues.
        """
        self._db_path     = Path(db_path) if db_path else None
        self.database_url = get_database_url()
        
        return
    
    async def enqueue( self, payload : WhatsAppPayload) -> bool :
        """
        Insert a payload only if it has not been seen before. \\
        Args:
            payload : WhatsAppPayload object to enqueue
        Returns:
            True if the payload was enqueued; False if it was a duplicate.
        """
        async with async_pooled_connection(self.database_url) as conn :
            row = await (
                await conn.execute(
                    SQL_ENQUEUE_PAYLOAD,
                    _enqueue_params(payload),
                )
            ).fetchone()
        
        return bool(row)
    
    async def claim_next(self) -> dict[ str, int | WhatsAppPayload] | None :
        """
        Atomically claim the oldest pending payload. \\
        Returns:
            Dict with keys `row_id` and `payload`, or None if queue empty.
        """
        async with async_pooled_connection(self.database_url) as conn :
            row = await ( await conn.execute( SQL_CLAIM_NEXT, {})).fetchone()
        
        if not row :
            return None
        
        return {
            "row_id"  : row["row_id"],
            "payload" : WhatsAppPayload.model_validate(row["payload"]),
        }
    
    async def mark_done( self, row_id : int) -> None :
        """
        Mark a queue row as processed successfully. \\
        Args:
            row_id : Queue row identifier
        """
        async with async_pooled_connection(self.database_url) as conn :
            await conn.execute( SQL_MARK_QUEUE_DONE, { "row_id" : row_id })
        
        return
    
    async def mark_error( self, row_id : int, error_msg : str) -> None :
        """
        Mark a queue row as failed and store the error message. \\
        Args:
            row_id    : Queue row identifier
            error_msg : Error details to persist
        """
        async with async_pooled_connection(self.database_url) as conn :
            await conn.execute(
                SQL_MARK_QUEUE_ERROR,
                { "row_id" : row_id, "last_error" : error_msg },
            )
        
        return
