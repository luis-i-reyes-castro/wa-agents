"""
Queue Database (SQLite3)
"""

import aiosqlite
import asyncio
import sqlite3
import threading

from pathlib import Path

from .basemodels import WhatsAppPayload


class QueueDB :
    """
    Lightweight SQLite-backed queue for incoming WhatsApp messages
    """
    
    def __init__( self, db_path : str | Path) -> None :
        """
        Initialize the queue database (creating schema if needed) \\
        Args:
            db_path : Path to the SQLite database file
        """
        
        self._db_path = Path(db_path)
        self._lock    = threading.Lock()
        self._init_db()
        
        return
    
    def _connect(self) -> sqlite3.Connection :
        """
        Connect to the queue database file \\
        Returns:
            SQLite connection configured with row factory
        """
        conn             = sqlite3.connect( self._db_path, timeout = 30)
        conn.row_factory = sqlite3.Row
        
        return conn
    
    def _init_db(self) -> None :
        """
        Create the queue tables and indexes when missing
        """
        with self._connect() as conn :
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS incoming_queue (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload     TEXT NOT NULL,
                    status      TEXT NOT NULL DEFAULT 'pending',
                    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                    last_error  TEXT
                );
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_incoming_queue_payload
                    ON incoming_queue(payload);
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_incoming_queue_status
                    ON incoming_queue(status);
                """
            )
    
    def enqueue( self, payload : WhatsAppPayload) -> bool :
        """
        Insert a payload only if it has not been seen before \\
        Args:
            payload : WhatsAppPayload object to enqueue
        Returns:
            True if the payload was enqueued; False if it was a duplicate.
        """
        with self._lock, self._connect() as conn :
            try :
                conn.execute(
                    """
                    INSERT INTO incoming_queue (payload)
                    VALUES (?)
                    ON CONFLICT (payload) DO NOTHING
                    """,
                    (payload.model_dump_json(by_alias = True),),
                )
                return conn.total_changes > 0
            
            except sqlite3.Error :
                return False
    
    def claim_next(self) -> dict[ str, str | WhatsAppPayload] | None :
        """
        Atomically claim the oldest pending payload \\
        Returns:
            Dict with keys `row_id` and `payload`, or None if queue empty.
        """
        with self._lock, self._connect() as conn :
            
            conn.execute( "BEGIN IMMEDIATE;")
            
            row = conn.execute(
                f"""
                SELECT id, payload
                  FROM incoming_queue
                 WHERE status = 'pending'
              ORDER BY created_at ASC
                 LIMIT 1;
                """,
                (),
            ).fetchone()
            
            if not row :
                conn.execute( "COMMIT;")
                return None
            
            conn.execute(
                """
                UPDATE incoming_queue
                   SET status     = 'processing',
                       updated_at = CURRENT_TIMESTAMP,
                       last_error = NULL
                 WHERE id = ?;
                """,
                ( row["id"],),
            )
            conn.execute("COMMIT;")
        
        return {
            "row_id"  : row["id"],
            "payload" : WhatsAppPayload.model_validate_json(row["payload"]),
        }
    
    def mark_done( self, row_id : int) -> None :
        """
        Mark a queue row as processed successfully \\
        Args:
            row_id : Queue row identifier
        """
        with self._connect() as conn :
            conn.execute(
                """
                UPDATE incoming_queue
                   SET status     = 'done',
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?;
                """,
                ( row_id,),
            )
        return
    
    def mark_error( self, row_id : int, error_msg : str) -> None :
        """
        Mark a queue row as failed and store the error message \\
        Args:
            row_id    : Queue row identifier
            error_msg : Error details to persist
        """
        with self._connect() as conn :
            conn.execute(
                """
                UPDATE incoming_queue
                   SET status     = 'error',
                       updated_at = CURRENT_TIMESTAMP,
                       last_error = ?
                 WHERE id = ?;
                """,
                ( error_msg, row_id),
            )
        return


class AsyncQueueDB :
    """
    Async SQLite-backed queue for incoming WhatsApp messages
    """
    def __init__( self, db_path : str | Path) -> None :
        """
        Configure the queue database path \\
        Args:
            db_path : Path to the SQLite database file
        """
        self._db_path     = Path(db_path)
        self._lock        = asyncio.Lock()
        self._initialized = False
        
        return
    
    async def _connect(self) -> aiosqlite.Connection :
        """
        Connect to the queue database file \\
        Returns:
            SQLite connection configured with row factory
        """
        conn             = await aiosqlite.connect( self._db_path, timeout = 30)
        conn.row_factory = sqlite3.Row
        
        return conn
    
    async def _ensure_db(self) -> None :
        """
        Create the queue tables and indexes when missing
        """
        if self._initialized :
            return
        
        async with self._lock :
            
            if self._initialized :
                return
            
            conn = await self._connect()
            try :
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS incoming_queue (
                        id          INTEGER PRIMARY KEY AUTOINCREMENT,
                        payload     TEXT NOT NULL,
                        status      TEXT NOT NULL DEFAULT 'pending',
                        created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                        last_error  TEXT
                    );
                    """
                )
                await conn.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_incoming_queue_payload
                        ON incoming_queue(payload);
                    """
                )
                await conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_incoming_queue_status
                        ON incoming_queue(status);
                    """
                )
                await conn.commit()
            
            finally :
                await conn.close()
            
            self._initialized = True
        
        return
    
    async def enqueue( self, payload : WhatsAppPayload) -> bool :
        """
        Insert a payload only if it has not been seen before \\
        Args:
            payload : WhatsAppPayload object to enqueue
        Returns:
            True if the payload was enqueued; False if it was a duplicate.
        """
        await self._ensure_db()
        
        async with self._lock :
            
            conn = await self._connect()
            try :
                await conn.execute(
                    """
                    INSERT INTO incoming_queue (payload)
                    VALUES (?)
                    ON CONFLICT (payload) DO NOTHING
                    """,
                    (payload.model_dump_json(by_alias = True),),
                )
                await conn.commit()
                
                return conn.total_changes > 0
            
            except sqlite3.Error :
                return False
            
            finally :
                await conn.close()
    
    async def claim_next(self) -> dict[ str, int | WhatsAppPayload] | None :
        """
        Atomically claim the oldest pending payload \\
        Returns:
            Dict with keys `row_id` and `payload`, or None if queue empty.
        """
        await self._ensure_db()
        
        async with self._lock :
            
            conn = await self._connect()
            try :
                await conn.execute( "BEGIN IMMEDIATE;")
                
                cursor = await conn.execute(
                    """
                    SELECT id, payload
                      FROM incoming_queue
                     WHERE status = 'pending'
                  ORDER BY created_at ASC
                     LIMIT 1;
                    """,
                    (),
                )
                
                row = await cursor.fetchone()
                if not row :
                    await conn.commit()
                    return None
                
                await conn.execute(
                    """
                    UPDATE incoming_queue
                       SET status     = 'processing',
                           updated_at = CURRENT_TIMESTAMP,
                           last_error = NULL
                     WHERE id = ?;
                    """,
                    ( row["id"],),
                )
                await conn.commit()
            
            finally :
                await conn.close()
        
        return {
            "row_id"  : row["id"],
            "payload" : WhatsAppPayload.model_validate_json(row["payload"]),
        }

    async def mark_done( self, row_id : int) -> None :
        """
        Mark a queue row as processed successfully \\
        Args:
            row_id : Queue row identifier
        """
        await self._ensure_db()
        
        conn = await self._connect()
        try :
            await conn.execute(
                """
                UPDATE incoming_queue
                   SET status     = 'done',
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?;
                """,
                ( row_id,),
            )
            await conn.commit()
        
        finally :
            await conn.close()
        
        return
    
    async def mark_error( self, row_id : int, error_msg : str) -> None :
        """
        Mark a queue row as failed and store the error message \\
        Args:
            row_id    : Queue row identifier
            error_msg : Error details to persist
        """
        await self._ensure_db()
        
        conn = await self._connect()
        try :
            await conn.execute(
                """
                UPDATE incoming_queue
                   SET status     = 'error',
                       updated_at = CURRENT_TIMESTAMP,
                       last_error = ?
                 WHERE id = ?;
                """,
                ( error_msg, row_id),
            )
            await conn.commit()
        
        finally :
            await conn.close()
        
        return
