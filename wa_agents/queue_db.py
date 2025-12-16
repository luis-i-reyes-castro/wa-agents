"""
Queue Database (SQLite3)
"""

import sqlite3
import threading
from pathlib import Path

from .basemodels import WhatsAppPayload


class QueueDB :
    """
    Lightweight SQLite-backed queue for incoming WhatsApp messages
    """
    
    def __init__( self, db_path : str | Path) -> None :
        
        self._db_path = Path(db_path)
        self._lock    = threading.Lock()
        self._init_db()
        
        return
    
    def _connect(self) -> sqlite3.Connection :
        """
        Connect to database
        """
        conn             = sqlite3.connect( self._db_path, timeout = 30)
        conn.row_factory = sqlite3.Row
        
        return conn
    
    def _init_db(self) -> None :
        """
        Initialize database
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
        Insert a payload if not already present.\n
        Args:
            payload: Object of class WhatsAppPayload
        Returns:
            True if enqueued, False otherwise.
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
        Atomically claim the next pending payload\n
        Returns:
            Dict with keys: 'row_id', 'payload'
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
        
        return { "row_id"  : row["id"],
                 "payload" : WhatsAppPayload.model_validate_json(row["payload"]) }
    
    def mark_done( self, row_id : int) -> None :
        """
        Mark payload as done.\n
        Args:
            row_id: Row ID of the payload
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
        Mark error during payload processing.\n
        Args:
            row_id    : Row ID of the payload
            error_msg : Error message
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
