import os

os.environ.setdefault( "BUCKET_REGION", "test-region")
os.environ.setdefault( "BUCKET_KEY_ID", "test-key")
os.environ.setdefault( "BUCKET_KEY_SECRET", "test-secret")
os.environ.setdefault( "BUCKET_NAME", "test-bucket")
os.environ.setdefault( "SUPABASE_DB_CONNECTION_URL_IPv4", "postgresql://test")

from .basemodels import ServerTextMsg
from .listener import Listener
from . import queue_db
from .storage_backend import get_storage_backend
from . import supabase_storage
from .supabase_storage import (
    SQL_INSERT_MESSAGE,
    SQL_INSERT_WEBHOOK_MESSAGE,
    SQL_INSERT_WEBHOOK_PAYLOAD,
    SQL_INSERT_WEBHOOK_STATUS,
    _payload_hash,
    _message_from_payload,
    get_database_url,
)


def test_message_idempotency_key_is_always_generated() -> None :
    
    message = ServerTextMsg(text = "hello")
    
    assert message.id
    assert message.idempotency_key


def test_message_payload_round_trip_uses_basemodel() -> None :
    
    message = ServerTextMsg( text = "hello", idempotency_key = "server-1")
    payload = message.model_dump( mode = "json")
    
    result = _message_from_payload(payload)
    
    assert isinstance( result, ServerTextMsg)
    assert result.id == message.id
    assert result.idempotency_key == "server-1"
    assert result.text == "hello"


def test_storage_backend_defaults_to_supabase( monkeypatch) -> None :
    
    monkeypatch.delenv( "WA_AGENTS_STORAGE_BACKEND", raising = False)
    
    assert get_storage_backend() == "supabase"


def test_database_url_prefers_ipv4( monkeypatch) -> None :
    
    monkeypatch.setenv( "SUPABASE_DB_CONNECTION_URL_IPv4", "postgresql://ipv4")
    monkeypatch.setenv( "SUPABASE_DB_CONNECTION_URL_IPv6", "postgresql://ipv6")
    
    assert get_database_url() == "postgresql://ipv4"


def test_message_insert_dedup_is_message_backed() -> None :
    
    assert "idempotency_key" in SQL_INSERT_MESSAGE
    assert "ON CONFLICT DO NOTHING" in SQL_INSERT_MESSAGE


class _FakeCursor :
    
    def __init__( self, row : dict | None = None) -> None :
        self.row = row
        return
    
    def fetchone(self) -> dict | None :
        return self.row


class _FakeConnection :
    
    def __init__( self, payload_row : dict) -> None :
        self.payload_row = payload_row
        self.calls       = []
        return
    
    def execute( self, sql : str, params : dict) -> _FakeCursor :
        self.calls.append(( sql, params))
        if sql == SQL_INSERT_WEBHOOK_PAYLOAD :
            return _FakeCursor(self.payload_row)
        return _FakeCursor()


class _FakeConnectionContext :
    
    def __init__( self, conn : _FakeConnection) -> None :
        self.conn = conn
        return
    
    def __enter__(self) -> _FakeConnection :
        return self.conn
    
    def __exit__( self, exc_type, exc, tb) -> None :
        return


def _message_payload_dict() -> dict :
    
    return {
        "object" : "whatsapp_business_account",
        "entry"  : [
            {
                "id"      : "waba-1",
                "changes" : [
                    {
                        "field" : "messages",
                        "value" : {
                            "messaging_product" : "whatsapp",
                            "metadata"          : {
                                "display_phone_number" : "15551234567",
                                "phone_number_id"      : "phone-1",
                            },
                            "contacts" : [
                                {
                                    "wa_id"   : "user-1",
                                    "profile" : { "name" : "User One" },
                                }
                            ],
                            "messages" : [
                                {
                                    "from"      : "user-1",
                                    "id"        : "wamid.message-1",
                                    "timestamp" : "1700000000",
                                    "type"      : "text",
                                    "text"      : { "body" : "hello" },
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


def _status_payload_dict() -> dict :
    
    return {
        "object" : "whatsapp_business_account",
        "entry"  : [
            {
                "id"      : "waba-1",
                "changes" : [
                    {
                        "field" : "messages",
                        "value" : {
                            "messaging_product" : "whatsapp",
                            "metadata"          : {
                                "display_phone_number" : "15551234567",
                                "phone_number_id"      : "phone-1",
                            },
                            "statuses" : [
                                {
                                    "id"           : "wamid.message-1",
                                    "recipient_id" : "user-1",
                                    "status"       : "delivered",
                                    "timestamp"    : "1700000001",
                                    "conversation" : {
                                        "id"     : "conversation-1",
                                        "origin" : { "type" : "service" },
                                    },
                                    "pricing" : {
                                        "billable"      : False,
                                        "category"      : "service",
                                        "pricing_model" : "CBP",
                                    },
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


def _patch_fake_connection( monkeypatch, payload_row : dict) -> _FakeConnection :
    
    conn = _FakeConnection(payload_row)
    monkeypatch.setattr(
        supabase_storage,
        "sync_pooled_conection",
        lambda _database_url : _FakeConnectionContext(conn),
    )
    
    return conn


def test_webhook_payload_hash_is_canonical() -> None :
    
    payload_1 = supabase_storage.WhatsAppPayload.model_validate(_message_payload_dict())
    payload_2 = supabase_storage.WhatsAppPayload.model_validate(_message_payload_dict())
    
    assert _payload_hash(payload_1) == _payload_hash(payload_2)


def test_webhook_payload_duplicate_does_not_explode_rows( monkeypatch) -> None :
    
    payload = supabase_storage.WhatsAppPayload.model_validate(_message_payload_dict())
    conn    = _patch_fake_connection(
        monkeypatch,
        { "id" : 11, "inserted" : False },
    )
    
    stored = supabase_storage.webhook_payload_write(payload)
    
    assert stored is False
    assert len(conn.calls) == 1
    assert conn.calls[0][0] == SQL_INSERT_WEBHOOK_PAYLOAD


def test_webhook_message_payload_expands_message_row( monkeypatch) -> None :
    
    payload = supabase_storage.WhatsAppPayload.model_validate(_message_payload_dict())
    conn    = _patch_fake_connection(
        monkeypatch,
        { "id" : 12, "inserted" : True },
    )
    
    stored = supabase_storage.webhook_payload_write(payload)
    
    assert stored is True
    assert [ call[0] for call in conn.calls ] == [
        SQL_INSERT_WEBHOOK_PAYLOAD,
        SQL_INSERT_WEBHOOK_MESSAGE,
    ]
    
    msg_params = conn.calls[1][1]
    assert msg_params["payload_id"] == 12
    assert msg_params["operator_id"] == "phone-1"
    assert msg_params["waba_id"] == "waba-1"
    assert msg_params["user_id"] == "user-1"
    assert msg_params["message_id"] == "wamid.message-1"
    assert msg_params["message_type"] == "text"


def test_webhook_status_only_payload_expands_status_row( monkeypatch) -> None :
    
    payload = supabase_storage.WhatsAppPayload.model_validate(_status_payload_dict())
    conn    = _patch_fake_connection(
        monkeypatch,
        { "id" : 13, "inserted" : True },
    )
    
    assert payload.has_messages() is False
    
    stored = supabase_storage.webhook_payload_write(payload)
    
    assert stored is True
    assert [ call[0] for call in conn.calls ] == [
        SQL_INSERT_WEBHOOK_PAYLOAD,
        SQL_INSERT_WEBHOOK_STATUS,
    ]
    
    status_params = conn.calls[1][1]
    assert status_params["payload_id"] == 13
    assert status_params["operator_id"] == "phone-1"
    assert status_params["recipient_id"] == "user-1"
    assert status_params["message_id"] == "wamid.message-1"
    assert status_params["status"] == "delivered"
    assert status_params["conversation_id"] == "conversation-1"
    assert status_params["pricing_category"] == "service"


class _QueueStub :
    
    def __init__(self) -> None :
        self.enqueued = False
        return
    
    def enqueue(self, _payload) -> bool :
        self.enqueued = True
        return True


def test_listener_enqueues_when_webhook_storage_fails( monkeypatch) -> None :
    
    queue = _QueueStub()
    app   = Listener( __name__, queue)
    
    def raise_storage_error(_payload) -> bool :
        raise RuntimeError("storage offline")
    
    monkeypatch.setattr(
        supabase_storage,
        "webhook_payload_write",
        raise_storage_error,
    )
    
    response = app.test_client().post(
        "/webhook",
        json = _message_payload_dict(),
    )
    
    data = response.get_json()
    
    assert response.status_code == 200
    assert data["status"] == "ok"
    assert data["enqueued"] is True
    assert data["stored"] is False
    assert "storage offline" in data["storage_error"]
    assert queue.enqueued is True


class _FakeQueueConnection :
    
    def __init__( self, rows : dict[str, dict | None]) -> None :
        self.rows  = rows
        self.calls = []
        return
    
    def execute( self, sql : str, params : dict) -> _FakeCursor :
        self.calls.append(( sql, params))
        return _FakeCursor(self.rows.get(sql))


class _FakeQueueConnectionContext :
    
    def __init__( self, conn : _FakeQueueConnection) -> None :
        self.conn = conn
        return
    
    def __enter__(self) -> _FakeQueueConnection :
        return self.conn
    
    def __exit__( self, exc_type, exc, tb) -> None :
        return


def _patch_fake_queue_connection(
    monkeypatch,
    rows : dict[str, dict | None],
) -> _FakeQueueConnection :
    
    conn = _FakeQueueConnection(rows)
    monkeypatch.setattr(
        queue_db,
        "sync_pooled_conection",
        lambda _database_url : _FakeQueueConnectionContext(conn),
    )
    
    return conn


def test_queue_enqueue_returns_true_only_for_insert( monkeypatch) -> None :
    
    payload = supabase_storage.WhatsAppPayload.model_validate(_message_payload_dict())
    conn    = _patch_fake_queue_connection(
        monkeypatch,
        { queue_db.SQL_ENQUEUE_PAYLOAD : { "id" : 1 } },
    )
    
    queue = queue_db.QueueDB()
    
    assert queue.enqueue(payload) is True
    assert conn.calls[0][0] == queue_db.SQL_ENQUEUE_PAYLOAD
    assert "payload_hash" in conn.calls[0][1]
    assert "payload" in conn.calls[0][1]


def test_queue_enqueue_returns_false_for_duplicate( monkeypatch) -> None :
    
    payload = supabase_storage.WhatsAppPayload.model_validate(_message_payload_dict())
    _patch_fake_queue_connection(
        monkeypatch,
        { queue_db.SQL_ENQUEUE_PAYLOAD : None },
    )
    
    queue = queue_db.QueueDB()
    
    assert queue.enqueue(payload) is False


def test_queue_claim_next_validates_payload( monkeypatch) -> None :
    
    payload = supabase_storage.WhatsAppPayload.model_validate(_message_payload_dict())
    conn    = _patch_fake_queue_connection(
        monkeypatch,
        {
            queue_db.SQL_CLAIM_NEXT : {
                "row_id"  : 7,
                "payload" : payload.model_dump( mode = "json", by_alias = True),
            }
        },
    )
    
    queue = queue_db.QueueDB()
    item  = queue.claim_next()
    
    assert item["row_id"] == 7
    assert isinstance( item["payload"], supabase_storage.WhatsAppPayload)
    assert item["payload"].entry[0].changes[0].value.messages[0].id == "wamid.message-1"
    assert conn.calls[0][0] == queue_db.SQL_CLAIM_NEXT


def test_queue_mark_done_and_error( monkeypatch) -> None :
    
    conn  = _patch_fake_queue_connection(monkeypatch, {})
    queue = queue_db.QueueDB()
    
    queue.mark_done(3)
    queue.mark_error( 4, "failed")
    
    assert conn.calls[0] == ( queue_db.SQL_MARK_QUEUE_DONE, { "row_id" : 3 })
    assert conn.calls[1] == (
        queue_db.SQL_MARK_QUEUE_ERROR,
        { "row_id" : 4, "last_error" : "failed" },
    )
