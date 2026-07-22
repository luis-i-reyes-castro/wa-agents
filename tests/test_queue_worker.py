from __future__ import annotations

import asyncio
import os

os.environ.setdefault("BUCKET_REGION", "test-region")
os.environ.setdefault("BUCKET_KEY_ID", "test-key-id")
os.environ.setdefault("BUCKET_KEY_SECRET", "test-key-secret")
os.environ.setdefault("BUCKET_NAME", "test-bucket")
os.environ.setdefault("BUCKET_ENDPOINT", "https://example.com")

from wa_agents.basemodels import WhatsAppPayload
from wa_agents.queue_worker import (
    AsyncQueueWorker,
    QueueWorker,
)


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
                                    "profile" : { "name" : "Test User" },
                                    "wa_id"   : "593995341161",
                                }
                            ],
                            "messages" : [
                                {
                                    "from"      : "593995341161",
                                    "id"        : "wamid.message-1",
                                    "timestamp" : "1795737600",
                                    "text"      : { "body" : "Hola" },
                                    "type"      : "text",
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


class _QueueStub :
    
    def __init__( self, payload : WhatsAppPayload) -> None :
        self._payload = payload
        self.done_ids : list[int] = []
    
    def claim_next(self) -> dict | None :
        if self._payload is None :
            return None
        payload       = self._payload
        self._payload = None
        return { "row_id" : 1, "payload" : payload }
    
    def mark_done( self, row_id : int) -> None :
        self.done_ids.append(row_id)
    
    def mark_error( self, _row_id : int, _error_msg : str) -> None :
        raise AssertionError("mark_error should not be called")


class _AsyncQueueStub :
    
    def __init__( self, payload : WhatsAppPayload) -> None :
        self._payload = payload
        self.done_ids : list[int] = []
    
    async def claim_next(self) -> dict | None :
        if self._payload is None :
            return None
        payload       = self._payload
        self._payload = None
        return { "row_id" : 1, "payload" : payload }
    
    async def mark_done( self, row_id : int) -> None :
        self.done_ids.append(row_id)
    
    async def mark_error( self, _row_id : int, _error_msg : str) -> None :
        raise AssertionError("mark_error should not be called")


class _ImmediateReplyHandler :
    
    def __init__( self, operator, user) -> None :
        self.operator = operator
        self.user     = user
        self._responded_in_ingest = False
    
    def process_message( self, _msg, _media_content = None ) -> bool :
        self._responded_in_ingest = True
        return False


class _AsyncImmediateReplyHandler :
    
    def __init__( self, operator, user) -> None :
        self.operator = operator
        self.user     = user
        self._responded_in_ingest = False
    
    async def process_message( self, _msg, _media_content = None ) -> bool :
        self._responded_in_ingest = True
        return False


def test_queue_worker_clears_stale_job_after_ingest_reply() -> None :
    
    payload = WhatsAppPayload.model_validate(_message_payload_dict())
    queue   = _QueueStub(payload)
    worker  = QueueWorker(queue, _ImmediateReplyHandler)
    
    value    = payload.entry[0].changes[0].value
    operator = value.metadata
    user     = value.contacts[0]
    job      = ( operator, user )
    
    worker._job_td[job] = 9999999999.0
    
    assert worker._process_payload() is True
    assert queue.done_ids == [1]
    assert job not in worker._job_td


def test_async_queue_worker_clears_stale_job_after_ingest_reply() -> None :
    
    payload = WhatsAppPayload.model_validate(_message_payload_dict())
    queue   = _AsyncQueueStub(payload)
    worker  = AsyncQueueWorker(queue, _AsyncImmediateReplyHandler)
    
    value    = payload.entry[0].changes[0].value
    operator = value.metadata
    user     = value.contacts[0]
    job      = ( operator, user )
    
    worker._job_td[job] = 9999999999.0
    
    assert asyncio.run(worker._process_payload()) is True
    assert queue.done_ids == [1]
    assert job not in worker._job_td
