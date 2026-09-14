from __future__ import annotations

import asyncio

from datetime import (
    UTC,
    datetime,
)
from uuid import uuid4

from wa_agents.queue_worker import (
    AsyncQueueWorker,
    QueueWorker,
    _job_and_message,
)


def _queue_item() -> dict :
    return {
        "row_id"              : 1,
        "id"                  : 21,
        "payload"             : 11,
        "is_echo"             : False,
        "msg_id"              : "wamid.ABC123=",
        "msg_ts"              : datetime( 2026, 11, 27, tzinfo = UTC),
        "msg_type"            : "text",
        "msg_data"            : { "text" : { "body" : "Hola" } },
        "contact"             : 31,
        "wa_id"               : "593995341161",
        "user_id"             : None,
        "profile_name"        : "Test User",
        "profile_username"    : None,
        "business"            : 41,
        "waba_id"             : "123456789012345",
        "phone_number_id"     : "1234567890",
        "display_phone_number" : "15551234567",
        "owner_token"         : uuid4(),
    }


class _QueueStub :

    def __init__(self) -> None :
        self.item       = _queue_item()
        self.done_ids   = []
        self.error_ids  = []

    def claim_next(self) -> dict | None :
        item      = self.item
        self.item = None
        return item

    def mark_done( self, row_id : int) -> None :
        self.done_ids.append(row_id)

    def mark_error( self, row_id : int) -> None :
        self.error_ids.append(row_id)


class _AsyncQueueStub :

    def __init__(self) -> None :
        self.item       = _queue_item()
        self.done_ids   = []
        self.error_ids  = []

    async def claim_next(self) -> dict | None :
        item      = self.item
        self.item = None
        return item

    async def mark_done( self, row_id : int) -> None :
        self.done_ids.append(row_id)

    async def mark_error( self, row_id : int) -> None :
        self.error_ids.append(row_id)


class _ImmediateReplyHandler :

    instances = []

    def __init__( self, _operator, _user, **kwargs) -> None :
        self.kwargs               = kwargs
        self._responded_in_ingest = True
        self.released             = False
        self.__class__.instances.append(self)

    def process_message( self, _message, _media_content = None) -> bool :
        return False

    def release_contact_lease(self) -> bool :
        self.released = True
        return True


class _AsyncImmediateReplyHandler :

    instances = []

    def __init__( self, _operator, _user, **kwargs) -> None :
        self.kwargs               = kwargs
        self._responded_in_ingest = True
        self.released             = False
        self.__class__.instances.append(self)

    async def process_message( self, _message, _media_content = None) -> bool :
        return False

    async def release_contact_lease(self) -> bool :
        self.released = True
        return True


class _DelayedReplyHandler :

    instances = []

    def __init__( self, _operator, _user, **kwargs) -> None :
        self.kwargs               = kwargs
        self._responded_in_ingest = False
        self.released             = False
        self.renewed              = False
        self.__class__.instances.append(self)

    def process_message( self, _message, _media_content = None) -> bool :
        return True

    def generate_response(self) -> bool :
        return False

    def renew_contact_lease(self) -> bool :
        self.renewed = True
        return True

    def release_contact_lease(self) -> bool :
        self.released = True
        return True


def test_queue_row_reconstructs_job_and_message() -> None :
    job, message = _job_and_message(_queue_item())

    assert job.business_id == 41
    assert job.contact_id == 31
    assert job.api_inbound_msg_id == 21
    assert isinstance( hash(job), int)
    assert message.id == "wamid.ABC123="
    assert message.text.body == "Hola"


def test_queue_worker_releases_lease_after_ingest_reply() -> None :
    _ImmediateReplyHandler.instances.clear()
    queue  = _QueueStub()
    worker = QueueWorker(queue, _ImmediateReplyHandler)

    assert worker._process_message() is True
    assert queue.done_ids == [ 1 ]
    assert queue.error_ids == []
    assert not worker._job_td
    assert _ImmediateReplyHandler.instances[0].released is True
    assert _ImmediateReplyHandler.instances[0].kwargs["contact_id"] == 31


def test_async_queue_worker_releases_lease_after_ingest_reply() -> None :
    _AsyncImmediateReplyHandler.instances.clear()
    queue  = _AsyncQueueStub()
    worker = AsyncQueueWorker(queue, _AsyncImmediateReplyHandler)

    assert asyncio.run(worker._process_message()) is True
    assert queue.done_ids == [ 1 ]
    assert queue.error_ids == []
    assert not worker._job_td
    assert _AsyncImmediateReplyHandler.instances[0].released is True
    assert _AsyncImmediateReplyHandler.instances[0].kwargs["contact_id"] == 31


def test_queue_worker_holds_lease_through_delayed_response() -> None :
    _DelayedReplyHandler.instances.clear()
    queue  = _QueueStub()
    worker = QueueWorker(queue, _DelayedReplyHandler)

    assert worker._process_message() is True

    ingest_handler = _DelayedReplyHandler.instances[0]
    jobs           = list(worker._job_td)

    assert ingest_handler.released is False
    assert len(jobs) == 1
    assert worker._process_jobs(jobs) is True

    response_handler = _DelayedReplyHandler.instances[1]

    assert response_handler.renewed is True
    assert response_handler.released is True
    assert not worker._job_td
