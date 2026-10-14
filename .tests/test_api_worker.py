from __future__ import annotations

import asyncio

from datetime import (
    UTC,
    datetime,
    timedelta,
)
from uuid import uuid4

import pytest

from wa_agents import api_worker
from wa_agents.api_worker import (
    WhatsAppAPIWorker,
    _job_and_message,
)
from wa_agents.io_models import WhatsApp_IB_MessageEcho


def _queue_item(
    *,
    row_id     : int = 1,
    message_id : int = 21,
    msg_id     : str = "wamid.ABC123=",
    body       : str = "Hola",
    contact    : int = 31,
) -> dict :
    return {
        "row_id"              : row_id,
        "id"                  : message_id,
        "payload"             : 11,
        "is_echo"             : False,
        "msg_id"              : msg_id,
        "msg_ts"              : datetime( 2026, 11, 27, tzinfo = UTC),
        "msg_type"            : "text",
        "msg_data"            : { "text" : { "body" : body } },
        "contact"             : contact,
        "wa_id"               : str(593995341130 + contact),
        "user_id"             : None,
        "profile_name"        : "Test User",
        "profile_username"    : None,
        "business"            : 41,
        "waba_id"             : "123456789012345",
        "phone_number_id"     : "1234567890",
        "display_phone_number" : "15551234567",
        "handler_id"           : 7,
        "handler_key"          : "default",
        "silence_timeout_minutes" : None,
        "owner_token"         : uuid4(),
    }


def test_legacy_worker_class_names_are_removed() -> None :
    assert not hasattr( api_worker, "QueueWorker")
    assert not hasattr( api_worker, "AsyncQueueWorker")
    assert not hasattr( api_worker, "WhatsAppDatabaseQueueWorker")


class _AsyncQueueStub :

    def __init__(self) -> None :
        self.item       = _queue_item()
        self.contact_items = []
        self.done_ids   = []
        self.error_ids  = []
        self.claimed_keys = None
        self.contact_claims = []
        self.pending_echo  = False
        self.pending_echo_checks = []
        self.released_leases = []
        self.storage      = object()

    async def claim_next( self, handler_keys) -> dict | None :
        self.claimed_keys = handler_keys
        item      = self.item
        self.item = None
        return item

    async def claim_next_for_contact(
        self,
        contact,
        owner_token,
        after_row_id,
        handler_keys,
    ) -> dict | None :
        self.contact_claims.append(
            ( contact, owner_token, after_row_id, handler_keys)
        )
        if not self.contact_items :
            return None

        item = self.contact_items.pop(0)
        assert item["contact"] == contact
        assert item["row_id"] > after_row_id
        return { **item, "owner_token" : owner_token }

    async def mark_done( self, row_id : int) -> None :
        self.done_ids.append(row_id)

    async def mark_error( self, row_id : int) -> None :
        self.error_ids.append(row_id)

    async def release_contact_lease(self, contact, owner_token) -> bool :
        self.released_leases.append(( contact, owner_token))
        return True

    async def has_pending_echo( self, contact : int, after_row_id : int) -> bool :
        self.pending_echo_checks.append(( contact, after_row_id))
        return self.pending_echo


class _ImmediateReplyHandler :

    instances = []

    def __init__( self, operator, user, **kwargs) -> None :
        self.operator             = operator
        self.user                 = user
        self.kwargs               = kwargs
        self.released             = False
        self.__class__.instances.append(self)

    def process_message( self, _message, _media_content = None) -> bool :
        return False

    def release_contact_lease(self) -> bool :
        self.released = True
        return True


class _AsyncImmediateReplyHandler :

    instances = []

    def __init__( self, operator, user, **kwargs) -> None :
        self.operator             = operator
        self.user                 = user
        self.kwargs               = kwargs
        self.released             = False
        self.__class__.instances.append(self)

    async def process_message( self, _message, _media_content = None) -> bool :
        return False

    async def release_contact_lease(self) -> bool :
        self.released = True
        return True


class _RetailReplyHandler (_ImmediateReplyHandler) :
    HANDLER_KEY = "retail"


class _EnterpriseReplyHandler (_ImmediateReplyHandler) :
    HANDLER_KEY = "enterprise"


class _DelayedReplyHandler :

    instances = []

    def __init__( self, _operator, _user, **kwargs) -> None :
        self.kwargs               = kwargs
        self.released             = False
        self.renewed              = False
        self.ran                  = False
        self.__class__.instances.append(self)

    def process_message( self, _message, _media_content = None) -> bool :
        return True

    def run_while_in_action(self) -> bool :
        self.ran = True
        return False

    def renew_contact_lease(self) -> bool :
        self.renewed = True
        return True

    def release_contact_lease(self) -> bool :
        self.released = True
        return True


def test_queue_row_reconstructs_job_and_message() -> None :
    job, message = _job_and_message(
        _queue_item(),
        timedelta( minutes = 30),
    )

    assert job.operator.row_id == 41
    assert job.user.row_id == 31
    assert job.api_inbound_msg_id == 21
    assert job.handler_id == 7
    assert job.handler_key == "default"
    assert job.queue_row_id == 1
    assert job.silence_timeout == timedelta( minutes = 30)
    assert isinstance( hash(job), int)
    assert message.id == "wamid.ABC123="
    assert message.text.body == "Hola"


def test_api_worker_releases_lease_after_ingest_reply(monkeypatch) -> None :
    monkeypatch.delenv( "QUEUE_SILENCE_TIMEOUT_MINUTES", raising = False)
    _ImmediateReplyHandler.instances.clear()
    queue  = _AsyncQueueStub()
    worker = WhatsAppAPIWorker(
        handler_cls = _ImmediateReplyHandler,
        queue       = queue,
    )

    assert asyncio.run(worker._process_message()) is True
    assert queue.done_ids == [ 1 ]
    assert queue.error_ids == []
    assert not worker._job_td
    assert _ImmediateReplyHandler.instances[0].released is True
    assert _ImmediateReplyHandler.instances[0].user.row_id == 31
    assert _ImmediateReplyHandler.instances[0].kwargs["handler_id"] == 7
    assert _ImmediateReplyHandler.instances[0].kwargs["silence_timeout"] == (
        timedelta( minutes = 30)
    )
    assert queue.claimed_keys == ( "default", )


def test_api_worker_supports_async_handler_methods() -> None :
    _AsyncImmediateReplyHandler.instances.clear()
    queue  = _AsyncQueueStub()
    worker = WhatsAppAPIWorker(
        handler_cls = _AsyncImmediateReplyHandler,
        queue       = queue,
    )

    assert asyncio.run(worker._process_message()) is True
    assert queue.done_ids == [ 1 ]
    assert queue.error_ids == []
    assert not worker._job_td
    assert _AsyncImmediateReplyHandler.instances[0].released is True
    assert _AsyncImmediateReplyHandler.instances[0].user.row_id == 31
    assert queue.claimed_keys == ( "default", )


def test_api_worker_holds_lease_through_delayed_response() -> None :
    _DelayedReplyHandler.instances.clear()
    queue  = _AsyncQueueStub()
    worker = WhatsAppAPIWorker(
        handler_cls = _DelayedReplyHandler,
        queue       = queue,
    )

    async def run() -> tuple[ object, list] :
        assert await worker._process_message() is True
        ingest_handler = _DelayedReplyHandler.instances[0]
        jobs           = worker._job_td.get_all()

        assert ingest_handler.released is False
        assert len(jobs) == 1
        assert await worker._process_jobs(jobs) is True

        return ingest_handler, jobs

    asyncio.run(run())

    response_handler = _DelayedReplyHandler.instances[1]

    assert response_handler.renewed is True
    assert response_handler.released is True
    assert not worker._job_td


def test_api_worker_uses_constructor_timing(monkeypatch) -> None :
    monkeypatch.setenv( "QUEUE_SILENCE_TIMEOUT_MINUTES", "60")
    _DelayedReplyHandler.instances.clear()
    worker = WhatsAppAPIWorker(
        handler_cls        = _DelayedReplyHandler,
        queue              = _AsyncQueueStub(),
        poll_interval_busy = 0.1,
        poll_interval_idle = 2.0,
        response_delay     = 3.0,
        silence_timeout    = timedelta( minutes = 45),
    )
    monkeypatch.setattr( api_worker.time, "time", lambda : 10.0)

    assert asyncio.run(worker._process_message()) is True
    assert worker.poll_interval_busy == 0.1
    assert worker.poll_interval_idle == 2.0
    timed = next(iter(worker._job_td.values()))
    assert timed.response_time == 13.0
    assert timed.job.silence_timeout == timedelta( minutes = 45)


def test_api_worker_uses_environment_silence_timeout(monkeypatch) -> None :
    monkeypatch.setenv( "QUEUE_SILENCE_TIMEOUT_MINUTES", "45")
    worker = WhatsAppAPIWorker(
        handler_cls = _ImmediateReplyHandler,
        queue       = _AsyncQueueStub(),
    )

    assert worker.silence_timeout == timedelta( minutes = 45)


def test_api_worker_uses_configured_silence_timeout() -> None :
    item                            = _queue_item()
    item["silence_timeout_minutes"] = 15

    job, _message = _job_and_message(
        item,
        timedelta( minutes = 30),
    )

    assert job.silence_timeout == timedelta( minutes = 15)


def test_newer_immediate_reply_cancels_delayed_response() -> None :
    class ImmediateSecondReplyHandler (_DelayedReplyHandler) :

        def process_message( self, message, _media_content = None) -> bool :
            return message.id != "wamid.SECOND="

    _DelayedReplyHandler.instances.clear()
    queue = _AsyncQueueStub()
    queue.contact_items.append(
        _queue_item(
            row_id     = 2,
            message_id = 22,
            msg_id     = "wamid.SECOND=",
        )
    )
    worker             = WhatsAppAPIWorker(
        handler_cls = ImmediateSecondReplyHandler,
        queue       = queue,
    )

    async def run() -> None :
        assert await worker._process_message() is True
        assert await worker._process_pending_job_messages() == { 31 }

    asyncio.run(run())

    assert queue.done_ids == [ 1, 2 ]
    assert not worker._job_td
    assert ImmediateSecondReplyHandler.instances[-1].released is True


def test_newer_echo_cancels_delayed_response() -> None :
    class EchoAwareHandler (_DelayedReplyHandler) :

        def process_message( self, message, _media_content = None) -> bool :
            return not isinstance( message, WhatsApp_IB_MessageEcho)

    EchoAwareHandler.instances.clear()
    queue = _AsyncQueueStub()
    echo  = _queue_item(
        row_id     = 2,
        message_id = 22,
        msg_id     = "wamid.ECHO123=",
    )
    echo["is_echo"] = True
    echo["msg_data"] = {
        "text" : { "body" : "Operator reply" },
        "to"   : echo["wa_id"],
    }
    queue.contact_items.append(echo)
    worker = WhatsAppAPIWorker(
        handler_cls = EchoAwareHandler,
        queue       = queue,
    )

    async def run() -> None :
        assert await worker._process_message() is True
        assert await worker._process_pending_job_messages() == { 31 }

    asyncio.run(run())

    assert queue.done_ids == [ 1, 2 ]
    assert not worker._job_td
    assert sum(handler.ran for handler in EchoAwareHandler.instances) == 0


def test_response_delay_debounces_contact_message_burst(monkeypatch) -> None :
    _DelayedReplyHandler.instances.clear()
    now    = [ 10.0 ]
    queue  = _AsyncQueueStub()
    worker = WhatsAppAPIWorker(
        handler_cls    = _DelayedReplyHandler,
        queue          = queue,
        response_delay = 5.0,
    )
    monkeypatch.setattr( api_worker.time, "time", lambda : now[0])

    async def run() -> None :
        assert await worker._process_message() is True
        first_job = worker._job_td.get_all()[0]
        owner     = first_job.owner_token

        now[0] = 11.0
        queue.contact_items.append(
            _queue_item(
                row_id     = 2,
                message_id = 22,
                msg_id     = "wamid.SECOND=",
            )
        )
        assert await worker._process_pending_job_messages() == { 31 }
        second_timed = next(iter(worker._job_td.values()))
        assert second_timed.job.queue_row_id == 2
        assert second_timed.job.owner_token == owner
        assert second_timed.response_time == 16.0

        now[0] = 12.0
        queue.contact_items.append(
            _queue_item(
                row_id     = 3,
                message_id = 23,
                msg_id     = "wamid.THIRD=",
            )
        )
        assert await worker._process_pending_job_messages() == { 31 }
        latest_timed = next(iter(worker._job_td.values()))
        assert latest_timed.job.queue_row_id == 3
        assert latest_timed.job.api_inbound_msg_id == 23
        assert latest_timed.response_time == 17.0

        now[0] = 16.9
        assert worker._job_td.get_due_now() == []

        now[0] = 17.0
        assert await worker._process_jobs(worker._job_td.get_due_now()) is True

    asyncio.run(run())

    assert queue.done_ids == [ 1, 2, 3 ]
    assert queue.error_ids == []
    assert not worker._job_td
    assert sum(handler.ran for handler in _DelayedReplyHandler.instances) == 1
    response_handler = next(
        handler for handler in _DelayedReplyHandler.instances if handler.ran
    )
    assert response_handler.kwargs["api_inbound_msg_id"] == 23


def test_delayed_response_deadlines_are_independent_per_contact() -> None :
    jobs = api_worker.JobTimeDict()
    first, _ = _job_and_message(
        _queue_item( contact = 31),
        timedelta( minutes = 30),
    )
    second, _ = _job_and_message(
        _queue_item(
            row_id     = 2,
            message_id = 22,
            msg_id     = "wamid.SECOND=",
            contact    = 32,
        ),
        timedelta( minutes = 30),
    )
    first_latest, _ = _job_and_message(
        _queue_item(
            row_id     = 3,
            message_id = 23,
            msg_id     = "wamid.THIRD=",
            contact    = 31,
        ),
        timedelta( minutes = 30),
    )

    jobs.schedule( first, 10.0)
    jobs.schedule( second, 11.0)
    jobs.schedule( first_latest, 12.0)

    assert len(jobs) == 2
    assert jobs[31] == api_worker.TimedHandlerJob( first_latest, 12.0)
    assert jobs[32] == api_worker.TimedHandlerJob( second, 11.0)


def test_messages_after_quiet_period_start_a_new_burst(monkeypatch) -> None :
    _DelayedReplyHandler.instances.clear()
    now    = [ 10.0 ]
    queue  = _AsyncQueueStub()
    worker = WhatsAppAPIWorker(
        handler_cls    = _DelayedReplyHandler,
        queue          = queue,
        response_delay = 5.0,
    )
    monkeypatch.setattr( api_worker.time, "time", lambda : now[0])

    async def run() -> None :
        assert await worker._process_message() is True

        now[0] = 15.0
        assert await worker._process_jobs(worker._job_td.get_due_now()) is True
        assert not worker._job_td

        queue.item = _queue_item(
            row_id     = 2,
            message_id = 22,
            msg_id     = "wamid.SECOND=",
        )
        assert await worker._process_message() is True

        now[0] = 20.0
        assert await worker._process_jobs(worker._job_td.get_due_now()) is True

    asyncio.run(run())

    assert queue.done_ids == [ 1, 2 ]
    assert sum(handler.ran for handler in _DelayedReplyHandler.instances) == 2


def test_newer_message_failure_keeps_previous_delayed_response() -> None :
    class FailingSecondMessageHandler (_DelayedReplyHandler) :

        def process_message( self, message, _media_content = None) -> bool :
            if message.id == "wamid.SECOND=" :
                raise RuntimeError("test ingest failure")
            return True

    FailingSecondMessageHandler.instances.clear()
    queue = _AsyncQueueStub()
    queue.contact_items.append(
        _queue_item(
            row_id     = 2,
            message_id = 22,
            msg_id     = "wamid.SECOND=",
        )
    )
    worker = WhatsAppAPIWorker(
        handler_cls = FailingSecondMessageHandler,
        queue       = queue,
    )

    async def run() -> None :
        assert await worker._process_message() is True
        first_job = worker._job_td.get_all()[0]

        assert await worker._process_pending_job_messages() == { 31 }
        assert worker._job_td.get_all() == [ first_job ]

    asyncio.run(run())

    assert queue.done_ids == [ 1 ]
    assert queue.error_ids == [ 2 ]
    assert len(worker._job_td) == 1


def test_api_worker_claims_and_dispatches_registered_handler_keys() -> None :
    _RetailReplyHandler.instances.clear()
    queue                     = _AsyncQueueStub()
    queue.item["handler_key"] = "retail"
    worker                    = WhatsAppAPIWorker(
        queue           = queue,
        handler_classes = {
            "enterprise" : _EnterpriseReplyHandler,
            "retail"     : _RetailReplyHandler,
        },
    )

    assert asyncio.run(worker._process_message()) is True
    assert queue.claimed_keys == ( "enterprise", "retail" )
    assert len(_RetailReplyHandler.instances) == 1


def test_api_worker_rejects_registry_key_mismatch() -> None :
    with pytest.raises( ValueError, match = "does not match") :
        WhatsAppAPIWorker(
            queue           = _AsyncQueueStub(),
            handler_classes = { "wrong" : _RetailReplyHandler },
        )


@pytest.mark.parametrize( "handler_key", [ "", "has whitespace", 1 ] )
@pytest.mark.parametrize( "use_registry", [ False, True ] )
def test_api_worker_rejects_invalid_handler_key(
    handler_key,
    use_registry : bool,
) -> None :
    Handler = type(
        "InvalidKeyHandler",
        ( _ImmediateReplyHandler, ),
        { "HANDLER_KEY" : handler_key },
    )
    kwargs = (
        { "handler_classes" : { handler_key : Handler } }
        if use_registry else
        { "handler_cls" : Handler }
    )

    with pytest.raises( ValueError, match = "Invalid handler registry key") :
        WhatsAppAPIWorker( queue = _AsyncQueueStub(), **kwargs)


def test_single_handler_shorthand_aligns_queue_fallback_key() -> None :
    queue  = _AsyncQueueStub()
    worker = WhatsAppAPIWorker(
        handler_cls = _RetailReplyHandler,
        queue       = queue,
    )

    assert queue.fallback_handler_key == "retail"
    assert worker.handler_keys == ( "retail", )
