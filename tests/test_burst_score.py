"""
Burst-score integration and worker regression tests.

Run unit tests with `python -m unittest discover -s tests`.
Include database tests with `python tests/test_burst_score.py --database-url URL`
after building abc_DDL.sql in a disposable PostgreSQL database.
"""

import argparse
import asyncio
import math
import psycopg
import unittest

from datetime import (
    UTC,
    datetime,
    timedelta,
)
from types import SimpleNamespace
from unittest.mock import (
    AsyncMock,
    Mock,
    patch,
)
from uuid import uuid4

from sofia_utils.psycopg import (
    close_async_database_connection_pool,
    close_sync_database_connection_pool,
)
from wa_agents.api_server import WhatsAppAPIServer
from wa_agents.api_worker import (
    HandlerJob,
    WhatsAppAPIWorker,
)
from wa_agents.database_queue import WhatsAppDatabaseQueue
from wa_agents.supabase import (
    AsyncSupabaseStorage,
    SyncSupabaseStorage,
    WhatsAppDatabaseRecord_Business,
    WhatsAppDatabaseRecord_Contact,
)


DATABASE_URL = None


class Handler :
    HANDLER_KEY = "default"


class BurstWorkerTests (unittest.IsolatedAsyncioTestCase) :

    def setUp(self) -> None :
        
        self.queue = SimpleNamespace(
            storage = SimpleNamespace(
                update_contact_burst_score = AsyncMock(
                    return_value = { "score" : 2.0, "response_delay" : 16.0 },
                ),
                renew_contact_lease = AsyncMock( return_value = True),
            ),
            claim_next = AsyncMock( return_value = None),
            claim_next_for_contact = AsyncMock( return_value = None),
            mark_done = AsyncMock(),
            mark_error = AsyncMock(),
        )
        self.worker = WhatsAppAPIWorker( handler_cls = Handler, queue = self.queue)
        self.handler = SimpleNamespace(
            process_message = AsyncMock( return_value = True),
            renew_contact_lease = AsyncMock( return_value = True),
            release_contact_lease = AsyncMock(),
            run_while_in_action = AsyncMock( return_value = False),
        )
        self.worker._handler = Mock( return_value = self.handler)
        self.job = HandlerJob(
            queue_row_id = 1,
            handler_id = None,
            handler_key = "default",
            operator = WhatsAppDatabaseRecord_Business(
                row_id = 1,
                waba_id = "123456789",
                phone_number_id = "987654321",
                display_phone_number = "593999999999",
            ),
            user = WhatsAppDatabaseRecord_Contact( row_id = 1, wa_id = "593999999998"),
            api_inbound_msg_id = 1,
            owner_token = uuid4(),
            silence_timeout = timedelta( minutes = 30),
        )
        self.message = SimpleNamespace( media_data = None)
        self.reconstruct = patch(
            "wa_agents.api_worker._job_and_message",
            return_value = ( self.job, self.message ),
        )
        self.reconstruct.start()
        self.addCleanup(self.reconstruct.stop)

    async def test_adaptive_deadline_and_scoring_before_immediate_send(self) -> None :
        
        async def process( *args) :
            self.queue.storage.update_contact_burst_score.assert_awaited_once()
            return True

        self.handler.process_message.side_effect = process
        with patch( "wa_agents.api_worker.time.time", return_value = 100.0) :
            await self.worker._process_claimed_message({ "row_id" : 1 })
        self.assertEqual( self.worker._job_td[1].response_time, 116.0)
        self.handler.renew_contact_lease.assert_awaited_once()
        self.handler.release_contact_lease.assert_not_awaited()
        self.queue.mark_done.assert_awaited_once_with(1)

    async def test_ingestion_failure_preserves_previous_job(self) -> None :
        
        self.worker._job_td.schedule( self.job, 123.0)
        self.handler.process_message.side_effect = RuntimeError("ingestion failed")
        with self.assertLogs( level = "ERROR") :
            await self.worker._process_claimed_message(
                { "row_id" : 2 },
                previous_job = self.job,
            )
        self.assertEqual( self.worker._job_td[1].response_time, 123.0)
        self.queue.mark_error.assert_awaited_once_with(2)
        self.handler.release_contact_lease.assert_not_awaited()

    async def test_false_result_cancels_job(self) -> None :
        
        self.worker._job_td.schedule( self.job, 123.0)
        self.handler.process_message.return_value = False
        await self.worker._process_claimed_message(
            { "row_id" : 2 },
            previous_job = self.job,
        )
        self.assertFalse(self.worker._job_td)
        self.handler.release_contact_lease.assert_awaited_once()

    async def test_waiting_lease_renewal_and_loss(self) -> None :
        
        self.worker._job_td.schedule( self.job, math.inf)
        with patch( "wa_agents.api_worker.time.monotonic", return_value = 100.0) :
            self.worker._last_lease_renewal = 0.0
            self.assertTrue(await self.worker.tick())
            self.queue.storage.renew_contact_lease.assert_awaited_once()
            self.queue.storage.renew_contact_lease.return_value = None
            self.worker._last_lease_renewal = 0.0
            self.assertFalse(await self.worker.tick())
        self.assertFalse(self.worker._job_td)

    async def test_sync_handler_and_fractional_deadline(self) -> None :
        
        self.handler.process_message = Mock( return_value = True)
        self.handler.renew_contact_lease = Mock( return_value = True)
        self.queue.storage.update_contact_burst_score.return_value = {
            "response_delay" : 4 * math.sqrt(2),
        }
        with patch( "wa_agents.api_worker.time.time", return_value = 100.0) :
            await self.worker._process_claimed_message({ "row_id" : 1 })
        self.assertAlmostEqual(
            self.worker._job_td[1].response_time,
            100 + 4 * math.sqrt(2),
        )

    async def test_due_job_drains_newer_arrival_before_response(self) -> None :
        
        self.worker._job_td.schedule( self.job, 0.0)
        self.queue.claim_next_for_contact.return_value = { "row_id" : 2 }
        await self.worker._process_jobs([ self.job ])
        self.handler.run_while_in_action.assert_not_awaited()
        self.assertGreater( self.worker._job_td[1].response_time, 0.0)

    async def test_burst_restarts_deadline_and_runs_one_response(self) -> None :
        
        for i, delay in enumerate(( 4.0, 8.0, 16.0 )) :
            self.queue.storage.update_contact_burst_score.return_value = {
                "response_delay" : delay,
            }
            with patch( "wa_agents.api_worker.time.time", return_value = 100.0 + i) :
                await self.worker._process_claimed_message(
                    { "row_id" : i + 1 },
                    previous_job = self.job if i else None,
                )
        with patch( "wa_agents.api_worker.time.time", return_value = 117.0) :
            await self.worker.tick()
        self.handler.run_while_in_action.assert_not_awaited()
        with patch( "wa_agents.api_worker.time.time", return_value = 118.0) :
            await self.worker.tick()
        self.handler.run_while_in_action.assert_awaited_once()
        self.assertFalse(self.worker._job_td)

    def test_configuration(self) -> None :
        
        with patch.dict( "os.environ", { "QUEUE_RESPONSE_DELAY" : "8" }) :
            worker = WhatsAppAPIWorker( handler_cls = Handler, queue = self.queue)
        self.assertEqual( worker.response_delay, 8.0)
        server = WhatsAppAPIServer(
            apps = ( { "app" : "test", "id" : "123", "secret" : "test-secret" }, ),
            handler_cls = Handler,
            queue = self.queue,
            response_delay = 2.0,
            max_response_delay = 32.0,
            burst_decay_period = timedelta( days = 1),
        )
        self.assertEqual( server.max_response_delay, 32.0)
        self.assertEqual( server.burst_decay_period, timedelta( days = 1))
        for kwargs in (
            { "response_delay" : 0 },
            { "response_delay" : math.nan },
            { "max_response_delay" : math.inf },
            { "response_delay" : 8, "max_response_delay" : 4 },
            { "burst_decay_period" : timedelta(0) },
        ) :
            with self.subTest(kwargs = kwargs), self.assertRaises(ValueError) :
                WhatsAppAPIWorker(
                    handler_cls = Handler,
                    queue = self.queue,
                    **kwargs,
                )


class BurstStorageTests (unittest.IsolatedAsyncioTestCase) :

    def setUp(self) -> None :
        
        if not DATABASE_URL :
            self.skipTest("Pass --database-url for a disposable PostgreSQL database")
        self.db = psycopg.connect( DATABASE_URL, autocommit = True)
        self.addCleanup(self.db.close)
        self.addCleanup(close_sync_database_connection_pool)
        self.storage = SyncSupabaseStorage(DATABASE_URL)
        self.async_storage = AsyncSupabaseStorage(DATABASE_URL)
        self.queue = WhatsAppDatabaseQueue(DATABASE_URL)
        number = str(uuid4().int)
        phone = self.storage.upsert_business(
            waba_id = number,
            phone_number_id = number,
            display_phone_number = number,
        )
        self.storage.activate_phone_number(number)
        self.phone = phone["id"]
        self.addCleanup(
            self.db.execute,
            "DELETE FROM wa_agents.api_businesses WHERE waba_id = %s",
            ( number, ),
        )
        self.contact = self.storage.upsert_contact(
            business = self.phone,
            wa_id = "593999999998",
            user_id = None,
        )["id"]
        self.payload = self.storage.insert_inbound_payload({ "test" : number })["id"]
        self.addCleanup(
            self.db.execute,
            "DELETE FROM wa_agents.api_inbound_payloads WHERE id = %s",
            ( self.payload, ),
        )
        self.owner = uuid4()
        self.storage.acquire_contact_lease( self.contact, self.owner)
        self.start = datetime( 2025, 1, 1, tzinfo = UTC)

    async def asyncTearDown(self) -> None :
        
        await close_async_database_connection_pool()

    def arrival(
        self,
        seconds : float,
        *,
        echo : bool = False,
        msg_type : str = "text",
        history_chunk : int | None = None,
    ) -> int :
        
        msg_id = "wamid." + uuid4().hex
        self.storage.insert_inbound_message(
            contact = self.contact,
            payload = self.payload,
            msg_id = msg_id,
            msg_ts = self.start + timedelta( seconds = seconds),
            msg_type = msg_type,
            msg_data = { "text" : { "body" : "Hola" } },
            is_echo = echo,
            history_chunk = history_chunk,
        )
        self.queue._enqueue_message(msg_id)
        return self.db.execute(
            "UPDATE wa_agents.api_to_case_handler_queue "
            "SET created_at = %s WHERE msg_id = %s RETURNING id",
            ( self.start + timedelta( seconds = seconds), msg_id ),
        ).fetchone()[0]

    def reply( self, seconds : float) -> None :
        
        result = self.storage.insert_outbound_message(
            contact = self.contact,
            msg_id = "wamid." + uuid4().hex,
            msg_type = "text",
            msg_data = { "text" : { "body" : "Reply" } },
        )
        self.db.execute(
            "UPDATE wa_agents.api_outbound_messages SET sent_at = %s WHERE id = %s",
            ( self.start + timedelta( seconds = seconds), result["id"] ),
        )

    def score( self, row : int) -> dict | None :
        
        return self.storage.update_contact_burst_score(
            queue_row_id = row,
            owner_token = self.owner,
            response_delay = 4.0,
            max_response_delay = 60.0,
            burst_decay_period = timedelta( hours = 1),
        )

    async def test_fractional_decay_reply_and_zero_floor(self) -> None :
        
        self.assertEqual( self.score(self.arrival(0))["score"], 0)
        self.assertEqual( self.score(self.arrival(0))["score"], 1)
        half_hour = self.score(self.arrival(1800))
        self.assertEqual( half_hour["score"], 1.5)
        self.assertAlmostEqual( half_hour["response_delay"], 4 * 2**1.5)
        self.reply(1801)
        self.assertEqual( self.score(self.arrival(3600))["score"], 1)
        self.reply(3601)
        self.assertEqual( self.score(self.arrival(86400))["score"], 0)

    async def test_cap_ten_sync_async_and_restart(self) -> None :
        
        for i in range(14) :
            row = self.arrival(0)
            result = await self.async_storage.update_contact_burst_score(
                queue_row_id = row,
                owner_token = self.owner,
                response_delay = 4.0,
                max_response_delay = 60.0,
                burst_decay_period = timedelta( hours = 1),
            )
            self.assertEqual( result["score"], min( 10, i))
            self.assertEqual( result["response_delay"], min( 60, 4 * 2**i))
        self.storage = SyncSupabaseStorage(DATABASE_URL)
        self.reply(1)
        result = self.score(self.arrival(3600))
        self.assertEqual( result["score"], 9)
        self.assertEqual( result["response_delay"], 60)
        self.reply(3601)
        self.assertEqual( self.score(self.arrival(36000))["score"], 0)

    async def test_arrival_during_generation_and_failed_send(self) -> None :
        
        self.score(self.arrival(0))
        queued = self.arrival(1)
        self.reply(2)
        result = self.score(queued)
        self.assertEqual( result["increment"], 1)
        self.assertEqual( self.score(self.arrival(3))["increment"], 0)
        # A failed send produces no outbound row, so the window remains open.
        self.assertEqual( self.score(self.arrival(4))["increment"], 1)

    async def test_duplicate_concurrent_calls_and_atomic_history(self) -> None :
        
        self.score(self.arrival(0))
        row = self.arrival(1)
        results = await asyncio.gather(*(
            asyncio.to_thread( self.score, row)
            for _ in range(4)
        ))
        self.assertTrue(all( result == results[0] for result in results ))
        state = self.db.execute(
            "SELECT score FROM wa_agents.case_handler_contact_burst_scores "
            "WHERE contact = %s",
            ( self.contact, ),
        ).fetchone()[0]
        count = self.db.execute(
            "SELECT count(*) FROM wa_agents.case_handler_contact_burst_score_histories "
            "WHERE contact = %s",
            ( self.contact, ),
        ).fetchone()[0]
        self.assertEqual( state, results[0]["score"])
        self.assertEqual( count, 2)

    async def test_echo_silence_and_history_exclusions(self) -> None :
        
        self.score(self.arrival(0))
        self.assertIsNone(self.score(self.arrival(1, echo = True)))
        self.db.execute(
            "INSERT INTO wa_agents.case_handler_case_manifests "
            "(contact, case_index, silenced_until) "
            "VALUES (%s, 1, now() + interval '1 hour')",
            ( self.contact, ),
        )
        self.assertIsNone(self.score(self.arrival(2)))
        self.db.execute(
            "UPDATE wa_agents.case_handler_case_manifests "
            "SET silenced_until = now() - interval '1 second' WHERE contact = %s",
            ( self.contact, ),
        )
        self.assertEqual( self.score(self.arrival(3))["increment"], 0)
        chunk = self.storage.insert_history_chunk(
            phone_number = self.phone,
            payload = self.payload,
            history_index = 0,
            phase = 0,
            chunk_order = 0,
            progress = 100,
        )["id"]
        self.assertIsNone(self.score(self.arrival(4, history_chunk = chunk)))
        self.assertEqual( self.score(self.arrival(5, msg_type = "image"))["increment"], 1)
        self.assertEqual(
            self.score(self.arrival(6, msg_type = "interactive"))["increment"],
            1,
        )

    async def test_invalid_lease_cannot_write(self) -> None :
        
        row = self.arrival(0)
        owner = self.owner
        self.owner = uuid4()
        with self.assertRaises(RuntimeError) :
            self.score(row)
        self.owner = owner
        self.db.execute(
            "UPDATE wa_agents.case_handler_contact_leases "
            "SET heartbeat_at = now() - interval '2 minutes', "
            "expires_at = now() - interval '1 second' WHERE contact = %s",
            ( self.contact, ),
        )
        with self.assertRaises(RuntimeError) :
            self.score(row)
        count = self.db.execute(
            "SELECT count(*) FROM wa_agents.case_handler_contact_burst_score_histories "
            "WHERE contact = %s",
            ( self.contact, ),
        ).fetchone()[0]
        self.assertEqual( count, 0)

    async def test_contacts_are_independent_and_elapsed_never_negative(self) -> None :
        
        self.score(self.arrival(0))
        self.assertEqual( self.score(self.arrival(10))["score"], 1)
        result = self.score(self.arrival(5))
        self.assertEqual( result["score"], 2)
        self.assertEqual( result["decayed_points"], 0)
        self.assertEqual( result["received_at"], self.start + timedelta( seconds = 10))
        self.contact = self.storage.upsert_contact(
            business = self.phone,
            wa_id = "593999999997",
            user_id = None,
        )["id"]
        self.storage.acquire_contact_lease( self.contact, self.owner)
        self.assertEqual( self.score(self.arrival(10))["score"], 0)

    async def test_fixed_delay_and_large_base(self) -> None :
        
        self.score(self.arrival(0))
        for base, maximum in ( ( 4.0, 4.0 ), ( 1e308, 1.7e308 ) ) :
            result = await self.async_storage.update_contact_burst_score(
                queue_row_id = self.arrival(1),
                owner_token = self.owner,
                response_delay = base,
                max_response_delay = maximum,
                burst_decay_period = timedelta( days = 1),
            )
            self.assertEqual( result["response_delay"], maximum)


if __name__ == "__main__" :
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url")
    options, remaining = parser.parse_known_args()
    DATABASE_URL = options.database_url
    unittest.main( argv = [ __file__, *remaining ])
