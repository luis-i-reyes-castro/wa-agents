"""
Recovery and deployment regressions.
Run with --database-url pointing only at a disposable database built from abc_DDL.sql.
"""

import argparse
import asyncio
import importlib
import os
import psycopg
import sys
import threading
import unittest

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from sofia_utils.psycopg import (
    close_async_database_connection_pool,
    close_sync_database_connection_pool,
)
from wa_agents.api_listener import WhatsAppAPIListener
from wa_agents.api_server import WhatsAppAPIServer
from wa_agents.api_worker import WhatsAppAPIWorker
from wa_agents.case_handler_base import AsyncWhatsAppCaseHandler, WhatsAppCaseHandler
from wa_agents.database_queue import AsyncWhatsAppDatabaseQueue, WhatsAppDatabaseQueue
from wa_agents.supabase import (
    SyncSupabaseStorage,
    async_pooled_connection,
)


DATABASE_URL = None
APPS = ( { "app" : "test", "id" : "123", "secret" : "test-secret" }, )


class AsyncHandler (AsyncWhatsAppCaseHandler) :
    HANDLER_KEY = "ecs-test"
    responses = 0
    fail_ingestion = False
    fail_response = False

    async def process_message( self, message, media) :
        stored = await self.dedup_and_ingest_message( message, media)
        if self.fail_ingestion :
            raise RuntimeError("Interrupted ingestion")
        return bool(stored)

    async def run_while_in_action(self) :
        if self.fail_response :
            raise RuntimeError("Failed response")
        type(self).responses += 1
        return False


class SyncHandler (WhatsAppCaseHandler) :
    HANDLER_KEY = "ecs-test"
    responses = 0

    def process_message( self, message, media) :
        return bool(self.dedup_and_ingest_message( message, media))

    def run_while_in_action(self) :
        type(self).responses += 1
        return False


class RuntimeDatabaseTests (unittest.IsolatedAsyncioTestCase) :

    def setUp(self) -> None :
        if not DATABASE_URL :
            self.skipTest("Pass --database-url for a disposable database")
        self.db = psycopg.connect( DATABASE_URL, autocommit = True)
        self.addCleanup(self.db.close)
        self.addCleanup(close_sync_database_connection_pool)
        self.storage = SyncSupabaseStorage(DATABASE_URL)
        self.number = str(uuid4().int)
        phone = self.storage.upsert_business(
            waba_id = self.number,
            phone_number_id = self.number,
            display_phone_number = self.number,
        )
        self.storage.activate_phone_number(self.number)
        self.phone = phone["id"]
        self.addCleanup(
            self.db.execute,
            "DELETE FROM wa_agents.api_businesses WHERE waba_id = %s",
            ( self.number, ),
        )
        self.db.execute(
            "INSERT INTO wa_agents.case_handler_configurations "
            "(handler_key, case_stale_hours, response_delay_seconds, "
            "max_response_delay_seconds, burst_decay_minutes) "
            "VALUES ('ecs-test', 48, 1, 2, 60) ON CONFLICT DO NOTHING"
        )
        self.listener_queue = AsyncWhatsAppDatabaseQueue(DATABASE_URL)
        self.worker_queue = AsyncWhatsAppDatabaseQueue(DATABASE_URL)
        self.worker = WhatsAppAPIWorker(
            handler_cls = AsyncHandler, queue = self.worker_queue,
        )
        AsyncHandler.responses = 0
        AsyncHandler.fail_ingestion = False
        AsyncHandler.fail_response = False
        SyncHandler.responses = 0

    async def asyncTearDown(self) -> None :
        await close_async_database_connection_pool()

    def payload(self) -> dict :
        return {
            "object" : "whatsapp_business_account",
            "entry" : [ {
                "id" : self.number,
                "changes" : [ {
                    "field" : "messages",
                    "value" : {
                        "messaging_product" : "whatsapp",
                        "metadata" : {
                            "phone_number_id" : self.number,
                            "display_phone_number" : self.number,
                        },
                        "messages" : [ {
                            "from" : "593999999998",
                            "id" : "wamid." + uuid4().hex,
                            "timestamp" : str(int(datetime.now(UTC).timestamp())),
                            "type" : "text",
                            "text" : { "body" : "hello" },
                        } ],
                    },
                } ],
            } ],
        }

    def rows(self) :
        return self.db.execute(
            "SELECT q.id, q.msg_status, q.ingested_at, q.response_due_at, q.attempts "
            "FROM wa_agents.api_to_case_handler_queue q "
            "JOIN wa_agents.api_inbound_messages m ON m.msg_id = q.msg_id "
            "JOIN wa_agents.api_contacts c ON c.id = m.contact "
            "WHERE c.phone_number = %s ORDER BY q.id",
            ( self.phone, ),
        ).fetchall()

    def abandon(self) :
        self.db.execute(
            "UPDATE wa_agents.case_handler_contact_leases "
            "SET heartbeat_at = now() - interval '100 seconds', "
            "expires_at = now() - interval '1 second' "
            "WHERE contact IN (SELECT id FROM wa_agents.api_contacts "
            "WHERE phone_number = %s)", ( self.phone, ),
        )

    async def test_concurrent_duplicates_and_partial_normalization_retry(self) :
        payload = self.payload()
        original = self.listener_queue._enqueue_message
        self.listener_queue._enqueue_message = AsyncMock(side_effect=RuntimeError("DB failure"))
        with self.assertRaises(RuntimeError) :
            await self.listener_queue.enqueue(payload)
        self.assertEqual( self.rows(), [])
        self.listener_queue._enqueue_message = original
        results = await asyncio.gather(
            self.listener_queue.enqueue(payload), self.listener_queue.enqueue(payload),
        )
        self.assertEqual( sum(r["enqueued"] for r in results), 1)
        self.assertEqual( len(self.rows()), 1)
        self.assertEqual( self.rows()[0][1], "pending")
        # Listener has no fallback, worker does: this must still be claimable.
        claimed = await self.worker_queue.claim_next([ "ecs-test" ])
        self.assertIsNotNone(claimed)

    async def test_retry_repairs_preexisting_partial_ingestion(self) :
        payload = self.payload()
        raw = self.storage.insert_inbound_payload(payload)
        contact = self.storage.upsert_contact(
            business = self.phone, wa_id = "593999999998", user_id = None,
        )
        message = payload["entry"][0]["changes"][0]["value"]["messages"][0]
        self.storage.insert_inbound_message(
            contact = contact["id"], payload = raw["id"], msg_id = message["id"],
            msg_ts = datetime.now(UTC), msg_type = "text",
            msg_data = { "text" : message["text"] },
        )
        result = await self.listener_queue.enqueue(payload)
        self.assertFalse(result["stored"])
        self.assertTrue(result["enqueued"])
        self.assertEqual( len(self.rows()), 1)

    async def test_sync_listener_transaction_retry(self) :
        queue = WhatsAppDatabaseQueue(DATABASE_URL)
        payload = self.payload()
        with patch.object( queue, "_enqueue_message", side_effect = RuntimeError("failed")) :
            with self.assertRaises(RuntimeError) :
                await asyncio.to_thread( queue.enqueue, payload)
        self.assertEqual( self.rows(), [])
        self.assertTrue((await asyncio.to_thread( queue.enqueue, payload))["enqueued"])

    async def test_late_route_and_unrelated_worker_do_not_discard(self) :
        await self.listener_queue.enqueue(self.payload())
        self.assertIsNone(await self.listener_queue.claim_next([ "other-handler" ]))
        self.assertEqual( self.rows()[0][1], "pending")
        self.db.execute(
            "INSERT INTO wa_agents.case_handler_routes_by_phone_number "
            "(phone_number, handler_config) SELECT %s, id "
            "FROM wa_agents.case_handler_configurations WHERE handler_key = 'ecs-test'",
            ( self.phone, ),
        )
        self.assertIsNotNone(await self.listener_queue.claim_next([ "ecs-test" ]))

    async def test_two_workers_race_for_one_contact(self) :
        await self.listener_queue.enqueue(self.payload())
        await self.listener_queue.enqueue(self.payload())
        other = AsyncWhatsAppDatabaseQueue( DATABASE_URL, fallback_handler_key = "ecs-test")
        claims = await asyncio.gather(
            self.worker_queue.claim_next([ "ecs-test" ]),
            other.claim_next([ "ecs-test" ]),
        )
        self.assertEqual( sum(item is not None for item in claims), 1)
        self.assertEqual( [ row[1] for row in self.rows() ], [ "processing", "pending" ])

    async def test_abandoned_claim_exclusive_owner_and_fencing(self) :
        await self.listener_queue.enqueue(self.payload())
        first = await self.worker_queue.claim_next([ "ecs-test" ])
        self.assertIsNone(await self.worker_queue.claim_next([ "ecs-test" ]))
        self.abandon()
        recovered = await self.worker_queue.claim_next([ "ecs-test" ])
        self.assertEqual( first["row_id"], recovered["row_id"])
        self.assertNotEqual( first["owner_token"], recovered["owner_token"])
        with self.assertRaisesRegex( RuntimeError, "lease lost") :
            await self.worker_queue.mark_done(
                first["row_id"], owner_token = first["owner_token"],
            )
        self.assertFalse(await self.worker_queue.mark_error(
            first["row_id"], owner_token = first["owner_token"],
        ))

    async def test_ingestion_rollback_and_recovery(self) :
        await self.listener_queue.enqueue(self.payload())
        item = await self.worker_queue.claim_next([ "ecs-test" ])
        AsyncHandler.fail_ingestion = True
        with self.assertLogs( level = "ERROR") :
            await self.worker._process_claimed_message(item)
        self.assertIsNone(self.rows()[0][2])
        self.assertFalse(self.storage.case_handler_message_exists(item["id"]))
        self.assertIsNone(await self.worker_queue.claim_next([ "ecs-test" ]))
        self.db.execute(
            "UPDATE wa_agents.api_to_case_handler_queue SET available_at = now() "
            "WHERE id = %s", ( item["row_id"], ),
        )
        AsyncHandler.fail_ingestion = False
        recovered = await self.worker_queue.claim_next([ "ecs-test" ])
        await self.worker._process_claimed_message(recovered)
        self.assertIsNotNone(self.rows()[0][2])
        self.assertIsNotNone(self.rows()[0][3])
        self.assertTrue(self.storage.case_handler_message_exists(item["id"]))

    async def test_delayed_reply_survives_worker_replacement(self) :
        await self.listener_queue.enqueue(self.payload())
        await self.worker._process_message()
        self.assertIsNotNone(self.rows()[0][3])
        self.abandon()
        replacement = WhatsAppAPIWorker(
            handler_cls = AsyncHandler,
            queue = AsyncWhatsAppDatabaseQueue(DATABASE_URL),
        )
        await replacement._process_message()
        self.assertEqual( len(replacement._job_td), 1)
        await replacement._process_jobs(replacement._job_td.get_all())
        self.assertEqual( AsyncHandler.responses, 1)
        self.assertIsNone(self.rows()[0][3])
        self.assertIsNone(await replacement.queue.claim_next([ "ecs-test" ]))

    async def test_sync_ingestion_and_restart(self) :
        await self.listener_queue.enqueue(self.payload())
        worker = WhatsAppAPIWorker( handler_cls = SyncHandler, queue = self.worker_queue)
        await worker._process_message()
        self.assertIsNotNone(self.rows()[0][3])
        self.abandon()
        replacement = WhatsAppAPIWorker(
            handler_cls = SyncHandler, queue = AsyncWhatsAppDatabaseQueue(DATABASE_URL),
        )
        await replacement._process_message()
        await replacement._process_jobs(replacement._job_td.get_all())
        self.assertEqual( SyncHandler.responses, 1)
        self.assertIsNone(self.rows()[0][3])

    async def test_response_failure_has_bounded_retries(self) :
        await self.listener_queue.enqueue(self.payload())
        await self.worker._process_message()
        AsyncHandler.fail_response = True
        for attempt in range(1, 6) :
            with self.assertLogs( level = "ERROR") :
                await self.worker._process_jobs(self.worker._job_td.get_all())
            row = self.rows()[0]
            self.assertEqual( row[1], "error")
            self.assertEqual( row[4], attempt)
            self.assertIsNotNone(row[3])
            self.assertIsNone(await self.worker_queue.claim_next([ "ecs-test" ]))
            self.db.execute(
                "UPDATE wa_agents.api_to_case_handler_queue SET available_at = now() "
                "WHERE id = %s", ( row[0], ),
            )
            if attempt < 5 :
                await self.worker._process_message()
        self.assertIsNone(await self.worker_queue.claim_next([ "ecs-test" ]))

    async def test_configured_response_retry_limit(self) :
        self.db.execute(
            "INSERT INTO wa_agents.api_worker_configurations "
            "(server_name, poll_interval_busy, poll_interval_idle, max_retries) "
            "VALUES (%s, 1, 1, 2)", ( self.number, ),
        )
        self.addCleanup(
            self.db.execute,
            "DELETE FROM wa_agents.api_worker_configurations WHERE server_name = %s",
            ( self.number, ),
        )
        worker = WhatsAppAPIWorker(
            configuration = self.number,
            handler_cls = AsyncHandler,
            queue = self.worker_queue,
        )
        self.assertEqual( worker.queue.max_retries, 2)
        with self.assertRaises(psycopg.errors.CheckViolation) :
            self.db.execute(
                "UPDATE wa_agents.api_worker_configurations SET max_retries = -1 "
                "WHERE server_name = %s", ( self.number, ),
            )
        await self.listener_queue.enqueue(self.payload())
        await worker._process_message()
        AsyncHandler.fail_response = True
        for attempt in range(1, 4) :
            with self.assertLogs( level = "ERROR") :
                await worker._process_jobs(worker._job_td.get_all())
            row = self.rows()[0]
            self.assertEqual( row[4], attempt)
            self.db.execute(
                "UPDATE wa_agents.api_to_case_handler_queue SET available_at = now() "
                "WHERE id = %s", ( row[0], ),
            )
            if attempt < 3 :
                self.assertTrue(await worker._process_message())
        self.assertFalse(await worker._process_message())

    async def test_zero_retries_skips_exhausted_older_messages(self) :
        self.worker_queue.max_retries = 0
        for _ in range(4) :
            await self.listener_queue.enqueue(self.payload())
        first = await self.worker_queue.claim_next([ "ecs-test" ])
        owner = first["owner_token"]
        await self.worker_queue.mark_done( first["row_id"], owner_token = owner)
        second = await self.worker_queue.claim_next_for_contact(
            first["contact"], owner, first["row_id"], [ "ecs-test" ],
        )
        await self.worker_queue.mark_error( second["row_id"], owner_token = owner)
        third = await self.worker_queue.claim_next_for_contact(
            first["contact"], owner, first["row_id"], [ "ecs-test" ],
        )
        self.assertEqual( third["row_id"], self.rows()[2][0])
        await self.worker_queue.mark_error( third["row_id"], owner_token = owner)
        await self.worker_queue.release_contact_lease( first["contact"], owner)
        fourth = await self.worker_queue.claim_next([ "ecs-test" ])
        self.assertEqual( fourth["row_id"], self.rows()[3][0])
        await self.worker_queue.mark_error(
            fourth["row_id"], owner_token = fourth["owner_token"],
        )
        await self.worker_queue.release_contact_lease(
            fourth["contact"], fourth["owner_token"],
        )
        self.db.execute(
            "UPDATE wa_agents.api_to_case_handler_queue SET available_at = now() "
            "WHERE id = ANY(%s)", ( [ row[0] for row in self.rows() ], ),
        )
        self.assertIsNone(await self.worker_queue.claim_next([ "ecs-test" ]))
        # Raising the budget also makes exhausted work eligible again.
        queue = WhatsAppDatabaseQueue( DATABASE_URL, fallback_handler_key = "ecs-test")
        queue.max_retries = 1
        retry = queue.claim_next([ "ecs-test" ])
        self.assertEqual( retry["row_id"], second["row_id"])
        queue.mark_done( retry["row_id"], owner_token = retry["owner_token"])
        next_retry = queue.claim_next_for_contact(
            retry["contact"], retry["owner_token"], retry["row_id"], [ "ecs-test" ],
        )
        self.assertEqual( next_retry["row_id"], third["row_id"])
        self.db.execute(
            "UPDATE wa_agents.api_to_case_handler_queue SET attempts = 2048 "
            "WHERE id = %s", ( next_retry["row_id"], ),
        )
        queue.mark_error( next_retry["row_id"], owner_token = retry["owner_token"])
        delay = self.db.execute(
            "SELECT extract(epoch FROM available_at - clock_timestamp()) "
            "FROM wa_agents.api_to_case_handler_queue WHERE id = %s",
            ( next_retry["row_id"], ),
        ).fetchone()[0]
        self.assertGreater( delay, 295)
        self.assertLessEqual( delay, 300)

    async def test_new_message_replaces_durable_deadline(self) :
        await self.listener_queue.enqueue(self.payload())
        await self.worker._process_message()
        await self.listener_queue.enqueue(self.payload())
        await self.worker._process_pending_job_messages()
        rows = self.rows()
        self.assertEqual( len(rows), 2)
        self.assertIsNone(rows[0][3])
        self.assertIsNotNone(rows[1][3])

    async def test_cancelled_response_is_recovered(self) :
        await self.listener_queue.enqueue(self.payload())
        await self.worker._process_message()
        started = asyncio.Event()

        async def slow_response(handler) :
            started.set()
            await asyncio.sleep(60)

        with patch.object( AsyncHandler, "run_while_in_action", slow_response) :
            task = asyncio.create_task(
                self.worker._process_jobs(self.worker._job_td.get_all())
            )
            await asyncio.wait_for( started.wait(), 2)
            task.cancel()
            await asyncio.gather( task, return_exceptions = True)
        self.assertIsNotNone(self.rows()[0][3])
        replacement = WhatsAppAPIWorker(
            handler_cls = AsyncHandler, queue = AsyncWhatsAppDatabaseQueue(DATABASE_URL),
        )
        await replacement._process_message()
        await replacement._process_jobs(replacement._job_td.get_all())
        self.assertEqual( AsyncHandler.responses, 1)
        self.assertIsNone(self.rows()[0][3])

    async def test_shutdown_rolls_back_interrupted_ingestion(self) :
        await self.listener_queue.enqueue(self.payload())
        started = asyncio.Event()
        original = AsyncHandler.process_message

        async def slow_ingestion(handler, message, media) :
            await original( handler, message, media)
            started.set()
            await asyncio.sleep(60)

        self.worker.SHUTDOWN_TIMEOUT_SECONDS = 0.01
        with patch.object( AsyncHandler, "process_message", slow_ingestion) :
            async with self.worker.worker_lifespan(self.worker) :
                await asyncio.wait_for( started.wait(), 2)
        self.assertIsNone(self.rows()[0][2])
        replacement = WhatsAppAPIWorker(
            handler_cls = AsyncHandler, queue = AsyncWhatsAppDatabaseQueue(DATABASE_URL),
        )
        await replacement._process_message()
        self.assertIsNotNone(self.rows()[0][2])
        self.assertIsNotNone(self.rows()[0][3])

    async def test_child_tasks_do_not_share_transaction_connection(self) :
        async with async_pooled_connection(DATABASE_URL) as parent :
            async def child() :
                async with async_pooled_connection(DATABASE_URL) as conn :
                    self.assertIsNot( conn, parent)
            await asyncio.create_task(child())
            async with async_pooled_connection(DATABASE_URL) as nested :
                self.assertIs( nested, parent)


class RuntimeUnitTests (unittest.IsolatedAsyncioTestCase) :

    async def test_listener_and_worker_health(self) :
        queue = SimpleNamespace(
            storage = SimpleNamespace(),
            _fetch_one = AsyncMock( return_value = { "healthy" : 1 }),
        )
        listener = WhatsAppAPIListener( apps = APPS, queue = queue)
        self.assertEqual( (await listener.healthz()).status_code, 200)
        queue._fetch_one.side_effect = RuntimeError("Database unavailable")
        self.assertEqual( (await listener.healthz()).status_code, 503)
        queue._fetch_one.side_effect = None
        worker = WhatsAppAPIWorker( handler_cls = AsyncHandler, queue = queue)
        self.assertEqual( (await worker.worker_healthz()).status_code, 503)
        worker.worker_task = asyncio.create_task(asyncio.sleep(60))
        worker.heartbeat_task = asyncio.create_task(asyncio.sleep(60))
        self.assertEqual( (await worker.worker_healthz()).status_code, 200)
        worker.worker_task.cancel()
        await asyncio.gather( worker.worker_task, return_exceptions = True)
        self.assertEqual( (await worker.worker_healthz()).status_code, 503)
        worker.heartbeat_task.cancel()
        await asyncio.gather( worker.heartbeat_task, return_exceptions = True)
        server = WhatsAppAPIServer( apps = APPS, handler_cls = AsyncHandler, queue = queue)
        route = next(r for r in server.routes if r.path == "/healthz")
        self.assertEqual( (await route.endpoint()).status_code, 503)

    async def test_heartbeat_continues_during_slow_handler(self) :
        renew = AsyncMock( return_value = True)
        queue = SimpleNamespace( storage = SimpleNamespace(renew_contact_lease=renew))
        worker = WhatsAppAPIWorker( handler_cls = AsyncHandler, queue = queue)
        worker.LEASE_HEARTBEAT_SECONDS = 0.005
        worker._leases[1] = uuid4()
        heartbeat = asyncio.create_task(worker._renew_leases())
        await worker._call_handler_method( asyncio.sleep, 0.03)
        heartbeat.cancel()
        await asyncio.gather( heartbeat, return_exceptions = True)
        self.assertGreaterEqual( renew.await_count, 3)

    async def test_sync_timeout_retains_thread_until_completion(self) :
        queue = SimpleNamespace( storage = SimpleNamespace())
        worker = WhatsAppAPIWorker( handler_cls = AsyncHandler, queue = queue)
        worker.HANDLER_TIMEOUT_SECONDS = 0.001
        finished = threading.Event()
        call = asyncio.create_task(worker._call_handler_method( finished.wait, 1))
        try :
            await asyncio.sleep(0.03)
            self.assertTrue(worker._lease_error)
            self.assertTrue(worker._stop_flag)
            self.assertFalse(call.done())
        finally :
            finished.set()
            await call

    def test_s3_role_credentials_and_no_acl(self) :
        variables = {
            "BUCKET_NAME" : "test-bucket", "BUCKET_REGION" : "us-east-1",
            "BUCKET_KEY_ID" : "", "BUCKET_KEY_SECRET" : "", "BUCKET_ENDPOINT" : "",
        }
        with patch.dict( os.environ, variables), patch("boto3.client") as client :
            sys.modules.pop( "wa_agents.S3_bucket_io", None)
            module = importlib.import_module("wa_agents.S3_bucket_io")
            args = client.call_args.kwargs
            self.assertNotIn( "aws_access_key_id", args)
            self.assertNotIn( "aws_secret_access_key", args)
            self.assertIsNone(args["endpoint_url"])
            module.b3_put_media( "test", b"hello", "text/plain")
            self.assertNotIn( "ACL", client.return_value.put_object.call_args.kwargs)
        sys.modules.pop( "wa_agents.S3_bucket_io", None)


if __name__ == "__main__" :
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url")
    options, remaining = parser.parse_known_args()
    DATABASE_URL = options.database_url
    unittest.main( argv = [ __file__, *remaining ])
