"""
Signup persistence on a database containing only the wa_agents schema.
Use --database-url only with a disposable PostgreSQL database built from abc_DDL.sql.
"""

import argparse
import unittest

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from wa_agents import supabase as storage


DATABASE_URL = None


class SignupStorageTests (unittest.IsolatedAsyncioTestCase) :

    async def asyncSetUp(self) -> None :
        
        if not DATABASE_URL :
            self.skipTest("Pass --database-url for a disposable database")
        self.conn = await psycopg.AsyncConnection.connect(
            DATABASE_URL, row_factory = dict_row,
        )
        self.addAsyncCleanup(self.conn.close)
        self.addAsyncCleanup(self.conn.rollback)
        self.number = str(uuid4().int)
        self.state = uuid4().hex
        self.session = await storage.async_create_signup_session(
            self.conn, 101, str(uuid4()), self.state,
            datetime.now(UTC) + timedelta( minutes = 15),
        )

    async def test_signup_assets_and_routing(self) -> None :
        
        self.assertIsNone(await storage.async_get_signup_session(
            self.conn, 202, self.state,
        ))
        waba = await storage.async_upsert_waba(
            self.conn, self.number, "connected", True, "test-token", None,
            secret_key = "test-key",
        )
        phone = await storage.async_upsert_phone_number(
            self.conn, waba["id"], self.number, self.number, "Test", "CLOUD_API",
            Jsonb({ "is_on_biz_app" : True }),
        )
        self.assertTrue(phone["is_on_biz_app"])
        self.assertIsNone(await storage.async_upsert_phone_number(
            self.conn, waba["id"], self.number, "different-number", None, None, None,
        ))
        await storage.async_update_signup_session_success(
            self.conn, self.session["id"], self.number, self.number,
            Jsonb({ "ready" : True }),
            { "type" : "WA_EMBEDDED_SIGNUP", "event" : "FINISH", "version" : 3 },
        )
        session = await storage.async_get_signup_session( self.conn, 101, self.state)
        self.assertEqual( session["status"], "completed")
        self.assertEqual( session["setup_payload"], { "ready" : True })
        self.assertEqual( session["waba_id"], self.number)
        wabas = await storage.async_list_wabas( self.conn, 101)
        self.assertIn( waba["id"], [ row["id"] for row in wabas ])
        token = await storage.async_get_waba_token(
            self.conn, 101, self.number, secret_key = "test-key",
        )
        self.assertEqual( token["business_token"], "test-token")
        self.assertIsNone(await storage.async_get_waba_token(
            self.conn, 202, self.number, secret_key = "test-key",
        ))
        self.assertFalse(await storage.async_list_wabas( self.conn, 202))
        operator = await storage.async_get_wa_operator( self.conn, self.number)
        self.assertEqual( operator["waba_id"], self.number)
        operators = await storage.async_list_wa_operators(self.conn)
        self.assertIn( self.number, [ row["operator_id"] for row in operators ])
        self.assertIsNone(await storage.async_assign_case_handler_config(
            self.conn, self.number, "missing-handler",
        ))
        await self.conn.execute(
            "INSERT INTO wa_agents.case_handler_configurations "
            "(handler_key, case_stale_hours, response_delay_seconds, "
            "max_response_delay_seconds, burst_decay_minutes) "
            "VALUES (%s, 48, 1, 2, 60)", ( self.state, ),
        )
        route = await storage.async_assign_case_handler_config(
            self.conn, self.number, self.state,
        )
        self.assertIsNotNone(route)
        self.assertEqual( route, await storage.async_assign_case_handler_config(
            self.conn, self.number, self.state,
        ))
        sync = await storage.async_reserve_app_data_sync(
            self.conn, phone["id"], "smb_app_state_sync",
        )
        self.assertIsNone(await storage.async_reserve_app_data_sync(
            self.conn, phone["id"], "smb_app_state_sync",
        ))
        await storage.async_accept_app_data_sync( self.conn, sync["id"], "request-1")
        await storage.async_fail_app_data_sync(
            self.conn, sync["id"], Jsonb({ "error" : "too late" }),
        )
        history = await storage.async_reserve_app_data_sync(
            self.conn, phone["id"], "history",
        )
        await storage.async_fail_app_data_sync(
            self.conn, history["id"], Jsonb({ "error" : "test failure" }),
        )
        cur = await self.conn.execute(
            "SELECT sync_type, sync_status FROM wa_agents.api_embedded_signup_syncs "
            "WHERE phone_number = %s", ( phone["id"], ),
        )
        statuses = { row["sync_type"] : row["sync_status"] for row in await cur.fetchall() }
        self.assertEqual( statuses, { "smb_app_state_sync" : "accepted", "history" : "failed" })
        await self.conn.rollback()
        cur = await self.conn.execute(
            "SELECT id FROM wa_agents.api_businesses WHERE waba_id = %s",
            ( self.number, ),
        )
        self.assertIsNone(await cur.fetchone())

    async def test_failed_signup_and_caller_rollback(self) -> None :
        
        await storage.async_update_signup_session_failure(
            self.conn, self.session["id"], Jsonb({ "step" : "oauth" }), "test failure",
        )
        session = await storage.async_get_signup_session( self.conn, 101, self.state)
        self.assertEqual( session["status"], "failed")
        self.assertEqual( session["error_message"], "test failure")
        self.assertEqual( session["setup_payload"], { "step" : "oauth" })
        async with await psycopg.AsyncConnection.connect(DATABASE_URL) as observer :
            cur = await observer.execute(
                "SELECT id FROM wa_agents.api_embedded_signup_sessions "
                "WHERE session_state = %s", ( self.state, ),
            )
            self.assertIsNone(await cur.fetchone())
        await self.conn.rollback()
        self.assertIsNone(await storage.async_get_signup_session(
            self.conn, 101, self.state,
        ))


if __name__ == "__main__" :
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url")
    options, remaining = parser.parse_known_args()
    DATABASE_URL = options.database_url
    unittest.main( argv = [ __file__, *remaining ])
