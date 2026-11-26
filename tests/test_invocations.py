"""
Invocation isolation regressions. Pass --database-url for a disposable DDL database.
"""

import argparse
import asyncio
import psycopg
import unittest

from uuid import uuid4

from sofia_utils.psycopg import (
    close_async_database_connection_pool,
    close_sync_database_connection_pool,
)
from wa_agents.case_handler_base import (
    AsyncCaseHandlerBase,
    CaseHandlerBase,
    CaseHandlerState,
)
from wa_agents.case_handler_models import (
    AssistantMsg,
    HumanUserContentMsg,
    OpenRouterCompletionUsage,
)
from wa_agents.supabase import (
    AsyncSupabaseStorage,
    SyncSupabaseStorage,
    async_pooled_connection,
)


DATABASE_URL = None


class SyncHandler (CaseHandlerBase) :
    
    HANDLER_KEY = "test-invocation"
    AGENT_NAMES = ( "main", )
    
    def process_message( self, message) -> bool :
        
        return False
    
    def run_while_in_action(self) -> bool :
        
        return False


class StatefulHandler (SyncHandler) :
    
    @classmethod
    def define_state_machine_config(cls) :
        
        return (
            [ CaseHandlerState("initial"), CaseHandlerState("done") ],
            "initial",
            [ { "trigger" : "finish", "source" : "initial", "dest" : "done" } ],
        )
    
    def apply_message_to_state_machine( self, message) -> None :
        
        self.trigger("finish")


class AsyncHandler (AsyncCaseHandlerBase) :
    
    HANDLER_KEY = "test-invocation"
    AGENT_NAMES = ( "main", )
    
    async def process_message( self, message) -> bool :
        
        return False
    
    async def run_while_in_action(self) -> bool :
        
        return False


class InvocationTests (unittest.IsolatedAsyncioTestCase) :
    
    def setUp(self) -> None :
        
        if not DATABASE_URL :
            self.skipTest("Pass --database-url for a disposable database")
        self.db = psycopg.connect( DATABASE_URL, autocommit = True)
        self.addCleanup(self.db.close)
        self.addCleanup(close_sync_database_connection_pool)
        self.storage = SyncSupabaseStorage(DATABASE_URL)
        self.identity = f"invocation-{uuid4()}"
        row = self.storage.upsert_business(
            self.identity, self.identity, self.identity,
        )
        contact = self.storage.upsert_contact( row["id"], None, "shared-backend")
        self.business, self.contact = self.storage.resolve_business_and_contact(
            row["id"], contact["id"],
        )
        self.addCleanup(
            self.db.execute,
            "DELETE FROM wa_agents.api_businesses WHERE waba_id = %s",
            ( self.identity, ),
        )
        self.addCleanup(
            self.db.execute,
            "DELETE FROM wa_agents.case_handler_invocations WHERE id IN "
            "(SELECT invocation FROM wa_agents.case_handler_case_manifests "
            "WHERE contact = %s)",
            ( self.contact.row_id, ),
        )
    
    async def asyncTearDown(self) -> None :
        
        await close_async_database_connection_pool()
    
    def handler( self, cls = SyncHandler, **kwargs) :
        
        return cls(
            self.business,
            self.contact,
            database_url  = DATABASE_URL,
            headless      = True,
            hydrate_media = False,
            **kwargs,
        )
    
    def test_sync_lifecycle_and_ownership(self) -> None :
        
        handler = self.handler()
        handler.case_open_new()
        original = HumanUserContentMsg( text = "first request")
        handler.agent_context_append( "main", original)
        stored = handler.apply_and_persist_message(original)
        manifest = handler.case_manifest
        self.assertIsNotNone(manifest.invocation)
        self.assertEqual( manifest.case_index, 1)
        self.assertEqual( manifest.contact, self.contact.row_id)
        self.assertEqual( handler.case_decide()[0], manifest.id)
        handler.context_build()
        self.assertEqual( [ m.id for m in handler.agent_contexts["main"] ], [ stored.id ])
        self.assertEqual(
            self.db.execute(
                "SELECT count(*) FROM wa_agents.case_handler_state_histories "
                "WHERE case_manifest = %s", ( manifest.id, ),
            ).fetchone()[0],
            2,
        )
        self.assertIsNone(
            self.storage.get_case_manifest( manifest.id, self.contact.row_id)
        )
        for contact, invocation in [
            ( self.contact.row_id + 10000, manifest.invocation ),
            ( self.contact.row_id, manifest.invocation + 10000 ),
        ] :
            self.assertIsNone(self.storage.get_case_manifest(
                manifest.id, contact, invocation = invocation,
            ))
        wrong = manifest.model_copy( update = { "invocation" : None })
        self.assertIsNone(self.storage.update_case_manifest(wrong))
        self.assertIsNone(self.storage.get_open_case_manifest(self.contact.row_id))
        for method in [
            handler.acquire_contact_lease,
            handler.renew_contact_lease,
            handler.release_contact_lease,
        ] :
            with self.assertRaises(ValueError) :
                method()
        with self.assertRaises(RuntimeError) :
            handler.case_open_new()
        handler.case_mark_as_resolved()
        saved = self.storage.get_case_manifest(
            manifest.id, self.contact.row_id, invocation = manifest.invocation,
        )
        self.assertFalse(saved.is_open)
        with self.assertRaises(RuntimeError) :
            handler.apply_and_persist_message(HumanUserContentMsg( text = "too late"))
    
    async def test_async_concurrent_cases_and_contexts(self) -> None :
        
        handlers = [ self.handler(AsyncHandler) for _ in range(12) ]
        await asyncio.gather(*( h.case_open_new() for h in handlers ))
        self.assertEqual( len({ h.case_id for h in handlers }), 12)
        self.assertEqual( len({ h.case_manifest.invocation for h in handlers }), 12)
        
        async def persist( index, handler) :
            
            message = HumanUserContentMsg( text = str(index))
            handler.agent_context_append( "main", message)
            stored = await handler.apply_and_persist_message(message)
            self.assertEqual( stored.message_index, 1)
            await handler.context_build()
            self.assertEqual( handler.agent_contexts["main"][0].text, str(index))
            await handler.case_mark_as_resolved()
            saved = await handler.storage.get_case_manifest(
                handler.case_id,
                self.contact.row_id,
                invocation = handler.case_manifest.invocation,
            )
            self.assertFalse(saved.is_open)
            with self.assertRaises(RuntimeError) :
                await handler.apply_and_persist_message(message)
            with self.assertRaises(RuntimeError) :
                await handler.case_open_new()
            with self.assertRaises(ValueError) :
                await handler.acquire_contact_lease()
        
        await asyncio.gather(*( persist( i, h) for i, h in enumerate(handlers) ))
        self.assertEqual(
            self.db.execute(
                "SELECT count(*) FROM wa_agents.api_contacts WHERE phone_number = %s",
                ( self.business.row_id, ),
            ).fetchone()[0],
            1,
        )
        for table in [
            "case_handler_contact_leases", "case_handler_routes_by_contact",
        ] :
            self.assertEqual(
                self.db.execute(
                    f"SELECT count(*) FROM wa_agents.{table} WHERE contact = %s",
                    ( self.contact.row_id, ),
                ).fetchone()[0],
                0,
            )
        self.assertEqual(
            self.db.execute(
                "SELECT count(*) FROM wa_agents.case_handler_to_api_bridge b "
                "JOIN wa_agents.case_handler_messages m ON m.id = b.case_handler_msg_id "
                "JOIN wa_agents.case_handler_case_manifests c ON c.id = m.case_id "
                "WHERE c.contact = %s", ( self.contact.row_id, ),
            ).fetchone()[0],
            0,
        )
    
    def test_contact_conversation_coexists_and_keeps_numbering(self) -> None :
        
        first = self.storage.insert_case_manifest( self.contact.row_id, None)
        self.handler().case_open_new()
        self.handler().case_open_new()
        self.assertIsNone(self.storage.insert_case_manifest( self.contact.row_id, None))
        self.assertEqual(
            self.storage.get_open_case_manifest(self.contact.row_id).id, first.id,
        )
        first.is_open = False
        self.storage.update_case_manifest(first)
        second = self.storage.insert_case_manifest( self.contact.row_id, None)
        self.assertEqual( second.case_index, 2)
        self.assertIsNone(second.invocation)
    
    def test_constraints_and_initialization_rollback(self) -> None :
        
        before = self.db.execute(
            "SELECT count(*) FROM wa_agents.case_handler_invocations"
        ).fetchone()[0]
        with self.assertRaises(psycopg.errors.ForeignKeyViolation) :
            self.storage.insert_case_manifest(
                self.contact.row_id + 10000,
                None,
                invocation_handler_key = "test-invocation",
            )
        with self.assertRaises(psycopg.errors.CheckViolation) :
            self.storage.insert_case_manifest(
                self.contact.row_id,
                None,
                [ "" ],
                invocation_handler_key = "test-invocation",
            )
        self.assertEqual(
            self.db.execute(
                "SELECT count(*) FROM wa_agents.case_handler_invocations"
            ).fetchone()[0],
            before,
        )
        handler = self.handler()
        handler.case_open_new()
        for assignment in [
            "contact = NULL", "case_index = 2", "silenced_until = now()",
        ] :
            with self.assertRaises(psycopg.IntegrityError) :
                self.db.execute(
                    f"UPDATE wa_agents.case_handler_case_manifests SET {assignment} "
                    "WHERE id = %s", ( handler.case_id, ),
                )
        with self.assertRaises(psycopg.errors.UniqueViolation) :
            self.db.execute(
                "INSERT INTO wa_agents.case_handler_case_manifests "
                "(contact, invocation, case_index) VALUES (%s, %s, 1)",
                ( self.contact.row_id, handler.case_manifest.invocation ),
            )
        with self.assertRaises(ValueError) :
            self.handler( route_by_contact = 123)
    
    def test_fsm_and_usage_share_the_invocation_case(self) -> None :
        
        handler = self.handler(StatefulHandler)
        handler.init_machine()
        message = handler.apply_and_persist_message(AssistantMsg(
            text  = "response",
            model = "test/model",
            usage = OpenRouterCompletionUsage(
                prompt_tokens = 2, completion_tokens = 3, total_tokens = 5, cost = 0.01,
            ),
        ))
        states = self.db.execute(
            "SELECT machine_state FROM wa_agents.case_handler_state_histories "
            "WHERE case_manifest = %s ORDER BY id", ( handler.case_id, ),
        ).fetchall()
        self.assertEqual( states, [ ( "initial", ), ( "done", ) ])
        handler.state = "initial"
        handler.context_build()
        self.assertEqual( handler.state, "done")
        self.assertEqual(
            self.db.execute(
                "SELECT total_tokens FROM wa_agents.case_handler_openrouter_usage "
                "WHERE case_message = %s", ( message.id, ),
            ).fetchone()[0],
            5,
        )
    
    async def test_shared_identity_creation_is_concurrent_safe(self) -> None :
        
        storage = AsyncSupabaseStorage(DATABASE_URL)
        identity = f"new-{self.identity}"
        self.addCleanup(
            self.db.execute,
            "DELETE FROM wa_agents.api_businesses WHERE waba_id = %s", ( identity, ),
        )
        
        async def build_identity() :
            
            async with async_pooled_connection(DATABASE_URL) :
                business = await storage.upsert_business( identity, identity, identity)
                return await storage.upsert_contact( business["id"], None, "backend")
        
        rows = await asyncio.gather(*( build_identity() for _ in range(12) ))
        self.assertEqual( len({ row["id"] for row in rows }), 1)
        self.assertEqual(
            self.db.execute(
                "SELECT registration_status FROM wa_agents.api_phone_numbers "
                "WHERE phone_number_id = %s", ( identity, ),
            ).fetchone()[0],
            "pending",
        )


if __name__ == "__main__" :
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url")
    args, remaining = parser.parse_known_args()
    DATABASE_URL = args.database_url
    unittest.main( argv = [ __file__, *remaining ])
