"""
Registration isolation integration tests against a disposable PostgreSQL container.
Requires the sibling sofia-server checkout for its shared-schema migration.
"""

import asyncio
import shutil
import subprocess

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from sofia_utils.psycopg import (
    close_async_database_connection_pool,
    close_sync_database_connection_pool,
    load_sql_script,
)
from wa_agents.supabase import (
    AsyncSupabaseStorage,
    SyncSupabaseStorage,
)

ROOT = Path(__file__).parents[1]
SOFIA = ROOT.parent / "sofia-server"
MIGRATION = SOFIA / "backend/sql/.migrations/20261030_phone_registrations.sql"


@pytest.fixture( scope = "module")
def postgres_url() :
    if not shutil.which("docker") :
        pytest.skip("Docker is required")
    name = "wa-registration-test-" + uuid4().hex[:10]
    subprocess.run(
        [ "docker", "run", "--name", name, "-e", "POSTGRES_PASSWORD=review",
          "-p", "127.0.0.1::5432", "-d", "postgres:17-alpine" ],
        check = True, capture_output = True,
    )
    try :
        import time
        for _ in range(50) :
            ready = subprocess.run(
                [ "docker", "exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres" ],
                capture_output = True,
            )
            if ready.returncode == 0 :
                break
            time.sleep(0.1)
        port = subprocess.check_output(
            [ "docker", "port", name, "5432" ], text = True,
        ).strip().rsplit( ":", 1)[1]
        yield f"postgresql://postgres:review@127.0.0.1:{port}/postgres"
    finally :
        close_sync_database_connection_pool()
        subprocess.run([ "docker", "rm", "-f", name ], check = True, capture_output = True)


@pytest.fixture
def database( postgres_url) :
    close_sync_database_connection_pool()
    with psycopg.connect( postgres_url, autocommit = True) as conn :
        conn.execute("DROP SCHEMA IF EXISTS wa_agents CASCADE")
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")
        conn.execute(( ROOT / "wa_agents/sql/abc_DDL.sql" ).read_text())
        conn.execute("CREATE TABLE public.business (id BIGINT PRIMARY KEY)")
        conn.execute(( SOFIA / "backend/sql/abc_DDL/whatsapp_coexistence.sql" ).read_text())
    yield postgres_url
    close_sync_database_connection_pool()


def inbound( conn, contact, phone_id, waba_id, display = "300", tag = "old") :
    raw = { "entry" : [ { "id" : waba_id, "changes" : [ {
        "field" : "messages", "value" : { "metadata" : {
            "phone_number_id" : phone_id, "display_phone_number" : display,
        } },
    } ] } ] }
    payload = conn.execute(
        """INSERT INTO wa_agents.api_inbound_payloads
        (contact,data_raw,data_hash) VALUES (%s,%s,%s) RETURNING id""",
        ( contact, Jsonb(raw), uuid4().hex),
    ).fetchone()["id"]
    msg_id = "wamid." + tag
    conn.execute(
        """INSERT INTO wa_agents.api_inbound_messages
        (contact,payload,msg_id,msg_ts,msg_type,msg_data)
        VALUES (%s,%s,%s,now(),'text','{}')""",
        ( contact, payload, msg_id),
    )
    return msg_id


def test_sync_identity_and_send_guards( database) :
    storage = SyncSupabaseStorage(database)
    old = storage.upsert_business( "100", "200", "300")
    with pytest.raises( RuntimeError, match = "pending") :
        storage.get_business_token("200")
    assert storage.activate_phone_number("200")
    assert storage.get_business_token("200") is None
    old_contact = storage.upsert_contact( old["id"], "400", None)
    new = storage.upsert_business( "101", "201", "300")
    new_contact = storage.upsert_contact( new["id"], "400", None)
    assert old["id"] != new["id"]
    assert old_contact["id"] != new_contact["id"]
    assert storage.activate_phone_number("201")
    assert storage.upsert_business( "100", "200", "300")["id"] == old["id"]
    assert storage.upsert_business( "101", "200", "300") is None
    assert storage.upsert_business( "100", "200", "301") is None
    assert storage.activate_phone_number("200") is None
    with pytest.raises( RuntimeError, match = "retired") :
        storage.get_business_token("200")


def test_async_identity_and_send_guards( database) :
    async def run() :
        storage = AsyncSupabaseStorage(database)
        try :
            old = await storage.upsert_business( "100", "200", "300")
            assert old
            with pytest.raises( RuntimeError, match = "pending") :
                await storage.get_business_token("200")
            assert await storage.activate_phone_number("200")
            assert await storage.get_business_token("200") is None
            await storage.upsert_business( "101", "201", "300")
            assert await storage.activate_phone_number("201")
            with pytest.raises( RuntimeError, match = "retired") :
                await storage.get_business_token("200")
        finally :
            await close_async_database_connection_pool()
    asyncio.run(run())


def test_retirement_closes_cases_and_stops_queue( database) :
    storage = SyncSupabaseStorage(database)
    old = storage.upsert_business( "100", "200", "300")
    storage.activate_phone_number("200")
    contact = storage.upsert_contact( old["id"], "400", None)["id"]
    with psycopg.connect( database, row_factory = dict_row) as conn :
        msg_id = inbound( conn, contact, "200", "100")
        conn.execute(
            """INSERT INTO wa_agents.case_handler_case_manifests
            (contact,case_index) VALUES (%s,1)""", (contact,),
        )
        enqueue = load_sql_script(ROOT / "wa_agents/sql/enqueue_case_handler_message.sql")
        assert conn.execute( enqueue, { "msg_id" : msg_id }).fetchone()
    storage.upsert_business( "101", "201", "300")
    assert storage.activate_phone_number("201")
    with psycopg.connect( database, row_factory = dict_row) as conn :
        assert not conn.execute(
            "SELECT is_open FROM wa_agents.case_handler_case_manifests"
        ).fetchone()["is_open"]
        assert conn.execute(
            "SELECT msg_status FROM wa_agents.api_to_case_handler_queue"
        ).fetchone()["msg_status"] == "error"
        late = inbound( conn, contact, "200", "100", tag = "late")
        assert conn.execute( enqueue, { "msg_id" : late }).fetchone() is None
        acquire = load_sql_script(ROOT / "wa_agents/sql/acquire_contact_lease.sql")
        assert conn.execute(
            acquire, { "contact" : contact, "owner_token" : uuid4() },
        ).fetchone() is None


def test_activation_waits_for_inflight_handler( database) :
    storage = SyncSupabaseStorage(database)
    old = storage.upsert_business( "100", "200", "300")
    storage.activate_phone_number("200")
    contact = storage.upsert_contact( old["id"], "400", None)["id"]
    with psycopg.connect(database) as conn :
        conn.execute(
            """INSERT INTO wa_agents.case_handler_contact_leases (contact,owner_token)
            VALUES (%s,%s)""", ( contact, uuid4()),
        )
    storage.upsert_business( "101", "201", "300")
    assert storage.activate_phone_number("201") is None
    assert storage.get_business_token("200") is None


def test_concurrent_activation_keeps_one_active( database) :
    storage = SyncSupabaseStorage(database)
    storage.upsert_business( "100", "200", "300")
    storage.upsert_business( "101", "201", "300")
    def activate(phone_id) :
        return SyncSupabaseStorage(database).activate_phone_number(phone_id)
    with ThreadPoolExecutor( max_workers = 2) as pool :
        results = list(pool.map( activate, [ "200", "201" ]))
    assert any(results)
    with psycopg.connect(database) as conn :
        assert conn.execute(
            "SELECT count(*) FROM wa_agents.api_phone_numbers WHERE registration_status='active'"
        ).fetchone()[0] == 1


def legacy_fixture( database, mixed = False) :
    with psycopg.connect( database, row_factory = dict_row) as conn :
        conn.execute("ALTER TABLE wa_agents.api_phone_numbers DROP COLUMN registration_status")
        conn.execute("ALTER TABLE wa_agents.api_phone_numbers ADD CONSTRAINT api_phone_numbers_display_phone_number_unique UNIQUE (display_phone_number)")
        conn.execute("INSERT INTO wa_agents.api_businesses (id,waba_id) OVERRIDING SYSTEM VALUE VALUES (1,'100'),(2,'101')")
        conn.execute("SELECT setval(pg_get_serial_sequence('wa_agents.api_businesses','id'),2)")
        conn.execute("""INSERT INTO wa_agents.api_phone_numbers
            (id,business,display_phone_number,phone_number_id) OVERRIDING SYSTEM VALUE VALUES (1,2,'300','201')""")
        conn.execute("SELECT setval(pg_get_serial_sequence('wa_agents.api_phone_numbers','id'),1)")
        contact = conn.execute("""INSERT INTO wa_agents.api_contacts
            (phone_number,wa_id) VALUES (1,'400') RETURNING id""").fetchone()["id"]
        inbound( conn, contact, "200", "100")
        if mixed :
            inbound( conn, contact, "201", "101", tag = "new")
        conn.execute("""INSERT INTO wa_agents.case_handler_case_manifests
            (contact,case_index) VALUES (%s,1)""", (contact,))
        return contact


def test_migration_repairs_history_and_is_rerunnable( database) :
    if not MIGRATION.exists() :
        pytest.skip("The applied registration migration file is no longer present")
    contact = legacy_fixture(database)
    with psycopg.connect( database, autocommit = True, row_factory = dict_row) as conn :
        conn.execute(MIGRATION.read_text())
        current = conn.execute("""SELECT num.phone_number_id,num.registration_status,cas.is_open
            FROM wa_agents.api_contacts con JOIN wa_agents.api_phone_numbers num
            ON num.id=con.phone_number JOIN wa_agents.case_handler_case_manifests cas
            ON cas.contact=con.id WHERE con.id=%s""", (contact,)).fetchone()
        assert current == {
            "phone_number_id" : "200", "registration_status" : "retired", "is_open" : False,
        }
        conn.execute(MIGRATION.read_text())
        assert conn.execute("SELECT count(*) AS n FROM wa_agents.api_phone_numbers").fetchone()["n"] == 2


def test_migration_rolls_back_ambiguous_history( database) :
    if not MIGRATION.exists() :
        pytest.skip("The applied registration migration file is no longer present")
    legacy_fixture( database, mixed = True)
    with psycopg.connect( database, autocommit = True) as conn :
        with pytest.raises( psycopg.errors.RaiseException, match = "contact spans") :
            conn.execute(MIGRATION.read_text())
        conn.execute("ROLLBACK")
        assert conn.execute("SELECT count(*) FROM wa_agents.api_phone_numbers").fetchone()[0] == 1
        assert conn.execute("""SELECT count(*) FROM information_schema.columns
            WHERE table_schema='wa_agents' AND table_name='api_phone_numbers'
            AND column_name='registration_status'""").fetchone()[0] == 0
