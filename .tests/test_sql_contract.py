import re

from pathlib import Path


SQL_DIR = Path(__file__).parents[1] / "wa_agents" / "sql"
DDL     = ( SQL_DIR / "abc_DDL.sql").read_text(encoding = "utf-8")

QUERY_FILES = tuple(
    path
    for path in SQL_DIR.glob("*.sql")
    if path.name not in { "DDL.sql", "abc_DDL.sql", "nuclear_option.sql" }
)


def _normalized_sql( sql : str) -> str :
    return " ".join(sql.split())


def test_query_tables_exist_in_new_schema() -> None :
    declared_tables = set(
        re.findall(
            r"CREATE TABLE IF NOT EXISTS public\.([a-z_]+)",
            DDL,
        )
    )
    referenced_tables = {
        table
        for path in QUERY_FILES
        for table in re.findall(
            r"public\.([a-z_]+)",
            path.read_text(encoding = "utf-8"),
        )
    }
    
    assert referenced_tables <= declared_tables


def test_query_parameter_blocks_match_placeholders() -> None :
    for path in QUERY_FILES :
        sql          = path.read_text(encoding = "utf-8")
        placeholders = set(re.findall( r"@([A-Za-z_]\w*)", sql))
        declared     = set(re.findall( r"^  -- ([A-Za-z_]\w*)\s+:", sql, re.M))
        
        assert placeholders == declared, path.name


def test_query_scripts_do_not_reference_legacy_tables() -> None :
    legacy_tables = {
        "wa_cases",
        "wa_incoming_queue",
        "wa_messages",
        "wa_operators",
        "wa_users",
        "wa_webhook_messages",
        "wa_webhook_payloads",
        "wa_webhook_statuses",
    }
    query_sql = "\n".join(
        path.read_text(encoding = "utf-8")
        for path in QUERY_FILES
    )
    
    assert not ( legacy_tables & set(re.findall( r"\bwa_[a-z_]+\b", query_sql)) )


def test_flow_encryption_key_table_has_explicit_name() -> None :
    query_sql = "\n".join(
        path.read_text(encoding = "utf-8")
        for path in QUERY_FILES
    )

    assert "public.wa_api_flow_encryption_keys" in DDL
    assert "wa_api_flow_wabas" not in DDL + query_sql


def test_media_uses_object_key() -> None :
    insert_media = ( SQL_DIR / "insert_media.sql").read_text(encoding = "utf-8")
    
    assert "object_key" in DDL
    assert "object_key" in insert_media
    assert not re.search( r"\bprefix\b", DDL)


def test_raw_payload_audit_contract() -> None :
    insert_payload = ( SQL_DIR / "insert_inbound_payload.sql").read_text(
        encoding = "utf-8"
    )
    
    assert "data_raw      JSONB" in DDL
    assert "errors        JSONB" in DDL
    assert "wa_api_inbound_payloads_validation_errors_idx" in DDL
    assert "ALTER TABLE public.wa_api_inbound_payloads" in DDL
    assert "data_raw" in insert_payload
    assert "data_hash" in insert_payload
    assert ( SQL_DIR / "get_inbound_payload_by_hash.sql").is_file()


def test_payload_validation_result_has_explicit_updates() -> None :
    valid = ( SQL_DIR / "mark_inbound_payload_valid.sql").read_text(
        encoding = "utf-8"
    )
    invalid = ( SQL_DIR / "mark_inbound_payload_invalid.sql").read_text(
        encoding = "utf-8"
    )
    
    assert "validated = TRUE" in valid
    assert "errors    = NULL" in valid
    assert "validated = FALSE" in invalid
    assert "errors    = @errors" in invalid


def test_payload_metadata_updates_raw_payload() -> None :
    metadata = ( SQL_DIR / "update_inbound_payload_metadata.sql").read_text(
        encoding = "utf-8"
    )

    assert "payload_id" in metadata
    assert "payload_hash" not in metadata
    assert "public.wa_api_inbound_payloads" in metadata
    assert "item_index" in metadata
    assert "change_index" in metadata
    assert not re.search( r"\b(item_idx|change_idx)\b", DDL + metadata)


def test_blocking_flow_session_uses_message_receipt_interval() -> None :
    query = _normalized_sql(
        ( SQL_DIR / "get_blocking_flow_session.sql").read_text(encoding = "utf-8")
    )

    assert "message.id = @inbound_message" in query
    assert "session.contact = @contact" in query
    assert "session.created_at <= payload.received_at" in query
    assert "payload.received_at < CASE" in query
    assert "session.session_status = 'completed'" in query
    assert "session.completed_at" in query
    assert "session.session_status IN ( 'failed', 'superseded')" in query
    assert "session.updated_at" in query
    assert "ELSE session.expires_at" in query


def test_frequently_refreshed_rows_update_before_insert() -> None :
    business = _normalized_sql(
        ( SQL_DIR / "upsert_business.sql").read_text(encoding = "utf-8")
    )
    profile = _normalized_sql(
        ( SQL_DIR / "upsert_contact_profile.sql").read_text(encoding = "utf-8")
    )

    assert "INSERT INTO public.wa_api_businesses AS bus" in business
    assert "ON CONFLICT (waba_id)" in business
    assert "ON CONFLICT (display_phone_number)" in business
    assert "phone_number_id = EXCLUDED.phone_number_id" in business
    assert "last_seen_at = now()" in business

    assert profile.index("WITH updated AS ( UPDATE") < profile.index(
        "inserted AS ( INSERT"
    )
    assert "IS NOT DISTINCT FROM (@profile_username)" in profile
    assert "WHERE NOT EXISTS ( SELECT 1 FROM updated )" in profile
    assert "ON CONFLICT ON CONSTRAINT wa_api_contact_profile_unique" in profile


def test_contact_lease_defaults_to_ninety_seconds() -> None :
    acquire = ( SQL_DIR / "acquire_contact_lease.sql").read_text(
        encoding = "utf-8"
    )
    renew = ( SQL_DIR / "renew_contact_lease.sql").read_text(
        encoding = "utf-8"
    )
    
    assert "INTERVAL '90 seconds'" in DDL
    assert "INTERVAL '90 seconds'" in acquire
    assert "INTERVAL '90 seconds'" in renew
    assert "owner_token = @owner_token" in _normalized_sql(renew)


def test_queue_claim_acquires_contact_lease_atomically() -> None :
    claim = ( SQL_DIR / "claim_next_case_handler_message.sql").read_text(
        encoding = "utf-8"
    )
    normalized = _normalized_sql(claim)

    assert "INSERT INTO public.wa_case_handler_contact_leases" in normalized
    assert "FOR UPDATE OF que SKIP LOCKED" in normalized
    assert "@owner_token" in claim
    assert "COALESCE( rou.handler_key, @fallback_handler_key )" in claim
    assert "= ANY( @handler_keys )" in claim
    assert "msg_status = 'processing'" in normalized


def test_contact_queue_claim_reuses_active_lease_and_preserves_fifo() -> None :
    claim = (
        SQL_DIR / "claim_next_case_handler_message_for_contact.sql"
    ).read_text(encoding = "utf-8")
    normalized = _normalized_sql(claim)

    assert "lea.contact = @contact" in normalized
    assert "lea.owner_token = @owner_token" in normalized
    assert "lea.expires_at > now()" in normalized
    assert "que.id > @after_row_id" in normalized
    assert "ORDER BY que.created_at ASC, que.id ASC LIMIT 1" in normalized
    assert "FOR UPDATE OF que SKIP LOCKED" in normalized
    assert normalized.index("LIMIT 1") < normalized.index(
        "can.handler_key = ANY( @handler_keys)"
    )
    assert "heartbeat_at = now()" in normalized
    assert "expires_at = now() + INTERVAL '90 seconds'" in normalized
    assert "msg_status = 'processing'" in normalized


def test_queue_claim_returns_handler_silence_timeout() -> None :
    claim = ( SQL_DIR / "claim_next_case_handler_message.sql").read_text(
        encoding = "utf-8"
    )

    assert "CREATE TABLE IF NOT EXISTS public.wa_case_handler_settings" in DDL
    assert "handler_key             T_NO_WS_STR PRIMARY KEY" in DDL
    assert "silence_timeout_minutes INTEGER     NOT NULL" in DDL
    assert "CHECK ( silence_timeout_minutes > 0 )" in DDL
    assert "public.wa_case_handler_settings" in claim
    assert "settings.silence_timeout_minutes" in claim


def test_pending_echo_query_is_contact_scoped_and_newer_than_job() -> None :
    sql = ( SQL_DIR / "has_pending_message_echo.sql").read_text(
        encoding = "utf-8"
    )
    normalized = " ".join(sql.split())

    assert "msg.contact = @contact" in normalized
    assert "msg.is_echo" in normalized
    assert "que.id > @after_row_id" in normalized
    assert "que.msg_status = 'pending'" in normalized


def test_queue_route_is_resolved_and_persisted_at_enqueue() -> None :
    enqueue = ( SQL_DIR / "enqueue_case_handler_message.sql").read_text(
        encoding = "utf-8"
    )

    assert "wa_case_handler_routes" in DDL
    assert "handler_key   T_NO_WS_STR" in DDL
    assert "wa_case_handler_routes_phone_number_fkey" in DDL
    assert "FOREIGN KEY ( phone_number, contact)" in DDL
    assert "UNIQUE NULLS NOT DISTINCT ( phone_number, contact)" in DDL
    assert "handler_url   T_NO_WS_STR DEFAULT NULL" in DDL
    assert "ON DELETE SET NULL" in DDL
    assert "INSERT INTO public.wa_api_to_case_handler_queue ( msg_id, handler_id )" in (
        _normalized_sql(enqueue)
    )
    assert "( rte.contact IS NOT NULL ) DESC" in enqueue


def test_history_chunks_and_messages_preserve_import_context() -> None :
    chunk = ( SQL_DIR / "insert_history_chunk.sql").read_text(
        encoding = "utf-8"
    )
    message = ( SQL_DIR / "insert_inbound_message.sql").read_text(
        encoding = "utf-8"
    )
    normalized_message = _normalized_sql(message)

    assert "wa_api_embedded_signup_syncs" in chunk
    assert "ON CONFLICT ( phone_number, sync_type)" in chunk
    assert "ON CONFLICT ( payload, history_index)" in chunk
    assert "history_chunk     BIGINT" in DDL
    assert "history_context   JSONB" in DDL
    assert "history_chunk" in normalized_message
    assert "history_context" in normalized_message
    assert "msg.msg_type = 'media_placeholder'" in normalized_message
    assert "'errors'" in DDL.partition("CREATE TYPE T_WHATSAPP_STATUS")[0]


def test_smb_app_contacts_update_existing_contact_and_sync_rows() -> None :
    upsert = ( SQL_DIR / "upsert_smb_app_contact.sql").read_text(
        encoding = "utf-8"
    )
    complete = ( SQL_DIR / "complete_smb_app_contact_sync.sql").read_text(
        encoding = "utf-8"
    )

    assert "wa_api_smb_app_contacts" in DDL + upsert
    assert "ON CONFLICT (contact)" in upsert
    assert "smb.changed_at <= EXCLUDED.changed_at" in upsert
    assert "'smb_app_state_sync'" in upsert + complete
    assert ")::T_WHATSAPP_APP_DATA_SYNC_STATUS" in upsert
    assert "sync_status  = 'completed'" in complete


def test_case_manifests_are_bound_to_handler_id() -> None :
    insert_manifest = ( SQL_DIR / "insert_case_manifest.sql").read_text(
        encoding = "utf-8"
    )

    assert re.search( r"handler_id\s+BIGINT\s+DEFAULT NULL", DDL)
    assert "@handler_id" in insert_manifest


def test_case_and_message_indices_are_one_based_and_scoped() -> None :
    insert_manifest = ( SQL_DIR / "insert_case_manifest.sql").read_text(
        encoding = "utf-8"
    )
    insert_message = ( SQL_DIR / "insert_case_handler_message.sql").read_text(
        encoding = "utf-8"
    )
    get_messages = ( SQL_DIR / "get_case_handler_messages.sql").read_text(
        encoding = "utf-8"
    )

    assert "case_index         INT          NOT NULL" in DDL
    assert "next_message_index INT          NOT NULL DEFAULT 1" in DDL
    assert "message_index INT         NOT NULL" in DDL
    assert "UNIQUE ( contact, case_index)" in DDL
    assert "UNIQUE ( case_id, message_index)" in DDL
    assert "CHECK ( case_index >= 1 )" in DDL
    assert "CHECK ( next_message_index >= 1 )" in DDL
    assert "CHECK ( message_index >= 1 )" in DDL
    assert "COALESCE( max(cas.case_index) + 1, 1 )" in insert_manifest
    assert "next_message_index = next_message_index + 1" in insert_message
    assert "next_message_index - 1  AS message_index" in insert_message
    assert "msg.message_index ASC" in get_messages


def test_case_handler_message_can_map_to_multiple_api_messages() -> None :
    assert "wa_case_handler_to_api_case_handler_msg_id_unique" not in DDL
    assert "wa_case_handler_to_api_case_handler_msg_id_idx" in DDL
    assert "wa_case_handler_to_api_api_inbound_msg_id_unique" in DDL
    assert "wa_case_handler_to_api_api_outbound_msg_id_unique" in DDL


def test_case_message_insert_persists_machine_state_atomically() -> None :
    sql        = ( SQL_DIR / "insert_case_handler_message.sql").read_text(
        encoding = "utf-8"
    )
    normalized = _normalized_sql(sql)
    
    assert "INSERT INTO public.wa_case_handler_messages" in normalized
    assert "UPDATE public.wa_case_handler_case_manifests" in normalized
    assert "machine_state = @machine_state" in normalized
    assert "silenced_until = @silenced_until" in normalized


def test_case_manifest_round_trips_silence_deadline() -> None :
    manifest_queries = [
        ( SQL_DIR / name).read_text(encoding = "utf-8")
        for name in (
            "get_case_manifest.sql",
            "get_open_case_manifest.sql",
            "insert_case_manifest.sql",
            "update_case_manifest.sql",
        )
    ]

    assert "silenced_until     TIMESTAMPTZ  DEFAULT NULL" in DDL
    assert all( "silenced_until" in sql for sql in manifest_queries)


def test_case_handler_state_history_schema_contract() -> None :
    assert "CREATE TABLE IF NOT EXISTS public.wa_case_handler_state_histories" in DDL
    assert "case_manifest   BIGINT       NOT NULL" in DDL
    assert "case_message    BIGINT       DEFAULT NULL" in DDL
    assert "machine_state   T_NO_WS_STR  DEFAULT NULL" in DDL
    assert "wa_case_handler_state_histories_case_manifest_fkey" in DDL
    assert "wa_case_handler_state_histories_case_message_fkey" in DDL
    assert "wa_case_handler_state_histories_snapshot_unique" in DDL
    assert "UNIQUE NULLS NOT DISTINCT" in DDL
    assert "wa_case_handler_state_histories_case_timeline_idx" in DDL
    assert "ALTER TABLE public.wa_case_handler_state_histories" in DDL


def test_case_creation_persists_initial_state_history() -> None :
    sql = ( SQL_DIR / "insert_case_manifest.sql").read_text(encoding = "utf-8")
    normalized = _normalized_sql(sql)

    assert "INSERT INTO public.wa_case_handler_state_histories" in normalized
    assert "NULL::BIGINT" in normalized
    assert "manifest.machine_state" in normalized
    assert "initial_state AS" in normalized


def test_case_message_insert_persists_state_history() -> None :
    sql = ( SQL_DIR / "insert_case_handler_message.sql").read_text(
        encoding = "utf-8"
    )
    normalized = _normalized_sql(sql)

    assert "INSERT INTO public.wa_case_handler_state_histories" in normalized
    assert "message.id" in normalized
    assert "manifest.machine_state" in normalized
    assert "state_history AS" in normalized
    assert "msg.id = history.case_message" in normalized


def test_agent_contexts_are_append_only_generations() -> None :
    get_contexts = ( SQL_DIR / "get_agent_contexts.sql").read_text(
        encoding = "utf-8"
    )
    insert_manifest = ( SQL_DIR / "insert_case_manifest.sql").read_text(
        encoding = "utf-8"
    )
    insert_message = ( SQL_DIR / "insert_case_handler_message.sql").read_text(
        encoding = "utf-8"
    )

    assert "context_index INT" in DDL
    assert "case_message  BIGINT      DEFAULT NULL" in DDL
    assert "UNIQUE NULLS NOT DISTINCT" in DDL
    assert "max(ctx.context_index)" in get_contexts
    assert not re.search(
        r"\bagent_context\b",
        DDL + get_contexts + insert_manifest + insert_message,
    )
    assert "NULL::BIGINT" in insert_manifest
    assert "NULL::BIGINT" in insert_message
    assert "cleared_agent_contexts" in insert_message
    assert "extended_agent_contexts" in insert_message
    assert "DELETE FROM public.wa_case_handler_agent_contexts" not in insert_message
