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


def test_payload_metadata_references_raw_payload() -> None :
    metadata = ( SQL_DIR / "insert_inbound_payload_metadata.sql").read_text(
        encoding = "utf-8"
    )
    nuclear = ( SQL_DIR / "nuclear_option.sql").read_text(encoding = "utf-8")
    
    assert "payload_id" in metadata
    assert "payload_hash" not in metadata
    assert "public.wa_api_inbound_payloads" in nuclear


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
    assert "msg_status = 'processing'" in normalized


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
