#!/usr/bin/env python3
"""
Check that the wa-agents Supabase tables exist.
"""

from __future__ import annotations

import argparse
import os
import sys

from pathlib import Path
from urllib.parse import (
    urlsplit,
    urlunsplit,
)

from sofia_utils.psycopg import (
    close_sync_database_connection_pool,
    sync_pooled_conection,
)


REQUIRED_TABLES = (
    "wa_users",
    "wa_cases",
    "wa_messages",
    "wa_webhook_payloads",
    "wa_webhook_messages",
    "wa_webhook_statuses",
    "wa_incoming_queue",
)


def _strip_env_value( value : str) -> str :
    
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"') :
        return value[1:-1]
    
    return value


def load_env_file( filepath : Path) -> None :
    """
    Load simple KEY=VALUE lines from a .env file into os.environ.
    """
    if not filepath.exists() :
        raise FileNotFoundError(f"Env file not found: {filepath}")
    
    for raw_line in filepath.read_text( encoding = "utf-8").splitlines() :
        line = raw_line.strip()
        if not line or line.startswith("#") :
            continue
        
        if line.startswith("export ") :
            line = line.removeprefix("export ").strip()
        
        if "=" not in line :
            continue
        
        key, value = line.split( "=", 1)
        key        = key.strip()
        if not key :
            continue
        
        os.environ[key] = _strip_env_value(value)
    
    return


def get_database_url() -> str :
    """
    Return the configured Supabase database URL, preferring IPv4.
    """
    if ( database_url := os.getenv("SUPABASE_DB_CONNECTION_URL_IPv4") ) \
    or ( database_url := os.getenv("SUPABASE_DB_CONNECTION_URL_IPv6") ) :
        return database_url
    
    raise RuntimeError(
        "Missing SUPABASE_DB_CONNECTION_URL_IPv4/SUPABASE_DB_CONNECTION_URL_IPv6"
    )


def mask_database_url( database_url : str) -> str :
    
    parsed = urlsplit(database_url)
    netloc = parsed.netloc
    if "@" in netloc :
        credentials, host = netloc.rsplit( "@", 1)
        if ":" in credentials :
            user, _password = credentials.split( ":", 1)
            netloc = f"{user}:***@{host}"
        else :
            netloc = f"***@{host}"
    
    return urlunsplit(
        (
            parsed.scheme,
            netloc,
            parsed.path,
            parsed.query,
            parsed.fragment,
        )
    )


def fetch_existing_tables(
    database_url : str,
    schema       : str,
) -> set[str] :
    """
    Return the required table names that exist in the target schema.
    """
    with sync_pooled_conection(database_url) as conn :
        rows = conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %(schema)s
              AND table_type = 'BASE TABLE'
              AND table_name = ANY(%(table_names)s)
            ORDER BY table_name
            """,
            {
                "schema"      : schema,
                "table_names" : list(REQUIRED_TABLES),
            },
        ).fetchall()
    
    return { row["table_name"] for row in rows }


def main() -> int :
    
    parser = argparse.ArgumentParser(
        description = "Check that the wa-agents Supabase tables exist.",
    )
    parser.add_argument(
        "env_file",
        type = Path,
        help = "Path to a .env file with Supabase DB connection URL vars.",
    )
    parser.add_argument(
        "--schema",
        default = "public",
        help    = "Database schema to inspect. Defaults to public.",
    )
    args = parser.parse_args()
    
    load_env_file(args.env_file.expanduser().resolve())
    database_url = get_database_url()
    
    print(f"Checking schema: {args.schema}")
    print(f"Database URL: {mask_database_url(database_url)}")
    
    try :
        existing = fetch_existing_tables(database_url, args.schema)
    finally :
        close_sync_database_connection_pool()
    
    missing = [ table for table in REQUIRED_TABLES if table not in existing ]
    
    for table in REQUIRED_TABLES :
        marker = "OK" if table in existing else "MISSING"
        print(f"{marker:7} {args.schema}.{table}")
    
    if missing :
        print(
            "\nMissing tables: " + ", ".join(missing),
            file = sys.stderr,
        )
        return 1
    
    print("\nAll wa-agents Supabase tables exist.")
    return 0


if __name__ == "__main__" :
    raise SystemExit(main())
