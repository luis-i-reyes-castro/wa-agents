---
name: maintain-wa-agents
description: Maintain and refactor the wa-agents Python package across webhook ingestion, Meta payload models, PostgreSQL persistence, queue claiming and contact leases, case-handler/FSM state, and S3 media storage. Use when reviewing or changing files under wa_agents/, its SQL schema and query scripts, its case-handler examples, or consumer integration in repositories such as sofia-server and da-assistant; also use when diagnosing payload-validation, queue-worker, persistence, or media-path failures caused by this package.
---

# Maintain wa-agents

Keep changes small and trace each behavior vertically from the external payload or
consumer call through Python, SQL, and storage. Treat current source files as
canonical; use the bundled architecture notes for invariants and navigation, not as
a substitute for reading affected code.

## Establish Context

1. Read `~/vscode/.cursor/rules.mdc` and the language rules relevant to the task.
2. Read `README.md` and check `git status --short --branch` before editing.
3. Inspect staged changes first when present. Preserve all user work and leave agent
   edits unstaged.
4. Read both the caller and callee around every changed interface. For SQL, always
   inspect the query file, its `supabase.py` method, and the DDL definition together.
5. Read [references/architecture.md](references/architecture.md) before changing
   persistence, queueing, case handling, or media storage.

## Route the Task

- Use `whatsapp_models.py` for Meta-controlled webhook and Graph API structures.
  Keep application database records out of that module.
- Use `abc_DDL.sql`, the focused scripts in `wa_agents/sql/`, and `supabase.py` for
  structured persistence.
- Use `queue_db.py` for payload audit, validation, normalization, enqueueing, and
  atomic claims. Use `queue_worker.py` for worker lifecycle and handler dispatch.
- Use `case_handler_models.py` for stored conversation models and
  `case_handler_base.py` for contact-bound context, FSM state, deduplication, sending,
  and media orchestration.
- Use `S3_bucket_storage.py` and `S3_bucket_io.py` only for media bytes and object
  operations. Store media metadata in PostgreSQL.
- Inspect consumer construction and overrides when changing a public signature.
  Search at least `~/sofia-server` and relevant local bots for `from wa_agents` and
  direct imports of the changed symbol.

## Implement Coherently

1. Confirm the observed payload or failing row before relaxing validation. Prefer a
   real sanitized example over a guessed Meta shape.
2. Preserve raw webhook auditing before Pydantic validation. Do not make audit rows
   depend on successfully resolving a business or contact.
3. Keep synchronous and asynchronous adapters behaviorally symmetric unless the
   task explicitly requires a difference.
4. Keep SQL parameter names, returned aliases, Python dictionary keys, and database
   record models synchronized.
5. Follow foreign-key precedence in multi-table reads: business, contact, payload,
   then message, followed by dependent tables.
6. Preserve per-contact exclusion through expiring leases; do not replace it with
   process-local locks or directory locks.
7. Import `get_qualname` from `sofia_utils.printing` as `here`. Prefix raised or
   logged diagnostic errors with `f"In {here()}: ..."` and call `here()` directly.
8. Avoid compatibility layers when the user explicitly authorizes a clean schema or
   public-API break. Otherwise identify consumer impact before changing behavior.

## Verify Proportionally

Read [references/verification.md](references/verification.md) before validating a
persistence or queue change.

At minimum:

1. Run `python -m compileall -q wa_agents`.
2. Search for stale table, query, method, and constructor names across this repo and
   known consumers.
3. Exercise both sync and async code paths when changing mirrored adapters.
4. Build `abc_DDL.sql` in a disposable PostgreSQL instance when changing schema or
   SQL, then execute the affected scripts through their real Python adapter.
5. Test a valid payload and an invalid payload for ingestion changes. Confirm the
   invalid raw payload remains auditable.
6. Recheck `git status` and report staged versus unstaged changes. Ignore warnings
   caused solely by the repository's intentional whitespace-only blank lines.

For deployed SOFIA database or dotenv diagnosis, use
`~/sofia-server/skills/inspect-sofia-runtime/` when available. Keep that inspection
read-only; return here for authorized package or schema changes.
