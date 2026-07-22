# wa-agents Development History

## 2026-07-22: Supabase Storage and Single-App FastAPI Runtime

### Motivation

`wa-agents` originally stored WhatsApp/caseflow text data in S3-compatible object
storage and ran as a two-process Flask listener plus queue worker. That worked,
but text-heavy access patterns were slow and operationally awkward compared with
Postgres-backed storage and a single FastAPI process.

The migration moved durable text/audit/queue state to Supabase Postgres while
keeping S3-compatible bucket storage for media bytes.

### Storage Changes

- Added Supabase-backed caseflow storage for `Message` subclasses:
  - `wa_users`
  - `wa_cases`
  - `wa_messages`
- Kept S3-compatible storage for media payload bytes.
- Added durable WhatsApp webhook audit storage:
  - `wa_webhook_payloads`
  - `wa_webhook_messages`
  - `wa_webhook_statuses`
- Added full-payload webhook deduplication by canonical JSON hash.
- Kept Meta message/status IDs non-unique in v1; dedup is by full payload hash.
- Added row-level security enabling statements for all `public.wa_*` tables so
  Supabase does not warn about public tables without RLS.

### Queue Changes

- Replaced the local SQLite queue with Supabase table `wa_incoming_queue`.
- Added sync and async `QueueDB` helpers for:
  - enqueue
  - claim next pending payload
  - mark done
  - mark error
- Status-only webhook payloads are now captured at listener/server time and no
  longer depend on handler processing.

### Runtime Changes

- Added `wa_agents.WhatsAppAPIServer`, a `FastAPI` subclass that:
  - registers WhatsApp verification and webhook routes,
  - persists webhook audit rows,
  - enqueues validated payloads,
  - starts `AsyncQueueWorker` from FastAPI lifespan,
  - shuts the worker down cleanly on app shutdown.
- This supports a single-container app model, e.g. `uvicorn app:app`, instead of
  a separate listener container and queue-worker container.
- Added `docs/Dockerfile.fastapi` as an example single-process deployment.

### Shared Database Helper Changes

- Moved direct psycopg JSONB usage behind `sofia_utils.psycopg.Jsonb`.
- Reused `sofia_utils.psycopg` connection-pool helpers from applications.
- Disabled psycopg automatic prepared statements in `sofia_utils.psycopg` with
  `prepare_threshold = None` for Supabase pooler compatibility.

The prepared-statement issue showed up during queue polling because the worker
repeats the same small set of SQL statements quickly. Supabase's pooler can make
psycopg's client-side prepared-statement cache disagree with server-side
prepared-statement state, causing errors such as:

- `DuplicatePreparedStatement`
- `InvalidSqlStatementName`

Disabling automatic prepared statements keeps parameterized queries while
avoiding that pooler-specific failure mode.

### Tests and Utilities

- Moved tests out of the package into root-level `tests/`.
- Kept the bucket I/O script as `tests/manual_do_bucket_io.py`, explicitly not a
  pytest module.
- Added `scripts/check_supabase_tables.py` to verify that all required `wa_*`
  Supabase tables exist from a supplied `.env` file.

### Application Testing Notes

- `ieced` was refactored to instantiate `WhatsAppAPIServer` from `app_abc.py`
  with `webhook_path = "/whatsapp"` while keeping autocomplete endpoints.
- Live testing confirmed:
  - webhook payload audit rows are inserted,
  - queue rows are claimed and marked done,
  - caseflow messages are stored in Supabase,
  - S3-compatible media storage still works,
  - WhatsApp replies and status callbacks are processed.
