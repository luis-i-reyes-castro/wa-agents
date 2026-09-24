# Verification Recipes

Select checks according to the changed boundary. Do not use live Supabase data for
destructive schema testing.

## Python and Reference Checks

```bash
python -m compileall -q wa_agents
rg -n "OLD_TABLE|old_method|OldModel" . ~/sofia-server ~/da-assistant
git diff --check
git diff --cached --check
git status --short --branch
```

Interpret trailing-whitespace output using the repository rule: do not trim
whitespace-only blank lines. Fix non-blank edited lines only.

## Disposable PostgreSQL Check

Use PostgreSQL 17 unless the deployment declares another version. Choose a unique
container name and an automatically assigned loopback port; inspect exact targets
before cleanup.

```bash
docker run --name wa-agents-review-pg \
  -e POSTGRES_PASSWORD=review \
  -e POSTGRES_DB=wa_agents_review \
  -p 127.0.0.1::5432 \
  -d postgres:17-alpine

docker exec wa-agents-review-pg \
  pg_isready -U postgres -d wa_agents_review

docker exec -i wa-agents-review-pg \
  psql -v ON_ERROR_STOP=1 -U postgres -d wa_agents_review \
  < wa_agents/sql/abc_DDL.sql
```

Obtain the assigned port with `docker port wa-agents-review-pg 5432/tcp`. Exercise
SQL through `SyncSupabaseStorage`, `AsyncSupabaseStorage`, `QueueDB`, or
`AsyncQueueDB`, supplying the disposable connection URL explicitly. This validates
the placeholder conversion and returned row aliases in addition to SQL syntax.

Remove only the named disposable container after testing:

```bash
docker rm -f wa-agents-review-pg
```

## Ingestion Matrix

For payload or schema changes, cover:

| Path | Confirm |
| --- | --- |
| Valid text payload | Raw row, normalized message, pending queue row |
| Duplicate valid payload | Same payload/message identities; no duplicate queue work |
| Invalid payload | Raw row retained, `contact` null when unresolved, errors recorded |
| Sync claim | Queue status becomes processing and lease token is returned |
| Async claim | Same semantics and result keys as sync |
| Completion/error | Queue terminal status updates correctly |
| Lease release | Only the owning token releases the contact |

Add observed media/status payloads when their models or persistence change. Use
Meta-shaped message IDs, MIME values, timestamps, and SHA-256 encodings; synthetic
fixtures that violate those contracts test the fixture rather than the target path.

## S3 Checks

Test `media_object_key()` locally for the exact key shape and invalid path components.
Mock S3 I/O unless the user explicitly asks for a live bucket test. For a live test,
use a disposable object key and report cleanup.

## Consumer Checks

After changing handler construction or public models:

1. Compile affected consumer modules.
2. Search every `from wa_agents` import and constructor call.
3. Test the consumer's `process_message()` and `run_while_in_action()` boundary with
   the same sync/async semantics used in production.
4. For SOFIA runtime diagnosis, use `inspect-sofia-runtime` and keep queries bounded
   and read-only.
