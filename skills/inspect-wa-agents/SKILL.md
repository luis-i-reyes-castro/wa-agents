---
name: inspect-wa-agents
description: Inspect the wa-agents Python package as read-only scaffolding for consumer applications, trace its public handler, queue, persistence, FSM, context, and media contracts, diagnose persisted WhatsApp case and Flow state, and inspect display-name decisions and rejection reasons. Use when developing or debugging a repository that depends on wa-agents without modifying wa-agents itself, including Sofia case handlers, persisted case, agent-context, or Flow failures, and WhatsApp Business display-name reviews.
---

# Inspect wa-agents

Treat `~/wa-agents` source and schema as read-only scaffolding.
Read the current implementation to understand its contracts, then keep authorized changes in the consumer repository. If the task requires a package or package-schema change, switch
to `../maintain-wa-agents/SKILL.md` before editing.
Never print database URLs, access tokens, full raw payloads, WABA IDs, or phone numbers.

## Establish Context

1. Read `~/vscode/AGENTS.md`, the consumer repository's `README.md`, and `~/wa-agents/README.md`.
2. Check `git status --short --branch` in every repository being inspected. Preserve user work and leave agent edits unstaged.
3. Identify the consumer, target environment, handler key, business display phone number, contact WhatsApp ID, and approximate event time as applicable.
4. Read both the consumer call site and the current `wa-agents` implementation of every interface it uses. Inspect `wa_agents/sql/abc_DDL.sql` with persistence code.
5. For deployed Sofia inspection, read `~/sofia-server/skills/inspect-sofia-runtime/SKILL.md` and use its bounded, redacted tools.

## Use the Package as Scaffolding

- Derive behavior from current public classes and models instead of copying package internals into the consumer.
- Trace handler behavior through registration, queue dispatch, case/FSM persistence, agent-context reconstruction, media storage, and outbound delivery as needed.
- Keep consumer overrides consistent with the package's sync or async base class, constructor contract, persisted message models, and handler lifecycle.
- Search `~/wa-agents` for a symbol before assuming a method, model, table, or state transition exists.
- Do not edit `~/wa-agents`, its SQL schema, or production data during inspection.

## Diagnose Persisted Case State

Run the bundled read-only query with the reported chatbot number as `business_display_phone_number` and the sender number as `contact_wa_id`:
```bash
psql "$DATABASE_URL" \
  -v business_display_phone_number=593962939146 \
  -v contact_wa_id=593995341161 \
  -f skills/inspect-wa-agents/scripts/diagnose_case_state.sql
```

When running from another repository, use the absolute path under `~/wa-agents`.
Load database credentials without printing them. Keep the transaction and inspection
read-only, and treat message contents and identifiers as sensitive.

Interpret all three result sections together:
- Review every case's route, open state, current FSM state, snapshot coverage, and contact lease.
- Reconstruct ordered case events with their API mappings and queue state.
- Compare every agent-context generation and its ordered message membership.
- Treat one initial snapshot plus one snapshot per case message as the expected history shape.
  Compare the manifest state with the latest snapshot.
- Do not treat a missing API mapping as a delivery failure until message origin and `api_direction` show that an API message should exist.

## Diagnose Flow State

Run the bundled read-only Flow query with the same business and contact identifiers:
```bash
# ENCRYPTION_KEY must already be exported in the client environment.
psql "$DATABASE_URL" \
  -v business_display_phone_number=593962939146 \
  -v contact_wa_id=593995341161 \
  -f skills/inspect-wa-agents/scripts/diagnose_flow_state.sql
```

When running from another repository, use the absolute path under `~/wa-agents`.
The script reads `ENCRYPTION_KEY` directly from the client environment and decrypts
Flow data only in the query result; it never prints the Flow token.
Do not pass the key with `-v`, put it in the script, or commit it.
Treat decrypted customer data as sensitive, and do not redirect, retain, or share the output.
A wrong key stops the script at the first failed decryption.

Review every Flow session together with its launch and completion messages, status, definition, decrypted session state, data exchanges, HTTP results, latency, and errors.
Correlate these rows with the case-state diagnostic and bounded runtime logs before identifying the first broken boundary.

## Inspect Display-Name Decisions

For this infrequent workflow, read [references/rejection-reasons.md](references/rejection-reasons.md).
It contains the read-only query, rejection catalog, interpretation rules, replacement-name guidance, and reporting requirements.

## Report

Lead with the first broken boundary and the evidence supporting it.
For persisted cases and Flows, include the relevant case, queue, FSM, snapshot, context, Flow session, data-exchange, and delivery state.
Confirm that inspection was read-only and distinguish deployed behavior from local code.
