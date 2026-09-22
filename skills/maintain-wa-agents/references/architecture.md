# Architecture and Persistence Invariants

Use this map to locate the source of truth, then read the current implementation
before editing. The schema and APIs may evolve after this reference is written.

## Runtime Flow

1. `WhatsAppAPIServer.webhook()` receives a raw dictionary and passes it to
   `AsyncQueueDB.enqueue()`.
2. `QueueDB` or `AsyncQueueDB` inserts the raw payload before Pydantic validation.
3. Successful validation resolves the business and contact, attaches payload
   metadata, writes inbound messages or statuses, and enqueues new message IDs.
4. `QueueDB.claim_next()` atomically claims a pending row and a 90-second contact
   lease. This prevents concurrent work for the same contact across processes.
5. `QueueWorker` reconstructs `WhatsAppDatabaseRecord_Business`,
   `WhatsAppDatabaseRecord_Contact`, and the Meta message, then creates the consumer's
   case handler as a child of `WhatsAppCaseHandler` or
   `AsyncWhatsAppCaseHandler`.
6. The handler ingests the message, persists case context and FSM state, and may
   schedule one or more response passes while renewing the same contact lease.

## Ownership Boundaries

- `whatsapp_models.py`: structures controlled by Meta. Model observed webhook
  variants accurately; do not place database row models here.
- `case_handler_base.py`: transport-neutral sync and async case/FSM/context bases.
  The bases resolve business/contact records or row IDs through `supabase.py` and own
  S3 access and media hydration. WhatsApp subclasses add Meta-message ingestion,
  API-row linking, and sending.
- `case_handler_models.py`: application conversation messages, media descriptors,
  manifests, and LLM context types. `AssistantMsg` owns the LLM model/context fields;
  `CaseManifest.machine_state` owns persisted FSM state.
- `HumanUserMsg` represents customer-originated messages with LLM role `user`.
  `HumanServerMsg` represents WhatsApp Business message echoes with LLM role
  `assistant`; persist them in context without treating them as new customer input.
- `supabase.py`: database-record models and sync/async resolution methods plus thin
  PostgreSQL gateways. Keep it independent of S3.
- `S3_bucket_storage.py`: media-byte storage only.

## Database Invariants

Treat `wa_agents/sql/abc_DDL.sql` as the canonical schema.

- All structured data lives in PostgreSQL. Only media bytes live in S3.
- `wa_api_inbound_payloads` combines raw audit data and resolved metadata.
  `contact` must remain nullable so malformed or otherwise unresolvable payloads can
  still be persisted with validation errors.
- Treat each `WhatsAppValue` as carrying at most one contact identity. Do not
  generalize that cardinality without a real requirement and a schema decision.
- `data_hash` makes raw payload ingestion idempotent. Message IDs independently
  protect normalized inbound-message deduplication.
- Contact profiles are upserted against their unique identity rather than appended
  for every inbound message. Business and contact-profile refreshes update first so
  routine webhook processing does not consume identity sequence values.
- Case-handler routes belong to a business and may optionally target one contact.
  Contact routes override the business default; queue rows and case manifests retain
  the nullable route ID, while an absent route uses the worker's fallback handler.
- Inbound messages and statuses reference the combined payload row directly.
- Status `msg_id` values may identify API outbound messages or inbound message
  echoes, so the status table indexes that external ID without an outbound-only
  foreign key.
- One contact may have at most one open case through a partial unique index.
- Case and message indices are one-based insertion ordinals, unique within their
  contact and case respectively. Message allocation increments the manifest counter
  atomically with message persistence.
- Case manifests persist `machine_state` so rebuilding a handler does not require
  replay merely to recover its current state.
- A case-handler message may map to multiple outbound WhatsApp rows because text can
  be chunked. An inbound WhatsApp row maps to at most one case-handler message.
- Contact leases expire after 90 seconds and are owned by UUID tokens. Claim, renew,
  and release operations must remain owner-aware.

## S3 Invariant

Build media keys through `media_object_key()` and preserve this layout:

```text
<business_display_phone_number>/
  <contact_user_id>/
    <case_index>_<message_index>.<extension>
```

For inbound media, the case handler supplies the business display phone number,
contact API user ID, and the persisted one-based case and message indices. Persist
the resulting object key and media metadata in PostgreSQL; do not infer paths later
from mutable message fields.

## Consumer Integration

`sofia-server/backend/whatsapp_casehandler.py` is the main local production consumer.
Constructor, stored-message, FSM, or media changes often require a corresponding
consumer update. Search other repositories rather than assuming it is the only one.

For runtime inspection of the SOFIA deployment, follow
`~/sofia-server/skills/inspect-sofia-runtime/SKILL.md`. Its query and environment
tools provide bounded, redacted, read-only diagnosis and should not be copied here.
