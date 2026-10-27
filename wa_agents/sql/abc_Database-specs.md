# Database Specs

|     |     |
| --- | --- |
| **Provider** | Supabase |
| **SQL Flavor** | PostgreSQL |
| **Application Schema** | `wa_agents` |
| **Canonical DDL** | [`abc_DDL.sql`](abc_DDL.sql) |

# Introduction

This document describes the database objects declared by `abc_DDL.sql`.
The schema stores WhatsApp Business identities, contacts, webhook data, messages, Embedded
Signup state, case-handler routing and persistence, and WhatsApp Flow state.

All package-owned tables live in the `wa_agents` schema.
The DDL enables row-level security on every table but does not create RLS policies; the package accesses these tables through its PostgreSQL connection rather than exposing them through PostgREST.

The custom domain and enum names are unqualified in the DDL and are therefore created
in the active schema from the connection's `search_path` (normally `public`).
The two text-encryption helper functions are explicitly created in `public`.

# Schema Objects

## Extension and Functions

| OBJECT | PURPOSE |
| ------ | ------- |
| `pgcrypto` | Supplies the PGP symmetric-encryption primitives used by the helper functions. |
| `public.encrypt_text(data, key)` | Encrypts text with AES-256 and returns `BYTEA`. |
| `public.decrypt_text(data, key)` | Decrypts the corresponding `BYTEA` and returns text. |

## Domain and Enum Types

| TYPE | ALLOWED VALUES OR RULE |
| ---- | ---------------------- |
| `T_NO_WS_STR` | Non-empty `VARCHAR` containing no whitespace (`^\S+$`). |
| `T_WHATSAPP_MESSAGE` | `text`, `interactive`, `template`, `image`, `video`, `audio`, `document`, `sticker`, `reaction`, `contacts`, `location`, `media_placeholder`, `revoke`, `errors`, `unsupported` |
| `T_WHATSAPP_STATUS` | `delivered`, `failed`, `played`, `read`, `sent` |
| `T_WHATSAPP_CONNECTION_STATUS` | `pending`, `connected`, `attention_needed`, `disconnected` |
| `T_WHATSAPP_SIGNUP_SESSION_STATUS` | `pending`, `completed`, `failed`, `cancelled`, `expired` |
| `T_WHATSAPP_APP_DATA_SYNC_TYPE` | `smb_app_state_sync`, `history` |
| `T_WHATSAPP_APP_DATA_SYNC_STATUS` | `eligible`, `requesting`, `accepted`, `receiving`, `completed`, `declined`, `failed` |
| `T_ENQUEUED_MESSAGE_STATUS` | `done`, `error`, `pending`, `processing` |
| `T_WHATSAPP_FLOW_STATUS` | `DRAFT`, `PUBLISHED`, `DEPRECATED`, `BLOCKED`, `THROTTLED` |
| `T_WHATSAPP_FLOW_SESSION_STATUS` | `created`, `sent`, `active`, `completed`, `failed`, `expired`, `superseded` |
| `T_WHATSAPP_FLOW_EXCHANGE_STATUS` | `processing`, `succeeded`, `failed` |

# Table Aliases

Use the aliases below when a query needs to qualify columns. They are unique within
this schema and stay between two and four characters when practical.

| TABLE | ALIAS |
| ----- | ----- |
| `wa_agents.api_businesses` | `wab` |
| `wa_agents.api_phone_numbers` | `wapn` |
| `wa_agents.api_contacts` | `wac` |
| `wa_agents.api_contact_profiles` | `wacp` |
| `wa_agents.api_embedded_signup_data` | `waesd` |
| `wa_agents.api_embedded_signup_sessions` | `wess` |
| `wa_agents.api_embedded_signup_syncs` | `waessy` |
| `wa_agents.api_smb_app_contacts` | `wasac` |
| `wa_agents.api_inbound_payloads` | `waip` |
| `wa_agents.api_history_chunks` | `wahc` |
| `wa_agents.api_inbound_messages` | `waim` |
| `wa_agents.api_outbound_messages` | `waom` |
| `wa_agents.api_media` | `wam` |
| `wa_agents.api_statuses` | `was` |
| `wa_agents.case_handler_routes` | `chr` |
| `wa_agents.case_handler_settings` | `chs` |
| `wa_agents.api_to_case_handler_queue` | `chq` |
| `wa_agents.case_handler_contact_leases` | `chl` |
| `wa_agents.case_handler_case_manifests` | `chm` |
| `wa_agents.case_handler_messages` | `cmsg` |
| `wa_agents.case_handler_state_histories` | `csh` |
| `wa_agents.case_handler_to_api_bridge` | `cab` |
| `wa_agents.case_handler_agent_contexts` | `cac` |
| `wa_agents.api_flow_encryption_keys` | `wafk` |
| `wa_agents.api_flows` | `waf` |
| `wa_agents.api_flow_sessions` | `wafs` |
| `wa_agents.api_flow_data_exchanges` | `wafe` |
| `wa_agents.api_flow_events` | `wafv` |

# Table Overview

| TABLE | PURPOSE |
| ----- | ------- |
| `api_businesses` | WhatsApp Business Accounts (WABAs). |
| `api_phone_numbers` | Business phone numbers and their WhatsApp metadata. |
| `api_contacts` | Contacts scoped to a business phone number, identified by a WhatsApp ID or Business App user ID. |
| `api_contact_profiles` | Time-varying names and usernames observed for contacts. |
| `api_embedded_signup_data` | Current encrypted business token and connection/subscription state for a WABA. |
| `api_embedded_signup_sessions` | Expiring Embedded Signup attempts and their returned event/application data. |
| `api_embedded_signup_syncs` | Per-phone-number SMB app-state and message-history synchronization state. |
| `api_smb_app_contacts` | Contact data synchronized from the WhatsApp Business App. |
| `api_inbound_payloads` | Deduplicated raw webhook change payloads and validation results. |
| `api_history_chunks` | Progress and errors for individual history payload chunks. |
| `api_inbound_messages` | Normalized inbound customer messages, message echoes, and history messages. |
| `api_outbound_messages` | Messages sent by the chatbot through the WhatsApp API. |
| `api_media` | Object-storage metadata for one inbound or outbound message. |
| `api_statuses` | Delivery/read/failure status events for WhatsApp message IDs. |
| `case_handler_routes` | Business-phone defaults and contact-specific handler routes. |
| `case_handler_settings` | Settings keyed by handler implementation, currently the silence timeout. |
| `api_to_case_handler_queue` | Durable queue of inbound messages awaiting case-handler processing. |
| `case_handler_contact_leases` | Expiring ownership leases that serialize work per contact. |
| `case_handler_case_manifests` | Numbered cases, open/closed state, current FSM state, and silence deadline. |
| `case_handler_messages` | Ordered, typed messages persisted within a case. |
| `case_handler_state_histories` | Append-only FSM snapshots for case creation and persisted messages. |
| `case_handler_to_api_bridge` | Mapping between case-handler messages and inbound/outbound API messages. |
| `case_handler_agent_contexts` | Ordered membership of case messages in named agent contexts. |
| `api_flow_encryption_keys` | One public/encrypted-private RSA key pair per WABA. |
| `api_flows` | Registered WhatsApp Flow definitions and handler routing. |
| `api_flow_sessions` | Expiring single-contact Flow launch sessions and encrypted state/results. |
| `api_flow_data_exchanges` | Encrypted endpoint request/response audits and idempotency records. |
| `api_flow_events` | Flow lifecycle, health, and error webhook events. |

# Table Dependency Levels

**Definition of Dependency Level**

> A level 0 table has no foreign-key dependencies. A table at level `k` has a
> longest foreign-key dependency path of `k` edges to a level 0 table.

Nullable foreign keys still contribute to dependency depth. The levels therefore
describe a valid dependency order, not which relationships must be populated for
every row.

| LEVEL | TABLES |
| ----- | ------ |
| **0** | `api_businesses` <br> `case_handler_settings` |
| **1** | `api_embedded_signup_data` <br> `api_embedded_signup_sessions` <br> `api_flow_encryption_keys` <br> `api_phone_numbers` |
| **2** | `api_contacts` <br> `api_embedded_signup_syncs` <br> `api_flows` |
| **3** | `api_contact_profiles` <br> `api_inbound_payloads` <br> `api_outbound_messages` <br> `api_smb_app_contacts` <br> `case_handler_contact_leases` <br> `case_handler_routes` |
| **4** | `api_flow_events` <br> `api_history_chunks` <br> `api_statuses` <br> `case_handler_case_manifests` |
| **5** | `api_inbound_messages` <br> `case_handler_messages` |
| **6** | `api_flow_sessions` <br> `api_media` <br> `api_to_case_handler_queue` <br> `case_handler_agent_contexts` <br> `case_handler_state_histories` <br> `case_handler_to_api_bridge` |
| **7** | `api_flow_data_exchanges` |

# References and Foreign Keys

All foreign keys use `ON UPDATE CASCADE`. Unless noted otherwise, deletion of the
parent cascades to the child. The two route references use `ON DELETE SET NULL` so
deleting a route does not discard queued work or existing case manifests. Embedded
Signup sessions also retain their audit record and set `business` to `NULL` when the
referenced business is deleted.

## Level 0

### Table `api_businesses`

**OUTGOING REFERENCES:** None.

**INCOMING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_businesses.id` | `api_phone_numbers.business` |
| `api_businesses.id` | `api_embedded_signup_data.business` |
| `api_businesses.id` | `api_embedded_signup_sessions.business` (`ON DELETE SET NULL`) |
| `api_businesses.id` | `api_flow_encryption_keys.business` |

### Table `case_handler_settings`

**OUTGOING REFERENCES:** None.

**INCOMING REFERENCES:** None. The application associates rows with handlers through
`handler_key`; the DDL intentionally declares no foreign key from route or Flow tables.

## Level 1

### Table `api_embedded_signup_data`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_embedded_signup_data.business` | `api_businesses.id` |

**INCOMING REFERENCES:** None.

### Table `api_embedded_signup_sessions`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_embedded_signup_sessions.business` | `api_businesses.id` (`ON DELETE SET NULL`) |

**INCOMING REFERENCES:** None.

### Table `api_flow_encryption_keys`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_flow_encryption_keys.business` | `api_businesses.id` |

**INCOMING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_flow_encryption_keys.id` | `api_flows.flow_waba` |
| `api_flow_encryption_keys.id` | `api_flow_data_exchanges.flow_waba` |
| `api_flow_encryption_keys.id` | `api_flow_events.flow_waba` |

### Table `api_phone_numbers`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_phone_numbers.business` | `api_businesses.id` |

**INCOMING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_phone_numbers.id` | `api_contacts.phone_number` |
| `api_phone_numbers.id` | `api_embedded_signup_syncs.phone_number` |
| `api_phone_numbers.id` | `case_handler_routes.phone_number` |
| `api_phone_numbers.id` | `api_flow_sessions.phone_number` |

## Level 2

### Table `api_contacts`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_contacts.phone_number` | `api_phone_numbers.id` |

**INCOMING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_contacts.id` | `api_contact_profiles.contact` |
| `api_contacts.id` | `api_smb_app_contacts.contact` |
| `api_contacts.id` | `api_inbound_payloads.contact` |
| `api_contacts.id` | `api_inbound_messages.contact` |
| `api_contacts.id` | `api_outbound_messages.contact` |
| `api_contacts.(phone_number, id)` | `case_handler_routes.(phone_number, contact)` |
| `api_contacts.id` | `case_handler_contact_leases.contact` |
| `api_contacts.id` | `case_handler_case_manifests.contact` |
| `api_contacts.(phone_number, id)` | `api_flow_sessions.(phone_number, contact)` |

### Table `api_embedded_signup_syncs`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_embedded_signup_syncs.phone_number` | `api_phone_numbers.id` |

**INCOMING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_embedded_signup_syncs.id` | `api_history_chunks.sync` |

### Table `api_flows`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_flows.flow_waba` | `api_flow_encryption_keys.id` |

**INCOMING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_flows.id` | `api_flow_sessions.flow` |

## Level 3

### Table `api_contact_profiles`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_contact_profiles.contact` | `api_contacts.id` |

**INCOMING REFERENCES:** None.

### Table `api_inbound_payloads`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_inbound_payloads.contact` | `api_contacts.id` |

**INCOMING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_inbound_payloads.id` | `api_history_chunks.payload` |
| `api_inbound_payloads.id` | `api_inbound_messages.payload` |
| `api_inbound_payloads.id` | `api_statuses.payload` |
| `api_inbound_payloads.id` | `api_flow_events.payload` |

### Table `api_outbound_messages`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_outbound_messages.contact` | `api_contacts.id` |

**INCOMING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_outbound_messages.id` | `api_media.outbound_msg_id` |
| `api_outbound_messages.id` | `case_handler_to_api_bridge.api_outbound_msg_id` |
| `api_outbound_messages.id` | `api_flow_sessions.outbound_message` |

### Table `api_smb_app_contacts`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_smb_app_contacts.contact` | `api_contacts.id` |

**INCOMING REFERENCES:** None.

### Table `case_handler_contact_leases`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `case_handler_contact_leases.contact` | `api_contacts.id` |

**INCOMING REFERENCES:** None.

### Table `case_handler_routes`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `case_handler_routes.phone_number` | `api_phone_numbers.id` |
| `case_handler_routes.(phone_number, contact)` | `api_contacts.(phone_number, id)` |

The composite contact reference is nullable: `contact = NULL` denotes a phone-number
default route, while a non-null contact selects a contact-specific route.

**INCOMING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `case_handler_routes.id` | `api_to_case_handler_queue.handler_id` (`ON DELETE SET NULL`) |
| `case_handler_routes.id` | `case_handler_case_manifests.handler_id` (`ON DELETE SET NULL`) |

## Level 4

### Table `api_flow_events`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_flow_events.payload` | `api_inbound_payloads.id` |
| `api_flow_events.flow_waba` | `api_flow_encryption_keys.id` |

**INCOMING REFERENCES:** None.

### Table `api_history_chunks`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_history_chunks.sync` | `api_embedded_signup_syncs.id` |
| `api_history_chunks.payload` | `api_inbound_payloads.id` |

**INCOMING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_history_chunks.id` | `api_inbound_messages.history_chunk` |

### Table `api_statuses`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_statuses.payload` | `api_inbound_payloads.id` |

**INCOMING REFERENCES:** None.

`api_statuses.msg_id` intentionally has no foreign key. It may identify either an
outbound message or an inbound message echo, and a status can arrive before the echo
payload that would provide the matching inbound row.

### Table `case_handler_case_manifests`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `case_handler_case_manifests.handler_id` | `case_handler_routes.id` (`ON DELETE SET NULL`) |
| `case_handler_case_manifests.contact` | `api_contacts.id` |

**INCOMING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `case_handler_case_manifests.id` | `case_handler_messages.case_id` |
| `case_handler_case_manifests.id` | `case_handler_state_histories.case_manifest` |
| `case_handler_case_manifests.id` | `case_handler_agent_contexts.case_manifest` |

## Level 5

### Table `api_inbound_messages`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_inbound_messages.payload` | `api_inbound_payloads.id` |
| `api_inbound_messages.contact` | `api_contacts.id` |
| `api_inbound_messages.history_chunk` | `api_history_chunks.id` |

**INCOMING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_inbound_messages.id` | `api_media.inbound_msg_id` |
| `api_inbound_messages.msg_id` | `api_to_case_handler_queue.msg_id` |
| `api_inbound_messages.id` | `case_handler_to_api_bridge.api_inbound_msg_id` |
| `api_inbound_messages.id` | `api_flow_sessions.completion_message` |

### Table `case_handler_messages`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `case_handler_messages.case_id` | `case_handler_case_manifests.id` |

**INCOMING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `case_handler_messages.(case_id, id)` | `case_handler_state_histories.(case_manifest, case_message)` |
| `case_handler_messages.id` | `case_handler_to_api_bridge.case_handler_msg_id` |
| `case_handler_messages.(case_id, id)` | `case_handler_agent_contexts.(case_manifest, case_message)` |
| `case_handler_messages.id` | `api_flow_sessions.case_handler_message` |

## Level 6

### Table `api_flow_sessions`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_flow_sessions.flow` | `api_flows.id` |
| `api_flow_sessions.phone_number` | `api_phone_numbers.id` |
| `api_flow_sessions.(phone_number, contact)` | `api_contacts.(phone_number, id)` |
| `api_flow_sessions.case_handler_message` | `case_handler_messages.id` |
| `api_flow_sessions.outbound_message` | `api_outbound_messages.id` |
| `api_flow_sessions.completion_message` | `api_inbound_messages.id` |

**INCOMING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_flow_sessions.id` | `api_flow_data_exchanges.session` |

### Table `api_media`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_media.inbound_msg_id` | `api_inbound_messages.id` |
| `api_media.outbound_msg_id` | `api_outbound_messages.id` |

Exactly one of the two message references must be non-null.

**INCOMING REFERENCES:** None.

### Table `api_to_case_handler_queue`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_to_case_handler_queue.handler_id` | `case_handler_routes.id` (`ON DELETE SET NULL`) |
| `api_to_case_handler_queue.msg_id` | `api_inbound_messages.msg_id` |

**INCOMING REFERENCES:** None.

### Table `case_handler_agent_contexts`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `case_handler_agent_contexts.case_manifest` | `case_handler_case_manifests.id` |
| `case_handler_agent_contexts.(case_manifest, case_message)` | `case_handler_messages.(case_id, id)` |

**INCOMING REFERENCES:** None.

### Table `case_handler_state_histories`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `case_handler_state_histories.case_manifest` | `case_handler_case_manifests.id` |
| `case_handler_state_histories.(case_manifest, case_message)` | `case_handler_messages.(case_id, id)` |

**INCOMING REFERENCES:** None.

### Table `case_handler_to_api_bridge`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `case_handler_to_api_bridge.case_handler_msg_id` | `case_handler_messages.id` |
| `case_handler_to_api_bridge.api_inbound_msg_id` | `api_inbound_messages.id` |
| `case_handler_to_api_bridge.api_outbound_msg_id` | `api_outbound_messages.id` |

Exactly one API message reference must be non-null. An inbound API message maps to at
most one case-handler message. One case-handler message may map to multiple outbound
API messages when a long response is split into WhatsApp-sized chunks.

**INCOMING REFERENCES:** None.

## Level 7

### Table `api_flow_data_exchanges`

**OUTGOING REFERENCES:**

| COLUMN | REFERENCE |
| ------ | --------- |
| `api_flow_data_exchanges.flow_waba` | `api_flow_encryption_keys.id` |
| `api_flow_data_exchanges.session` | `api_flow_sessions.id` |

**INCOMING REFERENCES:** None.

# Key Integrity Rules

## Identity and Uniqueness

- WABA IDs, display phone numbers, phone-number IDs, webhook payload hashes,
  WhatsApp message IDs, signup states, sync request IDs, Flow IDs, Flow-token hashes,
  and Flow public-key fingerprints are globally unique in their respective tables.
- A contact's `wa_id` and `user_id` are unique within a business phone number. At
  least one identifier must be present.
- Contact profiles use `UNIQUE NULLS NOT DISTINCT` across contact, name, and username,
  so repeated observations of the same nullable profile tuple are not duplicated.
- There is at most one signup sync of each type per phone number, one Embedded Signup
  data row per WABA, and one Flow encryption-key row per WABA.
- History chunks are unique by payload and zero-based history index.
- Each inbound or outbound API message has at most one media row, and each inbound
  message has at most one queue row.
- Handler routes use `UNIQUE NULLS NOT DISTINCT (phone_number, contact)`. This permits
  one default route and one route per contact for each phone number.
- Case indices and message indices are one-based and unique within their parent. A
  partial unique index permits only one open case per contact.
- State-history snapshots are unique by case and nullable case message. This allows
  one initial snapshot without a message and one snapshot per persisted message.
- Agent-context membership is unique by agent, context index, case, and nullable case
  message.
- An active Flow key is unique within a WABA. Inactive historical definitions may
  reuse the same key.
- Flow endpoint requests are unique by `(session, request_hash)` when a session is
  resolved, enabling retry replay without duplicating work.

## Validation Checks

- JSON metadata/application-data objects and agent/case message objects must have the
  expected JSON object shape; history-chunk errors must be a JSON array.
- A validated inbound payload cannot retain validation errors.
- Completed Embedded Signup sessions must reference a business, a session is pending
  if and only if `finished_at` is null, and all signup/Flow expirations must be after
  creation.
- An Embedded Signup subscription marked active must have `subscribed_at` populated.
- History chunk indices and orders are nonnegative, phases are `0..2`, progress is
  `0..100`, and a chunk records complete progress coordinates, errors, or both.
- A history context is allowed only when its inbound message belongs to a history
  chunk.
- Media size and Flow exchange latency cannot be negative.
- Case and message indices begin at 1; agent context indices begin at 0.
- Contact leases must expire after their latest heartbeat, and handler silence
  timeouts must be positive.

# Indexes and Access Paths

In addition to primary-key and unique-constraint indexes, the DDL provides explicit
indexes for these access patterns:

- contact profile timelines;
- pending/expired signup sessions and recent sessions per business;
- signup syncs by status and update time;
- inbound payload receipt time, contact, and invalid payloads with errors;
- history synchronization progress;
- inbound messages by payload, contact timeline, and history chunk;
- outbound messages by contact and status events by payload or WhatsApp message ID;
- routes by handler key and queued jobs by handler/status/creation order;
- expired contact leases and open/current cases by contact;
- case-message composite references and state-history timelines;
- inbound bridge uniqueness and all bridge rows for a case-handler message;
- current agent context and context membership by message;
- active Flow definitions by WABA/key and definitions by handler key;
- Flow sessions by contact/status and unexpired-work scans;
- Flow exchanges by WABA timeline; and
- Flow events by Flow ID timeline.

# Design and Normalization Notes

The schema is predominantly normalized: WABAs own phone numbers, phone numbers own
contacts, webhook payloads are separated from normalized messages/statuses, and case
messages are separated from their API transport mappings. The following deliberate
choices are important when querying or evolving the schema:

- `api_statuses.msg_id` is deliberately polymorphic and unreferenced because it may
  identify an outbound message or an inbound echo, including an echo not yet stored.
- `case_handler_routes.handler_key`, `case_handler_settings.handler_key`, and
  `api_flows.handler_key` are application-level keys rather than database foreign
  keys. This lets workers claim only handler implementations in their local registry.
- Queue rows and case manifests retain work when a route is deleted by setting their
  nullable route ID to `NULL`; the application then uses its configured fallback.
- `case_handler_case_manifests.machine_state` caches the current FSM state, while
  `case_handler_state_histories` preserves the append-only snapshot timeline.
- `case_handler_case_manifests.next_message_index` is a counter used to allocate
  ordered case-message positions; it is intentionally stored rather than derived.
- `case_handler_to_api_bridge` supports one-to-many outbound chunking but restricts
  each inbound API message to a single case-handler message.
- Flow tokens, private keys, session application state, endpoint request bodies, and
  endpoint responses are represented as `BYTEA` so the application can store them
  encrypted at rest. Their hashes and public metadata remain separately queryable.
