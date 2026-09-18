# wa-agents

A Python toolkit for building WhatsApp chatbot backends.

It gives you reusable building blocks for:
- webhook validation and payload parsing,
- queued processing,
- per-user case/context storage in Supabase Postgres,
- media storage in S3-compatible buckets,
- idempotent ingestion,
- multi-turn orchestration,
- optional LLM calls (text, images, tools, structured output).

This README is based on:
- [`da-assistant`](https://github.com/luis-i-reyes-castro/da-assistant)
  (multi-turn + tools + image flow),
- private-safe templates under `docs/`.

## Installation

### As an editable package

```bash
git clone https://github.com/luis-i-reyes-castro/wa-agents.git
pip install -e wa-agents/
```

### As a dependency

Add this line to your `requirements.txt`:

```txt
wa-agents @ git+https://github.com/luis-i-reyes-castro/wa-agents.git@main
```

Then install:

```bash
pip install -r requirements.txt
```

## Runtime Architecture

`wa-agents` is designed around this flow:

1. `WhatsAppAPIServer` receives webhook payload dictionaries and passes them to `QueueDB`.
2. `QueueDB` audits and validates each payload, normalizes its messages in Supabase
   Postgres, and enqueues newly persisted message IDs.
3. `AsyncQueueWorker` runs inside the FastAPI lifespan, drains queue items, and
   calls your `CaseHandler`.
4. `CaseHandlerBase` resolves the persisted business/contact identity and handles
   case open/close logic, context persistence, FSM state, and S3 media access without
   depending on a messaging transport.
5. `WhatsAppCaseHandler` adds webhook-message conversion, deduplication, WhatsApp
   media-metadata persistence, API-message linking, and sending helpers.
6. Your `CaseHandler` implements business logic in:
   - `process_message(...)` for ingestion-time decisions,
   - `generate_response(...)` for response generation (single or multi-turn).

## Required Environment Variables

### Storage backend

| Variable | Description |
| --- | --- |
| `WA_AGENTS_STORAGE_BACKEND` | Optional `supabase` or `s3`.<br>Defaults to `supabase`. |

### Supabase Postgres storage (required)

| Variable | Description |
| --- | --- |
| `SUPABASE_DB_CONNECTION_URL_IPv4` | Dashboard<br>➡️ Connect<br>➡️ Connection String<br>✅ Type: URI<br>✅ **Method: Session Pooler (IPv4)** |
| `SUPABASE_DB_CONNECTION_URL_IPv6` | Optional fallback Supabase connection URI for IPv6. |

Supabase credentials are mandatory because webhook audit storage and `QueueDB`
always use Postgres. Apply the schema in `wa_agents/sql/DDL.sql` before running.

### S3-compatible bucket storage (required for media and legacy `s3` mode)

| Variable | Description |
| --- | --- |
| `BUCKET_NAME` | Bucket name |
| `BUCKET_REGION` | Region code, for example `atl1` or `us-east-1` |
| `BUCKET_KEY_ID` | Access key ID |
| `BUCKET_KEY_SECRET` | Secret access key |
| `BUCKET_ENDPOINT` | Optional endpoint URL.<br>Defaults to Digital Ocean Spaces for `BUCKET_REGION`.<br>Set this for other S3-compatible services. |
| `BUCKET_ADDRESSING_STYLE` | Optional `path`, `virtual`, or `auto`.<br>Defaults to `virtual` for DigitalOcean Spaces endpoints and `path` for custom endpoints. |

### WhatsApp (required)

| Variable | Description |
| --- | --- |
| `WA_APP_SECRET` | Meta app secret used to validate webhook payload signatures |
| `WA_TOKEN` | Graph API access token |
| `WA_VERIFY_TOKEN` | Token used by webhook verification endpoint |

### LLM keys (optional, only if using `Agent`)

- `OPENROUTER_API_KEY`
- `OPENAI_API_KEY`
- `MISTRAL_API_KEY`

### Queue tuning (optional)

| Variable | Default | Purpose |
| --- | --- | --- |
| `QUEUE_POLL_INTERVAL_BUSY` | `0.2` | worker sleep when active |
| `QUEUE_POLL_INTERVAL_IDLE` | `1.0` | worker sleep when idle |
| `QUEUE_RESPONSE_DELAY` | `1.0` | delay before running `generate_response` |

## Minimal App Skeleton

A production app can run as a single FastAPI process:

- `casehandler.py`
- `app.py`

### `app.py`

```python
#!/usr/bin/env python3

from dotenv import load_dotenv

load_dotenv()

from wa_agents.whatsapp_api_server import WhatsAppAPIServer
from casehandler import CaseHandler


class App(WhatsAppAPIServer):
    pass


app = App( title = "WhatsApp Bot", handler_cls = CaseHandler)
```

Run it with:

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

The server registers `GET /webhook` for Meta verification, `POST /webhook` for
payload ingestion, and `GET /healthz` for container health checks. `QueueDB`
performs payload auditing, validation, normalization, and enqueueing in the request
path; the async queue worker starts and stops with the FastAPI lifespan.

An example container recipe is available at
[`docs/Dockerfile.fastapi`](docs/Dockerfile.fastapi).

## `CaseHandler` Design Patterns

The WhatsApp patterns below extend `WhatsAppCaseHandler` or
`AsyncWhatsAppCaseHandler` and implement:
- `process_message(...) -> bool`
- `generate_response(...) -> bool`

Return semantics:
- `False` means no more immediate response work.
- `True` means worker should run another response pass.

### 1) Single-turn, no tools, no LLM

Template:
- [`docs/example_casehandler_single_turn_no_llm.py`](docs/example_casehandler_single_turn_no_llm.py)

- `process_message`: dedup + ingest, then `return True`.
- `generate_response`: run DB/business logic, send a text, `return False`.

Good for deterministic bots: lookups, status reports, alerts.

### 2) Single-turn, with LLM

Template:
- [`docs/example_casehandler_single_turn_with_llm.py`](docs/example_casehandler_single_turn_with_llm.py)

- `process_message`: gate by whitelist/regex (for example, patient ID).
- `generate_response`: gather external data, optionally call `Agent`, send one answer,
  then `return False`.

### 3) Multi-turn, with state machine

Used in [`da-assistant/casehandler.py`](https://github.com/luis-i-reyes-castro/da-assistant/blob/main/casehandler.py).

- Initialize `CaseHandlerBase` or `WhatsAppCaseHandler` itself as a
  `transitions.Machine` with `init_machine(...)`.
- Initialize `AsyncCaseHandlerBase` or `AsyncWhatsAppCaseHandler` itself as a
  `transitions.AsyncMachine`.
  Async handlers that fire triggers from `async` methods must use
  `await self.trigger(...)`.
- [`context_build()`](wa_agents/case_handler_base.py) replays stored case messages into the handler state machine.
- [`context_update()`](wa_agents/case_handler_base.py) incrementally feeds new messages into the handler state machine.
- [`generate_response()`](https://github.com/luis-i-reyes-castro/da-assistant/blob/main/casehandler.py) checks the current state's `while_in` actions and routes to step handlers.
- Each step can decide whether to continue (`True`) or wait for user (`False`).

Use `on_enter` and `on_exit` only for true FSM callbacks that should run when a
transition changes state. Use `while_in` for response-generation actions such as
`ask_for_*` and `call_*_agent`, which must still be available when a new message
is ingested but the machine remains in the same state.

This distinction matters because `CaseHandlerBase.init_machine(...)` sets
`auto_transitions = False`. With that setting, the machine can ingest a message,
stay in the same state, and therefore skip `on_enter`. `generate_response()`
must then manually dispatch the current state's `while_in` actions. If you
instead enabled `auto_transitions = True` to force same-state transitions, you
would also need to handle that same state's `on_exit` + `on_enter` firing on
each such loop.

### 4) Multi-turn, with tool calls

Used in [`da-assistant/casehandler.py`](https://github.com/luis-i-reyes-castro/da-assistant/blob/main/casehandler.py) (`call_match_agent`, `call_main_agent`) and mirrored in:
- [`docs/example_casehandler_multi_turn_tools.py`](docs/example_casehandler_multi_turn_tools.py)

Loop shape:

1. Call agent.
2. Save/send assistant text.
3. If there are no `tool_calls`, stop.
4. Execute tool calls in your tool server.
5. Store a `ToolResultsMsg` in context.
6. Return `True` so `generate_response()` runs again with updated context.

## Non-WhatsApp Case Handlers

The models in `case_handler_models.py` and the `Agent` classes are not tied to a
WhatsApp transport. For a stateless API handler, build the context directly from
`Message` subclasses, call the agent, and return its result. There is no need to
instantiate `CaseHandlerBase` or create WhatsApp API database records.

A non-WhatsApp handler can reuse `CaseHandlerBase` or `AsyncCaseHandlerBase` when it
needs persisted messages, case manifests, or FSM state. The current persistence
schema identifies cases through `wa_api_contacts`, so this setup requires placeholder
business and contact rows in `wa_api_businesses` and `wa_api_contacts`. Construct the
core handler with those rows or their IDs, then use `case_handler_models.Message`
subclasses for the actual API input and output.

In this mode:

- Implement `process_message()` as a no-op to satisfy the abstract interface.
- Persist messages with `context_update()` and load them with `context_build()`.
- Do not call `dedup_and_ingest_message()` or the WhatsApp `send_*()` methods.
- No row is written to `wa_case_handler_to_api`; that mapping is populated only by
  the WhatsApp ingestion and sending paths.
- Start a fresh case for every API call and close it in a `finally` block when the
  call completes. Before opening it, close or recover any case left open by an
  interrupted earlier call.

The schema permits only one open case per contact. Calls sharing one placeholder
contact must therefore be serialized. If requests may overlap, use separate
placeholder contacts for their independent concurrency keys.

## WhatsApp Payload Data You Can Use

The parsed payload model is `WhatsAppPayload`.

`ServerMsg.user_eyes=True` marks a message meant only for the end user, for example
  transient UX text like "Thinking..." or "Looking up in database...". These
  messages are excluded from LLM-readable context.

`dedup_and_ingest_message()` maps normal inbound messages to `HumanUserMsg`
subclasses and WhatsApp Business message echoes to `HumanServerMsg` subclasses.
Persist both in context, but generally return `False` for `HumanServerMsg` so an
operator's message does not trigger a chatbot reply.

Most routing happens in `WhatsAppMessage` fields:
- `message.type`: `text`, `interactive`, `image`, `video`, `audio`, `sticker`, etc.
- `message.text.body`: user text content.
- `message.interactive.choice`: selected button/list option.
- `message.media_data`: normalized media descriptor for image/video/audio/sticker.
- `message.context`: replied-to message metadata.

Also useful:
- `message.user`: sender phone.
- `contact.wa_id` and `contact.profile.name`.
- `value.metadata.phone_number_id` and `display_phone_number`.

Example branch logic in `process_message`:

```python
from wa_agents.case_handler_models import HumanServerMsg

def process_message(
  self,
  message       : WhatsAppMessage,
  media_content : MediaContent | None = None,
) -> bool:
    msg = self.dedup_and_ingest_message( message, media_content)
    if not msg:
        return False

    if isinstance( msg, HumanServerMsg):
        return False

    if message.type == "interactive":
        # user selected a button/list option
        return True

    if message.type == "image":
        # process media metadata/caption
        return True

    if message.type != "text":
        # optionally send unsupported-type message
        return False

    # text flow
    return True
```

## Agent Usage Recipes

Constructor behavior:
- pass a single OpenRouter model string, or
- pass a `list[str]` to enable OpenRouter fallback models.

### 1) Plain text completion

```python
from wa_agents.agent import AsyncAgent
from wa_agents.case_handler_models import HumanUserContentMsg

agent = AsyncAgent( "main", ["openai/gpt-5-mini"])
agent.load_prompts(["prompts/main.md"])

context = [ HumanUserContentMsg( text = "Hello, summarize this ticket.") ]
resp    = await agent.get_response( context = context, max_tokens = 400)
```

### 2) With tools

```python
agent = AsyncAgent( "main", ["openai/gpt-5-mini"])
agent.load_prompts(["prompts/main.md"])
agent.load_tools(["agent_tools/main_openai.json"])

resp = await agent.get_response( context = context)
if resp and resp.tool_calls:
    # execute tool calls, create ToolResultsMsg, append to context, call again
    ...
```

Note: pass a list of models to enable OpenRouter fallback models.

### 3) Structured output

```python
from pydantic import BaseModel

class TicketSummary (BaseModel):
    summary  : str
    severity : str

resp = await agent.get_response( context = context, output_st = TicketSummary)
if resp and resp.st_output:
    data = resp.st_output
```

### 4) Image + text context

```python
from wa_agents.case_handler_models import HumanUserContentMsg, load_media

md, mc     = load_media("tests/photo.jpg")
context    = [ HumanUserContentMsg( text = "Describe this issue.", media = md) ]
imgs_cache = { md.name: mc.content }

resp = await agent.get_response(
  context    = context,
  load_imgs  = True,
  imgs_cache = imgs_cache,
)
```

## Case and Storage Behavior (built in)

`CaseHandlerBase` and `AsyncCaseHandlerBase` provide:

- business/contact lookup when constructed with database row IDs,
- per-user data lookup from the resolved contact (`UserData`),
- stale-case rollover (`TIME_LIMIT_STALE`, default 48h),
- case manifests reconstructed from stored message rows,
- context replay into your optional handler state machine,
- S3 access and stored-media hydration,
- persisted FSM state, and
- contact leases.

`WhatsAppCaseHandler` and `AsyncWhatsAppCaseHandler` additionally provide:

- idempotent inbound-message ingestion,
- WhatsApp media-metadata persistence,
- WhatsApp API message linking, and
- send helpers for text, templates, and interactive messages.

Supabase stores:
- incoming queue rows in `wa_incoming_queue`,
- validated webhook payloads in `wa_webhook_payloads`,
- exploded webhook messages/statuses in `wa_webhook_messages` and
  `wa_webhook_statuses`,
- caseflow users/cases/messages in `wa_users`, `wa_cases`, and `wa_messages`.

S3-compatible bucket storage is only used for media bytes:

```txt
<business_id>/<contact_id>/<case_id>_<message_id>.<extension>
```

## Reference Implementations

Use these as templates when building new bots:

- [`da-assistant`](https://github.com/luis-i-reyes-castro/da-assistant)
  - Multi-turn chatbot.
  - Uses built-in case-handler state-machine support + staged agent calls + tool call loop.
  - Includes image flow and interactive model selection.

- [`docs/examples_chatbot_patterns.md`](docs/examples_chatbot_patterns.md)
  - Local templates for single-turn and multi-turn handlers.
  - Replaces references to private implementations.

## Local Webhook Introspection

For payload exploration only, see:
- [`docs/instructions_whatsapp_api.md`](docs/instructions_whatsapp_api.md)
- [`docs/instructions_demo_webhook.md`](docs/instructions_demo_webhook.md)
- [`docs/demo_webhook.py`](docs/demo_webhook.py)

These are useful when mapping incoming WhatsApp JSON to your bot routing logic.
