# wa-agents

A Python toolkit for building WhatsApp chatbot backends.

It gives you reusable building blocks for:
- webhook validation and payload parsing,
- queued processing,
- per-user case/context storage in DigitalOcean Spaces,
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

1. `Listener` receives webhook payloads and validates them as `WhatsAppPayload`.
2. Payloads are enqueued in `QueueDB` (SQLite) to decouple HTTP from processing.
3. `QueueWorker` drains queue items and calls your `CaseHandler`.
4. `CaseHandlerBase` handles dedup, case open/close logic, context persistence, and
   WhatsApp sending helpers.
5. Your `CaseHandler` implements business logic in:
   - `process_message(...)` for ingestion-time decisions,
   - `generate_response(...)` for response generation (single or multi-turn).

## Required Environment Variables

### DigitalOcean Spaces (required)

These names must match the current code:

| Variable | Description |
| --- | --- |
| `BUCKET_NAME` | Space name |
| `BUCKET_REGION` | Region code, for example `atl1` |
| `BUCKET_KEY_ID` | Access key ID |
| `BUCKET_KEY_SECRET` | Secret access key |

### WhatsApp (required)

| Variable | Description |
| --- | --- |
| `WA_TOKEN` | Graph API access token |
| `WA_VERIFY_TOKEN` | Token used by webhook verification endpoint |

### LLM keys (optional, only if using `Agent`)

- `OPENROUTER_API_KEY`
- `OPENAI_API_KEY`
- `MISTRAL_API_KEY`

### Queue tuning (optional)

| Variable | Default | Purpose |
| --- | --- | --- |
| `QUEUE_DB_DIR` | app directory | where SQLite queue file lives |
| `QUEUE_DB_NAME` | `queue.sqlite3` | SQLite file name |
| `QUEUE_POLL_INTERVAL_BUSY` | `0.2` | worker sleep when active |
| `QUEUE_POLL_INTERVAL_IDLE` | `1.0` | worker sleep when idle |
| `QUEUE_RESPONSE_DELAY` | `1.0` | delay before running `generate_response` |

## Minimal App Skeleton

A production app typically has these three files:

- `run_listener.py`
- `run_queue_worker.py`
- `casehandler.py`

### `run_listener.py`

```python
#!/usr/bin/env python3

import os
from dotenv import load_dotenv
from pathlib import Path

from sofia_utils.io import ensure_dir
from wa_agents.queue_db import QueueDB
from wa_agents.listener import Listener

load_dotenv()

QUEUE_DB_DIR  = os.getenv("QUEUE_DB_DIR", str(Path(__file__).parent))
QUEUE_DB_NAME = os.getenv("QUEUE_DB_NAME", "queue.sqlite3")
QUEUE_DB_PATH = Path(QUEUE_DB_DIR).expanduser().resolve() / QUEUE_DB_NAME
ensure_dir(QUEUE_DB_PATH.parent)

queue_db = QueueDB(QUEUE_DB_PATH)
app      = Listener(__name__, queue_db)

if __name__ == "__main__":
    app.run(port=8080, debug=True)
```

### `run_queue_worker.py`

```python
#!/usr/bin/env python3

import gc
import logging
import os
import signal
import sys
from dotenv import load_dotenv
from pathlib import Path

from sofia_utils.io import ensure_dir
from wa_agents.queue_db import QueueDB

load_dotenv()

from wa_agents.queue_worker import QueueWorker
from casehandler import CaseHandler

QUEUE_DB_DIR  = os.getenv("QUEUE_DB_DIR", str(Path(__file__).parent))
QUEUE_DB_NAME = os.getenv("QUEUE_DB_NAME", "queue.sqlite3")
QUEUE_DB_PATH = Path(QUEUE_DB_DIR).expanduser().resolve() / QUEUE_DB_NAME
ensure_dir(QUEUE_DB_PATH.parent)


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    queue  = QueueDB(QUEUE_DB_PATH)
    worker = QueueWorker(queue, CaseHandler)

    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)

    worker.serve_forever()
    gc.collect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

## `CaseHandler` Design Patterns

All patterns below extend `CaseHandlerBase` and implement:
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

- Initialize `CaseHandlerBase` itself as a `transitions.Machine` with `init_machine(...)`.
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

## WhatsApp Payload Data You Can Use

The parsed payload model is `WhatsAppPayload`.

Most routing happens in `WhatsAppMsg` fields:
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
def process_message(self, message: WhatsAppMsg, media_content: MediaContent | None=None) -> bool:
    msg = self.dedup_and_ingest_message(message, media_content)
    if not msg:
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
- pass a `list[str]` to use OpenRouter with optional fallback models.
- pass a single `str` for direct API mode (`openai/...` or `mistral/...`).

### 1) Plain text completion

```python
from wa_agents.agent import Agent
from wa_agents.basemodels import UserContentMsg

agent = Agent("main", ["openai/gpt-5-mini"])
agent.load_prompts(["prompts/main.md"])

context = [UserContentMsg(text="Hello, summarize this ticket.")]
resp    = agent.get_response(context=context, max_tokens=400)
```

### 2) With tools

```python
agent = Agent("main", ["openai/gpt-5-mini"])
agent.load_prompts(["prompts/main.md"])
agent.load_tools(["agent_tools/main_openai.json"])

resp = agent.get_response(context=context)
if resp and resp.tool_calls:
    # execute tool calls, create ToolResultsMsg, append to context, call again
    ...
```

Notes:
- Mistral is blocked for tool calls in `Agent.load_tools()`.
- For OpenRouter, pass a list of models to enable fallback models.

### 3) Structured output

```python
from pydantic import BaseModel

class TicketSummary(BaseModel):
    summary: str
    severity: str

resp = agent.get_response(context=context, output_st=TicketSummary)
if resp and resp.st_output:
    data = resp.st_output
```

### 4) Image + text context

```python
from wa_agents.basemodels import UserContentMsg, load_media

md, mc = load_media("tests/photo.jpg")
context    = [UserContentMsg(text="Describe this issue.", media=md)]
imgs_cache = {md.name: mc.content}

resp = agent.get_response(context=context,
                          load_imgs=True,
                          imgs_cache=imgs_cache)
```

## Case and Storage Behavior (built in)

`CaseHandlerBase` already provides:
- per-user data lookup from phone number (`UserData`),
- stale-case rollover (`TIME_LIMIT_STALE`, default 48h),
- case manifests with ordered `message_ids`,
- idempotency (`dedup/<provider_message_id>.json`),
- media persistence,
- context replay into your optional handler state machine,
- send helpers for text and interactive messages.

Storage layout under DigitalOcean Spaces:

```txt
<operator_id>/<user_id>/
  user_data.json
  case_index.json
  dedup/<idempotency_key>.json
  cases/<case_id>/
    case_manifest.json
    messages/<message_id>.json
    media/<message_id>.<extension>
```

## Reference Implementations

Use these as templates when building new bots:

- [`da-assistant`](https://github.com/luis-i-reyes-castro/da-assistant)
  - Multi-turn chatbot.
  - Uses built-in `CaseHandlerBase` state-machine support + staged agent calls + tool call loop.
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
