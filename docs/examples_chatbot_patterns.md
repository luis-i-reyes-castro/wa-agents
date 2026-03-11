# Chatbot Pattern Examples

These examples are private-safe, minimal templates for common `wa-agents`
architectures.

## Files

- `example_casehandler_single_turn_no_llm.py`
  - Deterministic responses (no model call, no tools).
  - Best for rule-based replies, fixed templates, simple lookups.

- `example_casehandler_single_turn_with_llm.py`
  - One model call per inbound message.
  - Best for summarization, rewriting, classification, lightweight assistants.

- `example_casehandler_multi_turn_tools.py`
  - Multi-turn assistant with tool-call loop.
  - Best for workflows where the model needs external functions/data.

## Notes

- These are templates, not a runnable app by themselves.
- Pair them with:
  - `run_listener.py`
  - `run_queue_worker.py`
- If you need webhook payload introspection, use:
  - `demo_webhook.py`
  - `instructions_demo_webhook.md`
  - `instructions_whatsapp_api.md`
