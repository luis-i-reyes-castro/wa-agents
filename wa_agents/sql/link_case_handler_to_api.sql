-- PARAMS:
  -- case_handler_msg_id : wa_case_handler_messages.id
  -- api_inbound_msg_id  : wa_api_inbound_messages.id | None
  -- api_outbound_msg_id : wa_api_outbound_messages.id | None
-- NOTE:
  -- An empty result may mean an incompatible mapping hit a uniqueness rule;
  -- callers must treat it as a rejected link, not necessarily an idempotent duplicate.

INSERT INTO public.wa_case_handler_to_api (
  case_handler_msg_id,
  api_inbound_msg_id,
  api_outbound_msg_id
)
VALUES (
  @case_handler_msg_id,
  @api_inbound_msg_id,
  @api_outbound_msg_id
)
ON CONFLICT
DO
  NOTHING
RETURNING
  id,
  created_at,
  case_handler_msg_id,
  api_inbound_msg_id,
  api_outbound_msg_id;
