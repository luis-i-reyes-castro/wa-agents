-- PARAMS:
  -- msg_id : WhatsAppMessageID

INSERT INTO public.wa_api_to_case_handler_queue (
  msg_id
)
VALUES (
  @msg_id
)
ON CONFLICT (msg_id)
DO
  NOTHING
RETURNING
  id,
  created_at,
  updated_at,
  last_error_at,
  msg_id,
  msg_status;
