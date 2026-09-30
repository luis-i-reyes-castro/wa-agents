-- PARAMS:
  -- session_id       : wa_api_flow_sessions.id
  -- outbound_message : wa_api_outbound_messages.id

UPDATE
  public.wa_api_flow_sessions AS session
SET
  updated_at       = now(),
  outbound_message = @outbound_message,
  session_status   = 'sent'
WHERE
  ( session.id = @session_id )
RETURNING *;
