-- PARAMS:
  -- session_id       : api_flow_sessions.id
  -- outbound_message : api_outbound_messages.id

UPDATE
  wa_agents.api_flow_sessions AS session
SET
  updated_at       = now(),
  outbound_message = @outbound_message,
  session_status   = 'sent'
WHERE
  ( session.id = @session_id )
RETURNING *;
