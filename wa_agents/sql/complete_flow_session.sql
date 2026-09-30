-- PARAMS:
  -- flow_token_hash   : SHA-256 hex digest
  -- completion_message : wa_api_inbound_messages.id
  -- completion_data   : canonical JSON string
  -- secret_key        : ENCRYPTION_KEY

UPDATE
  public.wa_api_flow_sessions AS session
SET
  updated_at         = now(),
  completed_at       = now(),
  completion_message = @completion_message,
  completion_data    = encrypt_text( @completion_data, @secret_key),
  session_status     = 'completed'
WHERE
  ( session.flow_token_hash = @flow_token_hash ) AND
  ( session.session_status IN ( 'sent', 'active') )
RETURNING *;
