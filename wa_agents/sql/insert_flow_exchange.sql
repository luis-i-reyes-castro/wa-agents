-- PARAMS:
  -- flow_waba     : wa_api_flow_wabas.id
  -- session_id    : wa_api_flow_sessions.id | null
  -- request_hash  : SHA-256 hex digest
  -- request_action : Flow endpoint action
  -- request_screen : Flow screen | null
  -- request_data  : canonical JSON string
  -- secret_key    : ENCRYPTION_KEY

INSERT INTO public.wa_api_flow_data_exchanges (
  flow_waba,
  session,
  request_hash,
  request_action,
  request_screen,
  request_data
)
VALUES (
  @flow_waba,
  @session_id,
  @request_hash,
  @request_action,
  @request_screen,
  encrypt_text( @request_data, @secret_key)
)
ON CONFLICT ( session, request_hash)
  WHERE ( session IS NOT NULL )
DO
  NOTHING
RETURNING *;
