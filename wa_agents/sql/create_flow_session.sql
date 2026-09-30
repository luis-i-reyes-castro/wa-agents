-- PARAMS:
  -- flow                 : wa_api_flows.id
  -- business             : wa_api_businesses.id
  -- contact              : wa_api_contacts.id
  -- case_handler_message : wa_case_handler_messages.id | null
  -- flow_token           : opaque session token
  -- flow_token_hash      : SHA-256 hex digest
  -- session_state        : JSON string | null
  -- secret_key           : ENCRYPTION_KEY

WITH superseded AS (
  UPDATE
    public.wa_api_flow_sessions AS session
  SET
    updated_at     = now(),
    session_status = 'superseded'
  WHERE
    ( session.flow = @flow ) AND
    ( session.contact = @contact ) AND
    ( session.session_status IN ( 'created', 'sent', 'active') )
)
INSERT INTO public.wa_api_flow_sessions (
  flow,
  business,
  contact,
  case_handler_message,
  expires_at,
  flow_token_encrypted,
  flow_token_hash,
  session_state
)
VALUES (
  @flow,
  @business,
  @contact,
  @case_handler_message,
  now() + INTERVAL '24 hours',
  encrypt_text( @flow_token, @secret_key),
  @flow_token_hash,
  CASE
    WHEN @session_state::TEXT IS NULL THEN NULL
    ELSE encrypt_text( @session_state::TEXT, @secret_key)
  END
)
RETURNING *;
