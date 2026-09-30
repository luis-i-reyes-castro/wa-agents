-- PARAMS:
  -- session_id      : wa_api_flow_sessions.id
  -- session_status  : T_WHATSAPP_FLOW_SESSION_STATUS
  -- session_state   : JSON string | null
  -- completion_data : JSON string | null
  -- last_error      : JSON | null
  -- secret_key      : ENCRYPTION_KEY

UPDATE
  public.wa_api_flow_sessions AS session
SET
  updated_at      = now(),
  activated_at    = CASE
    WHEN ( @session_status::T_WHATSAPP_FLOW_SESSION_STATUS = 'active' )
      THEN COALESCE( session.activated_at, now())
    ELSE session.activated_at
  END,
  completed_at    = CASE
    WHEN ( @session_status::T_WHATSAPP_FLOW_SESSION_STATUS = 'completed' )
      THEN now()
    ELSE session.completed_at
  END,
  session_status  = @session_status::T_WHATSAPP_FLOW_SESSION_STATUS,
  session_state   = CASE
    WHEN @session_state::TEXT IS NULL THEN session.session_state
    ELSE encrypt_text( @session_state::TEXT, @secret_key)
  END,
  completion_data = CASE
    WHEN @completion_data::TEXT IS NULL THEN session.completion_data
    ELSE encrypt_text( @completion_data::TEXT, @secret_key)
  END,
  last_error      = @last_error
WHERE
  ( session.id = @session_id )
RETURNING *;
