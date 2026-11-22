-- PARAMS:
  -- id_biz        : int
  -- id_user       : UUID
  -- session_state : str
  -- expires_at    : datetime

INSERT INTO wa_agents.api_embedded_signup_sessions (
  session_state,
  expires_at,
  application_data
)
VALUES (
  @session_state,
  @expires_at,
  JSONB_BUILD_OBJECT(
    'id_biz',  @id_biz::BIGINT,
    'id_user', @id_user::TEXT
  )
)
RETURNING
  id,
  session_state,
  expires_at;
