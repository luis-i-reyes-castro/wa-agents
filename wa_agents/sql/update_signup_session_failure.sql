-- PARAMS:
  -- id            : int
  -- event_type    : str | None
  -- event_name    : str | None
  -- event_version : int | None
  -- waba_id       : str | None
  -- setup_payload : Jsonb
  -- error_message : str
  -- event_data    : Jsonb | None

UPDATE
  wa_agents.api_embedded_signup_sessions AS ses
SET
  session_status   = 'failed',
  code_received_at = now(),
  event_type       = @event_type,
  event_name       = @event_name,
  event_version    = @event_version,
  event_data       = @event_data,
  application_data = ses.application_data || JSONB_BUILD_OBJECT(
    'waba_id',       @waba_id::TEXT,
    'setup_payload', @setup_payload::JSONB
  ),
  last_error       = @error_message,
  finished_at      = now(),
  updated_at       = now()
WHERE
  ses.id = @id
RETURNING
  ses.id;
