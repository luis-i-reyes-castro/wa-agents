-- PARAMS:
  -- id              : int
  -- event_type      : str | None
  -- event_name      : str | None
  -- event_version   : int | None
  -- waba_id         : str
  -- phone_number_id : str | None
  -- setup_payload   : Jsonb
  -- event_data      : Jsonb | None

UPDATE
  wa_agents.api_embedded_signup_sessions AS ses
SET
  business         = bus.id,
  session_status   = 'completed',
  code_received_at = now(),
  event_type       = @event_type,
  event_name       = @event_name,
  event_version    = @event_version,
  event_data       = @event_data,
  application_data = ses.application_data || JSONB_STRIP_NULLS(
    JSONB_BUILD_OBJECT(
      'waba_id',         @waba_id,
      'phone_number_id', @phone_number_id::TEXT,
      'setup_payload',   @setup_payload::JSONB
    )
  ),
  last_error       = NULL,
  finished_at      = now(),
  updated_at       = now()
FROM
  wa_agents.api_businesses AS bus
WHERE
  ( ses.id      = @id      ) AND
  ( bus.waba_id = @waba_id )
RETURNING
  ses.id;
