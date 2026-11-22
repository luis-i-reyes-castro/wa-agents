-- PARAMS:
  -- id_biz        : int
  -- session_state : str

SELECT
  ses.id                                      AS id,
  ( ses.application_data->>'id_biz' )::BIGINT AS id_biz,
  ses.application_data->>'id_user'            AS id_user,
  ses.session_state                        AS session_state,
  ses.session_status                          AS status,
  ses.code_received_at                        AS code_received_at,
  ses.event_type                              AS event_type,
  ses.event_name                              AS event_name,
  ses.event_version                           AS event_version,
  COALESCE(
    bus.waba_id,
    ses.application_data->>'waba_id'
  )                                           AS waba_id,
  ses.application_data->'setup_payload'       AS setup_payload,
  ses.last_error                              AS error_message,
  ses.expires_at                              AS expires_at,
  ses.finished_at                             AS completed_at,
  ses.created_at                              AS created_at,
  ses.updated_at                              AS updated_at
FROM
  wa_agents.api_embedded_signup_sessions AS ses
LEFT JOIN
  wa_agents.api_businesses           AS bus
ON
  bus.id = ses.business
WHERE
  (
    ses.application_data @> JSONB_BUILD_OBJECT(
      'id_biz',
      @id_biz::BIGINT
    )
  ) AND
  ( ses.session_state = @session_state )
ORDER BY
  ses.id DESC
LIMIT 1;
