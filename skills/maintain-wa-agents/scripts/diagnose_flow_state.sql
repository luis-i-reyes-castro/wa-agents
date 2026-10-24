-- Read-only WhatsApp Flow diagnostic for PostgreSQL/psql.
--
-- Reads ENCRYPTION_KEY from the client environment. Decrypted output is sensitive.
-- Usage:
--   # ENCRYPTION_KEY must already be exported in the client environment.
--   psql "$DATABASE_URL" \
--     -v business_display_phone_number=593962939146 \
--     -v contact_wa_id=593995341161 \
--     -f skills/maintain-wa-agents/scripts/diagnose_flow_state.sql

\set ON_ERROR_STOP on

\if :{?business_display_phone_number}
\else
  \echo 'Missing required psql variable: business_display_phone_number'
  \quit
\endif

\if :{?contact_wa_id}
\else
  \echo 'Missing required psql variable: contact_wa_id'
  \quit
\endif

\getenv encryption_key ENCRYPTION_KEY
\if :{?encryption_key}
\else
  \echo 'Missing required environment variable: ENCRYPTION_KEY'
  \quit
\endif

BEGIN TRANSACTION READ ONLY;

-- Every Flow session for the contact, including launch/completion links and state.
WITH target_contact AS (
  SELECT
    con.id                   AS contact_id,
    con.wa_id                AS contact_wa_id,
    bus.id                   AS business_id,
    bus.waba_id              AS waba_id,
    phn.id                   AS phone_number_id,
    phn.display_phone_number AS business_phone_number
  FROM
    wa_agents.api_businesses AS bus
  JOIN
    wa_agents.api_phone_numbers AS phn ON phn.business = bus.id
  JOIN
    wa_agents.api_contacts AS con ON con.phone_number = phn.id
  WHERE
    ( phn.display_phone_number = :'business_display_phone_number' ) AND
    ( con.wa_id                = :'contact_wa_id'                  )
)
SELECT
  tar.business_id,
  tar.business_phone_number,
  tar.contact_id,
  tar.contact_wa_id,
  ses.id                       AS flow_session_id,
  ses.created_at               AS session_created_at,
  ses.updated_at               AS session_updated_at,
  ses.expires_at               AS session_expires_at,
  ses.activated_at             AS session_activated_at,
  ses.completed_at             AS session_completed_at,
  ses.session_status           AS session_status,
  flo.id                       AS flow_definition_id,
  flo.flow_id                  AS meta_flow_id,
  flo.flow_key                 AS flow_key,
  flo.flow_name                AS flow_name,
  flo.handler_key              AS handler_key,
  flo.flow_status              AS flow_status,
  flo.flow_json_version        AS flow_json_version,
  flo.data_api_version         AS data_api_version,
  flo.endpoint_uri             AS endpoint_uri,
  tar.waba_id                  AS waba_id,
  ses.flow_token_hash          AS flow_token_hash,
  ses.case_handler_message     AS launch_case_message_id,
  ( launch_case.data - 'flow_token' ) AS launch_case_message_data,
  ses.outbound_message         AS launch_api_message_id,
  launch_api.msg_id            AS launch_whatsapp_message_id,
  launch_api.sent_at           AS launch_message_at,
  jsonb_set(
    launch_api.msg_data,
    '{interactive,action,parameters,flow_token}',
    '"[REDACTED]"'::JSONB,
    FALSE
  )                            AS launch_api_message_data,
  ses.completion_message       AS completion_api_message_id,
  completion.msg_id            AS completion_whatsapp_message_id,
  completion.msg_ts            AS completion_message_at,
  CASE
    WHEN ses.session_state IS NULL THEN NULL
    ELSE decrypt_text( ses.session_state, :'encryption_key')::JSONB
  END                          AS session_state,
  CASE
    WHEN ses.completion_data IS NULL THEN NULL
    ELSE (
      decrypt_text( ses.completion_data, :'encryption_key')::JSONB - 'flow_token'
    )
  END                          AS completion_data,
  ses.last_error               AS last_error
FROM
  target_contact AS tar
JOIN
  wa_agents.api_flow_sessions AS ses ON
    ( ses.phone_number = tar.phone_number_id ) AND
    ( ses.contact      = tar.contact_id      )
JOIN
  wa_agents.api_flows AS flo ON flo.id = ses.flow
JOIN
  wa_agents.api_flow_encryption_keys AS wba ON wba.id = flo.flow_waba
LEFT JOIN
  wa_agents.case_handler_messages AS launch_case
  ON launch_case.id = ses.case_handler_message
LEFT JOIN
  wa_agents.api_outbound_messages AS launch_api
  ON launch_api.id = ses.outbound_message
LEFT JOIN
  wa_agents.api_inbound_messages AS completion
  ON completion.id = ses.completion_message
ORDER BY
  ses.created_at ASC,
  ses.id         ASC;

-- Every data exchange for those sessions, including decrypted request/response data.
WITH target_sessions AS (
  SELECT
    ses.id,
    ses.created_at,
    ses.session_status,
    flo.flow_id,
    flo.flow_key
  FROM
    wa_agents.api_businesses AS bus
  JOIN
    wa_agents.api_phone_numbers AS phn ON phn.business = bus.id
  JOIN
    wa_agents.api_contacts AS con ON con.phone_number = phn.id
  JOIN
    wa_agents.api_flow_sessions AS ses ON
      ( ses.phone_number = phn.id ) AND
      ( ses.contact      = con.id )
  JOIN
    wa_agents.api_flows AS flo ON flo.id = ses.flow
  WHERE
    ( phn.display_phone_number = :'business_display_phone_number' ) AND
    ( con.wa_id                = :'contact_wa_id'                  )
)
SELECT
  ses.id                       AS flow_session_id,
  ses.created_at               AS session_created_at,
  ses.session_status           AS session_status,
  ses.flow_id                  AS meta_flow_id,
  ses.flow_key                 AS flow_key,
  exc.id                       AS flow_exchange_id,
  exc.created_at               AS exchange_created_at,
  exc.completed_at             AS exchange_completed_at,
  exc.request_hash             AS request_hash,
  exc.request_action           AS request_action,
  exc.request_screen           AS request_screen,
  (
    decrypt_text(
      exc.request_data,
      :'encryption_key'
    )::JSONB - 'flow_token'
  )                             AS request_data,
  CASE
    WHEN exc.response_data IS NULL THEN NULL
    ELSE (
      decrypt_text( exc.response_data, :'encryption_key')::JSONB - 'flow_token'
    )
  END                          AS response_data,
  exc.exchange_status          AS exchange_status,
  exc.http_status              AS http_status,
  exc.latency_ms               AS latency_ms,
  exc.error_code               AS error_code,
  exc.error_message            AS error_message
FROM
  target_sessions AS ses
JOIN
  wa_agents.api_flow_data_exchanges AS exc ON exc.session = ses.id
ORDER BY
  ses.created_at ASC,
  ses.id         ASC,
  exc.created_at ASC,
  exc.id         ASC;

ROLLBACK;
