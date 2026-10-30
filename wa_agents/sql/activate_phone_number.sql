-- PARAMS:
  -- phone_number_id : str

-- Trusted onboarding only. Serialize switches for the same telephone number.
WITH registrations AS MATERIALIZED (
  SELECT
    num.*
  FROM
    wa_agents.api_phone_numbers AS num
  WHERE
    num.display_phone_number = (
      SELECT
        target.display_phone_number
      FROM
        wa_agents.api_phone_numbers AS target
      WHERE
        ( target.phone_number_id = @phone_number_id )
    )
  ORDER BY
    num.id
  FOR UPDATE
),
target AS MATERIALIZED (
  SELECT
    num.*
  FROM
    registrations AS num
  WHERE
    ( num.phone_number_id     = @phone_number_id         ) AND
    ( num.registration_status IN ( 'pending', 'active' ) ) AND
    NOT EXISTS (
      SELECT
        1
      FROM
        registrations                         AS other
      JOIN
        wa_agents.api_contacts                AS contact ON
          other.id   = contact.phone_number
      JOIN
        wa_agents.case_handler_contact_leases AS lease   ON
          contact.id = lease.contact
      WHERE
        ( lease.expires_at > now() )
    )
),
retired AS (
  UPDATE
    wa_agents.api_phone_numbers AS num
  SET
    registration_status = 'retired'
  FROM
    target
  WHERE
    ( num.display_phone_number = target.display_phone_number ) AND
    ( num.id                  <> target.id                   ) AND
    ( num.registration_status = 'active'                     )
  RETURNING
    num.id
),
stopped_queue AS (
  UPDATE
    wa_agents.api_to_case_handler_queue AS queue
  SET
    msg_status    = 'error',
    updated_at    = now(),
    last_error_at = now()
  FROM
    retired
  JOIN
    wa_agents.api_contacts         AS contact ON retired.id = contact.phone_number
  JOIN
    wa_agents.api_inbound_messages AS msg     ON contact.id = msg.contact
  WHERE
    ( queue.msg_id      = msg.msg_id                  ) AND
    ( queue.msg_status IN ( 'pending', 'processing' ) )
  RETURNING
    queue.id
),
closed_cases AS (
  UPDATE
    wa_agents.case_handler_case_manifests AS manifest
  SET
    is_open    = FALSE,
    updated_at = now()
  FROM
    retired
  JOIN
    wa_agents.api_contacts AS contact ON retired.id = contact.phone_number
  WHERE
    ( manifest.contact = contact.id ) AND
    ( manifest.is_open              )
  RETURNING
    manifest.id
),
stopped_flows AS (
  UPDATE
    wa_agents.api_flow_sessions AS session
  SET
    session_status = 'superseded',
    updated_at     = now()
  FROM
    retired
  WHERE
    ( session.phone_number = retired.id                         ) AND
    ( session.session_status IN ( 'created', 'sent', 'active' ) )
  RETURNING
    session.id
)
UPDATE
  wa_agents.api_phone_numbers AS num
SET
  registration_status = 'active'
FROM
  target
WHERE
  ( num.id = target.id ) AND
  -- Force retirement before checking the partial unique index on activation.
  ( ( SELECT count(*) FROM retired ) >= 0 )
RETURNING
  num.id,
  num.phone_number_id,
  num.registration_status;
