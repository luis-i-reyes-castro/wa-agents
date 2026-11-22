-- PARAMS:
  -- row_id          : api_to_case_handler_queue.id
  -- owner_token     : UUID
  -- ingested        : bool, completing ingestion rather than response
  -- response_due_at : datetime | None

WITH CTE_lease AS MATERIALIZED (
  SELECT
    lea.contact
  FROM
    wa_agents.api_phone_numbers           AS num
  JOIN
    wa_agents.api_contacts                AS con ON num.id      = con.phone_number
  JOIN
    wa_agents.case_handler_contact_leases AS lea ON con.id      = lea.contact
  JOIN
    wa_agents.api_inbound_messages        AS msg ON lea.contact = msg.contact
  JOIN
    wa_agents.api_to_case_handler_queue   AS que ON msg.msg_id  = que.msg_id
  WHERE
    ( que.id                  = @row_id           ) AND
    ( lea.owner_token         = @owner_token      ) AND
    ( lea.expires_at          > clock_timestamp() ) AND
    ( num.registration_status = 'active'          )
  FOR UPDATE OF lea
),
CTE_superseded AS (
  UPDATE
    wa_agents.api_to_case_handler_queue AS que
  SET
    response_due_at = NULL,
    updated_at      = now(),
    msg_status      = 'done'
  FROM
    CTE_lease                     AS lea
  JOIN
    wa_agents.api_inbound_messages AS msg ON lea.contact = msg.contact
  WHERE
    @ingested AND
    ( que.msg_id           = msg.msg_id ) AND
    ( que.id              <> @row_id    ) AND
    ( que.response_due_at IS NOT NULL   )
  RETURNING
    que.id
)
UPDATE
  wa_agents.api_to_case_handler_queue
SET
  msg_status      = 'done',
  updated_at      = now(),
  ingested_at     = CASE
    WHEN @ingested THEN now()
    ELSE ingested_at
  END,
  response_due_at = @response_due_at::TIMESTAMPTZ,
  attempts        = CASE
    WHEN @ingested THEN 0
    ELSE attempts
  END,
  available_at    = now()
WHERE
  ( id = @row_id ) AND
  EXISTS (
    SELECT
      1
    FROM
      CTE_lease
  )
RETURNING
  id,
  msg_id,
  msg_status,
  updated_at;
