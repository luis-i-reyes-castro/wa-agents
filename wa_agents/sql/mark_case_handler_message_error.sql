-- PARAMS:
  -- row_id      : api_to_case_handler_queue.id
  -- owner_token : UUID

WITH CTE_lease AS MATERIALIZED (
  SELECT
    lea.contact
  FROM
    wa_agents.api_phone_numbers           AS num
  JOIN
    wa_agents.api_contacts                AS con ON num.id = con.phone_number
  JOIN
    wa_agents.api_inbound_messages        AS msg ON con.id = msg.contact
  JOIN
    wa_agents.api_to_case_handler_queue   AS que ON msg.msg_id = que.msg_id
  JOIN
    wa_agents.case_handler_contact_leases AS lea ON con.id = lea.contact
  WHERE
    ( que.id                  = @row_id           ) AND
    ( lea.owner_token         = @owner_token      ) AND
    ( lea.expires_at          > clock_timestamp() ) AND
    ( num.registration_status = 'active'          )
  FOR UPDATE OF lea
)
UPDATE
  wa_agents.api_to_case_handler_queue
SET
  msg_status    = 'error',
  updated_at    = now(),
  last_error_at = now(),
  attempts      = GREATEST( attempts, 1),
  -- Cap the exponent too, so large retry budgets cannot overflow power().
  available_at  = clock_timestamp() +
    LEAST(
      300,
      30 * power( 2, LEAST( 4, GREATEST( attempts - 1, 0)))
    ) * INTERVAL '1 second'
WHERE
  ( id = @row_id ) AND
  EXISTS (
    SELECT
      1
    FROM
      CTE_lease
  ) AND
  (
    ( ingested_at     IS NULL     ) OR
    ( response_due_at IS NOT NULL )
  )
RETURNING
  id,
  msg_id,
  msg_status,
  updated_at,
  last_error_at;
