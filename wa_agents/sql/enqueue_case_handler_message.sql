-- PARAMS:
  -- msg_id : WhatsAppMessageID

INSERT INTO wa_agents.api_to_case_handler_queue (
  msg_id,
  handler_id
)
(
SELECT
  msg.msg_id,
  rou.id
FROM
  wa_agents.api_inbound_messages AS msg
JOIN
  wa_agents.api_contacts         AS con ON con.id = msg.contact
JOIN
  wa_agents.api_phone_numbers    AS num ON num.id = con.phone_number
LEFT JOIN LATERAL (
  SELECT
    rte.id
  FROM
    wa_agents.case_handler_routes AS rte
  WHERE
    ( rte.phone_number = con.phone_number ) AND
    (
      ( rte.contact = con.id ) OR
      ( rte.contact IS NULL  )
    )
  ORDER BY
    ( rte.contact IS NOT NULL ) DESC
  LIMIT 1
) AS rou ON TRUE
WHERE
  ( msg.msg_id              = @msg_id ) AND
  ( num.registration_status = 'active' )
)
ON CONFLICT
  (msg_id)
DO
  NOTHING
RETURNING
  id,
  created_at,
  updated_at,
  last_error_at,
  handler_id,
  msg_id,
  msg_status;
