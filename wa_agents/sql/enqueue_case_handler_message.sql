-- PARAMS:
  -- msg_id               : WhatsAppMessageID
  -- fallback_handler_key : str | None

INSERT INTO wa_agents.api_to_case_handler_queue (
  msg_id,
  route_by_phone_number,
  route_by_contact,
  msg_status
)
(
SELECT
  msg.msg_id,
  phn.id,
  ctc.id,
  CASE
    WHEN (
      ( phn.id IS NOT NULL ) OR
      ( ctc.id IS NOT NULL ) OR
      ( fbk.id IS NOT NULL )
    )
    THEN
      'pending'::T_ENQUEUED_MESSAGE_STATUS
    ELSE
      'ignored'::T_ENQUEUED_MESSAGE_STATUS
  END
FROM
  wa_agents.api_phone_numbers    AS num
JOIN
  wa_agents.api_contacts         AS con ON num.id = con.phone_number
LEFT JOIN
  wa_agents.case_handler_routes_by_contact      AS ctc ON con.id = ctc.contact
LEFT JOIN
  wa_agents.case_handler_routes_by_phone_number AS phn ON con.phone_number = phn.phone_number
LEFT JOIN
  wa_agents.case_handler_configurations AS fbk ON
    ( phn.id          IS NULL                       ) AND
    ( ctc.id          IS NULL                       ) AND
    ( fbk.handler_key = @fallback_handler_key::text )
JOIN
  wa_agents.api_inbound_messages AS msg ON con.id = msg.contact
WHERE
  ( msg.msg_id              = @msg_id  ) AND
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
  route_by_phone_number,
  route_by_contact,
  msg_id,
  msg_status;
