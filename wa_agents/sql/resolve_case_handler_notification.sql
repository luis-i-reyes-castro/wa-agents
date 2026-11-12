-- PARAMS:
  -- id             : case_handler_notifications.id
  -- inbound_msg_id : api_inbound_messages.id

UPDATE
  wa_agents.case_handler_notifications AS ntf
SET
  resolved_at    = now(),
  inbound_msg_id = @inbound_msg_id
WHERE
  ( ntf.id          = @id ) AND
  ( ntf.resolved_at IS NULL )
RETURNING
  ntf.id             AS id,
  ntf.resolved_at    AS resolved_at,
  ntf.inbound_msg_id AS inbound_msg_id;
