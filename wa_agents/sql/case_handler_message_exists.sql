-- PARAMS:
  -- api_inbound_msg_id : api_inbound_messages.id

SELECT
  1
FROM
  wa_agents.case_handler_to_api_bridge
WHERE
  ( api_inbound_msg_id = @api_inbound_msg_id )
LIMIT 1;
