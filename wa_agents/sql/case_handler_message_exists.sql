-- PARAMS:
  -- api_inbound_msg_id : wa_api_inbound_messages.id

SELECT
  1
FROM
  public.wa_case_handler_to_api
WHERE
  ( api_inbound_msg_id = @api_inbound_msg_id )
LIMIT 1;
