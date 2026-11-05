-- PARAMS:
  -- queue_row_id : api_to_case_handler_queue.id
  -- owner_token  : UUID

SELECT
  lea.contact
FROM
  wa_agents.api_inbound_messages        AS msg
JOIN
  wa_agents.api_to_case_handler_queue   AS que ON msg.msg_id  = que.msg_id
JOIN
  wa_agents.case_handler_contact_leases AS lea ON msg.contact = lea.contact
WHERE
  ( que.id          = @queue_row_id     ) AND
  ( lea.owner_token = @owner_token      ) AND
  ( lea.expires_at  > clock_timestamp() )
FOR UPDATE OF lea;
