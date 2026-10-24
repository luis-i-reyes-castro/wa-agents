-- PARAMS:
  -- case_id : case_handler_case_manifests.id

SELECT
  msg.id
FROM
  wa_agents.case_handler_messages AS msg
WHERE
  ( msg.case_id = @case_id )
ORDER BY
  msg.message_index ASC;
