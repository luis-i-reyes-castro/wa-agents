-- PARAMS:
  -- case_id    : case_handler_case_manifests.id
  -- message_id : case_handler_messages.id

SELECT
  msg.id,
  msg.ts,
  msg.case_id,
  msg.message_index,
  msg.basemodel,
  msg.origin,
  msg.data
FROM
  wa_agents.case_handler_messages AS msg
WHERE
  ( msg.case_id = @case_id    ) AND
  ( msg.id      = @message_id );
