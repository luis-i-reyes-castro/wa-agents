-- PARAMS:
  -- case_id : wa_case_handler_case_manifests.id

SELECT
  msg.id,
  msg.ts,
  msg.case_id,
  msg.message_index,
  msg.basemodel,
  msg.origin,
  msg.data
FROM
  public.wa_case_handler_messages AS msg
WHERE
  ( msg.case_id = @case_id )
ORDER BY
  msg.message_index ASC;
