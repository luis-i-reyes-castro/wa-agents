-- PARAMS:
  -- case_id : wa_case_handler_case_manifests.id

SELECT
  msg.id
FROM
  public.wa_case_handler_messages AS msg
WHERE
  ( msg.case_id = @case_id )
ORDER BY
  msg.message_index ASC;
