-- PARAMS:
  -- row_id : wa_api_to_case_handler_queue.id

UPDATE
  public.wa_api_to_case_handler_queue
SET
  msg_status = 'done',
  updated_at = now()
WHERE
  ( id = @row_id )
RETURNING
  id,
  msg_id,
  msg_status,
  updated_at;
