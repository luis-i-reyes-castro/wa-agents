UPDATE
  public.wa_api_to_case_handler_queue AS que
SET
  msg_status = 'processing',
  updated_at = now()
WHERE
  que.id = (
    SELECT
      sub.id
    FROM
      public.wa_api_to_case_handler_queue AS sub
    WHERE
      ( sub.msg_status = 'pending' )
    ORDER BY
      sub.created_at ASC,
      sub.id         ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
  )
RETURNING
  que.id         AS row_id,
  que.msg_id     AS msg_id,
  que.msg_status AS msg_status;
