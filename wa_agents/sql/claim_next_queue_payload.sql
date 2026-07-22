UPDATE
  wa_incoming_queue AS que
SET
  status     = 'processing',
  updated_at = now(),
  last_error = NULL
WHERE
  que.id = (
    SELECT
      sub.id
    FROM
      wa_incoming_queue AS sub
    WHERE
      ( sub.status = 'pending' )
    ORDER BY
      sub.created_at ASC,
      sub.id         ASC
    LIMIT
      1
    FOR UPDATE SKIP LOCKED
  )
RETURNING
  que.id      AS row_id,
  que.payload AS payload;
