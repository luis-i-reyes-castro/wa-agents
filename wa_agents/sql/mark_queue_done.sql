-- PARAMS:
  -- row_id : int

UPDATE
  wa_incoming_queue
SET
  status     = 'done',
  updated_at = now()
WHERE
  ( id = @row_id );
