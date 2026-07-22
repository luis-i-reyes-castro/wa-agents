-- PARAMS:
  -- row_id     : int
  -- last_error : str

UPDATE
  wa_incoming_queue
SET
  status     = 'error',
  updated_at = now(),
  last_error = @last_error
WHERE
  ( id = @row_id );
