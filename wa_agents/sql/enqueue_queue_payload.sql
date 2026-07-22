-- PARAMS:
  -- payload_hash : str
  -- payload      : dict

INSERT INTO wa_incoming_queue (
  payload_hash,
  payload
)
VALUES (
  @payload_hash,
  @payload
)
ON CONFLICT
  (payload_hash)
DO
  NOTHING
RETURNING
  id;
