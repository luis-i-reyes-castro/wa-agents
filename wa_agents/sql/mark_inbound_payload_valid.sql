-- PARAMS:
  -- payload_id : wa_api_inbound_payloads.id

UPDATE
  public.wa_api_inbound_payloads
SET
  validated = TRUE,
  errors    = NULL
WHERE
  ( id = @payload_id )
RETURNING
  id,
  received_at,
  data_hash,
  validated,
  errors;
