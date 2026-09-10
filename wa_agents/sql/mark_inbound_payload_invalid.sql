-- PARAMS:
  -- payload_id : wa_api_inbound_payloads.id
  -- errors     : list[dict] | dict

UPDATE
  public.wa_api_inbound_payloads
SET
  validated = FALSE,
  errors    = @errors
WHERE
  ( id = @payload_id )
RETURNING
  id,
  received_at,
  data_hash,
  validated,
  errors;
