-- PARAMS:
  -- payload_id : api_inbound_payloads.id
  -- errors     : list[dict] | dict

UPDATE
  wa_agents.api_inbound_payloads
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
