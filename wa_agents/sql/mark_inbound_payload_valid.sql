-- PARAMS:
  -- payload_id : api_inbound_payloads.id

UPDATE
  wa_agents.api_inbound_payloads
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
