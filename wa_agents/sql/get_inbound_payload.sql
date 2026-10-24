-- PARAMS:
  -- payload_id : api_inbound_payloads.id

SELECT
  id,
  received_at,
  item_index,
  item_ts,
  change_index,
  contact,
  data_raw,
  data_hash,
  validated,
  errors
FROM
  wa_agents.api_inbound_payloads
WHERE
  ( id = @payload_id );
