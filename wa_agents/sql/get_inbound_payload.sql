-- PARAMS:
  -- payload_id : wa_api_inbound_payloads.id

SELECT
  id,
  received_at,
  data_raw,
  data_hash,
  validated,
  errors
FROM
  public.wa_api_inbound_payloads
WHERE
  ( id = @payload_id );
