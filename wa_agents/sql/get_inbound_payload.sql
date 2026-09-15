-- PARAMS:
  -- payload_id : wa_api_inbound_payloads.id

SELECT
  id,
  received_at,
  item_idx,
  item_ts,
  change_idx,
  contact,
  data_raw,
  data_hash,
  validated,
  errors
FROM
  public.wa_api_inbound_payloads
WHERE
  ( id = @payload_id );
