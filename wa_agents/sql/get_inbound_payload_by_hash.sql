-- PARAMS:
  -- data_hash : SHA256_Hex

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
  errors,
  FALSE AS inserted
FROM
  public.wa_api_inbound_payloads
WHERE
  data_hash = @data_hash;
