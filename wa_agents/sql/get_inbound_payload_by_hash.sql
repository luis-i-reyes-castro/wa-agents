-- PARAMS:
  -- data_hash : SHA256_Hex

SELECT
  id           AS id,
  received_at  AS received_at,
  item_index   AS item_index,
  item_ts      AS item_ts,
  change_index AS change_index,
  contact      AS contact,
  data_raw     AS data_raw,
  data_hash    AS data_hash,
  validated    AS validated,
  errors       AS errors,
  FALSE        AS inserted
FROM
  wa_agents.api_inbound_payloads
WHERE
  ( data_hash = @data_hash );
