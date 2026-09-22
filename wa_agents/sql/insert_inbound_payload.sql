-- PARAMS:
  -- data_raw  : dict
  -- data_hash : SHA256_Hex

WITH inserted AS (
  INSERT INTO public.wa_api_inbound_payloads (
    data_raw,
    data_hash
  )
  VALUES (
    @data_raw,
    @data_hash
  )
  ON CONFLICT (data_hash)
  DO
    NOTHING
  RETURNING
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
    TRUE AS inserted
)
(
SELECT
  pay.id,
  pay.received_at,
  pay.item_index,
  pay.item_ts,
  pay.change_index,
  pay.contact,
  pay.data_raw,
  pay.data_hash,
  pay.validated,
  pay.errors,
  pay.inserted
FROM
  inserted AS pay
)
UNION ALL
(
SELECT
  pay.id,
  pay.received_at,
  pay.item_index,
  pay.item_ts,
  pay.change_index,
  pay.contact,
  pay.data_raw,
  pay.data_hash,
  pay.validated,
  pay.errors,
  FALSE AS inserted
FROM
  public.wa_api_inbound_payloads AS pay
WHERE
  ( pay.data_hash = @data_hash ) AND
  NOT EXISTS (
    SELECT
      1
    FROM
      inserted
  )
);
