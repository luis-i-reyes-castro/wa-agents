-- PARAMS:
  -- payload_hash : str
  -- object_type  : str
  -- payload      : dict

WITH inserted AS (
  INSERT INTO wa_webhook_payloads (
    payload_hash,
    object_type,
    payload
  )
  VALUES (
    @payload_hash,
    @object_type,
    @payload
  )
  ON CONFLICT
    (payload_hash)
  DO
    NOTHING
  RETURNING
    id,
    TRUE AS inserted
)
SELECT
  ins.id       AS id,
  ins.inserted AS inserted
FROM
  inserted AS ins

UNION ALL

SELECT
  whp.id    AS id,
  FALSE     AS inserted
FROM
  wa_webhook_payloads AS whp
WHERE
  ( whp.payload_hash = @payload_hash ) AND
  NOT EXISTS (
    SELECT
      1
    FROM
      inserted
  );
