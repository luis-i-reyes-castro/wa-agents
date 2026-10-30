-- PARAMS:
  -- waba_id    : NumericID
  -- secret_key : ENCRYPTION_KEY

SELECT
  fw.id,
  fw.business,
  bus.waba_id,
  fw.public_key,
  fw.public_key_fingerprint,
  decrypt_text( fw.private_key_encrypted, @secret_key) AS private_key
FROM
  wa_agents.api_businesses           AS bus
JOIN
  wa_agents.api_flow_encryption_keys AS fw ON bus.id = fw.business
WHERE
  ( bus.waba_id = @waba_id );
