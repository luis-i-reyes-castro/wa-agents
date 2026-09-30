-- PARAMS:
  -- waba_id    : NumericID
  -- secret_key : ENCRYPTION_KEY

SELECT
  fw.id,
  fw.waba_id,
  fw.public_key,
  fw.public_key_fingerprint,
  decrypt_text( fw.private_key_encrypted, @secret_key) AS private_key
FROM
  public.wa_api_flow_wabas AS fw
WHERE
  ( fw.waba_id = @waba_id );
