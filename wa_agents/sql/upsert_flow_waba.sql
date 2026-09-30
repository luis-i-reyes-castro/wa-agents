-- PARAMS:
  -- waba_id                : NumericID
  -- public_key             : PEM public key
  -- public_key_fingerprint : SHA-256 hex digest
  -- private_key            : PEM private key
  -- secret_key             : ENCRYPTION_KEY

INSERT INTO public.wa_api_flow_wabas (
  waba_id,
  public_key,
  public_key_fingerprint,
  private_key_encrypted
)
VALUES (
  @waba_id,
  @public_key,
  @public_key_fingerprint,
  encrypt_text( @private_key, @secret_key)
)
ON CONFLICT (waba_id)
DO
  UPDATE
SET
  updated_at             = now(),
  public_key             = EXCLUDED.public_key,
  public_key_fingerprint = EXCLUDED.public_key_fingerprint,
  private_key_encrypted  = EXCLUDED.private_key_encrypted
RETURNING
  id,
  waba_id,
  public_key,
  public_key_fingerprint;
