-- PARAMS:
  -- waba_id                : NumericID
  -- public_key             : PEM public key
  -- public_key_fingerprint : SHA-256 hex digest
  -- private_key            : PEM private key
  -- secret_key             : ENCRYPTION_KEY

INSERT INTO wa_agents.api_flow_encryption_keys AS fw (
  business,
  public_key,
  public_key_fingerprint,
  private_key_encrypted
)
SELECT
  bus.id,
  @public_key,
  @public_key_fingerprint,
  encrypt_text( @private_key, @secret_key)
FROM
  wa_agents.api_businesses AS bus
WHERE
  ( bus.waba_id = @waba_id )
ON CONFLICT (business)
DO
  UPDATE
SET
  updated_at             = now(),
  public_key             = EXCLUDED.public_key,
  public_key_fingerprint = EXCLUDED.public_key_fingerprint,
  private_key_encrypted  = EXCLUDED.private_key_encrypted
RETURNING
  fw.id,
  fw.business,
  @waba_id AS waba_id,
  fw.public_key,
  fw.public_key_fingerprint;
