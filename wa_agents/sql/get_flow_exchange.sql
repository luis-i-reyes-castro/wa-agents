-- PARAMS:
  -- session_id  : wa_api_flow_sessions.id
  -- request_hash : SHA-256 hex digest
  -- secret_key  : ENCRYPTION_KEY

SELECT
  exchange.*,
  CASE
    WHEN exchange.response_data IS NULL THEN NULL
    ELSE decrypt_text( exchange.response_data, @secret_key)
  END AS response_data_decrypted
FROM
  public.wa_api_flow_data_exchanges AS exchange
WHERE
  ( exchange.session = @session_id ) AND
  ( exchange.request_hash = @request_hash );
