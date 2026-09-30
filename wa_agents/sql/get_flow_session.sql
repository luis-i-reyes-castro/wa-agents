-- PARAMS:
  -- flow_token_hash : SHA-256 hex digest
  -- secret_key      : ENCRYPTION_KEY

SELECT
  session.*,
  flow.flow_id,
  flow.flow_key,
  flow.handler_key,
  flow.data_api_version,
  decrypt_text(
    session.flow_token_encrypted,
    @secret_key
  ) AS flow_token,
  CASE
    WHEN session.session_state IS NULL THEN NULL
    ELSE decrypt_text( session.session_state, @secret_key)
  END AS session_state_data
FROM
  public.wa_api_flow_sessions AS session
INNER JOIN
  public.wa_api_flows AS flow
  ON ( flow.id = session.flow )
WHERE
  ( session.flow_token_hash = @flow_token_hash );
