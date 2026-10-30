-- PARAMS:
  -- session_id  : api_flow_sessions.id
  -- request_hash : SHA-256 hex digest
  -- secret_key  : ENCRYPTION_KEY

SELECT
  exchange.id              AS id,
  exchange.created_at      AS created_at,
  exchange.completed_at    AS completed_at,
  exchange.flow_waba       AS flow_waba,
  exchange.session         AS session,
  exchange.request_hash    AS request_hash,
  exchange.request_action  AS request_action,
  exchange.request_screen  AS request_screen,
  exchange.request_data    AS request_data,
  exchange.response_data   AS response_data,
  exchange.exchange_status AS exchange_status,
  exchange.http_status     AS http_status,
  exchange.latency_ms      AS latency_ms,
  exchange.error_code      AS error_code,
  exchange.error_message   AS error_message,
  CASE
    WHEN exchange.response_data IS NULL THEN NULL
    ELSE decrypt_text( exchange.response_data, @secret_key)
  END                      AS response_data_decrypted
FROM
  wa_agents.api_flow_data_exchanges AS exchange
WHERE
  ( exchange.session      = @session_id   ) AND
  ( exchange.request_hash = @request_hash );
