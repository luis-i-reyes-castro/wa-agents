-- PARAMS:
  -- exchange_id    : wa_api_flow_data_exchanges.id
  -- response_data  : canonical JSON string | null
  -- exchange_status : T_WHATSAPP_FLOW_EXCHANGE_STATUS
  -- http_status    : HTTP response status
  -- latency_ms     : processing time in milliseconds
  -- error_code     : sanitized error code | null
  -- error_message  : sanitized error message | null
  -- secret_key     : ENCRYPTION_KEY

UPDATE
  public.wa_api_flow_data_exchanges AS exchange
SET
  completed_at    = now(),
  response_data   = CASE
    WHEN @response_data::TEXT IS NULL THEN NULL
    ELSE encrypt_text( @response_data::TEXT, @secret_key)
  END,
  exchange_status = @exchange_status::T_WHATSAPP_FLOW_EXCHANGE_STATUS,
  http_status     = @http_status,
  latency_ms      = @latency_ms,
  error_code      = @error_code,
  error_message   = @error_message
WHERE
  ( exchange.id = @exchange_id )
RETURNING *;
