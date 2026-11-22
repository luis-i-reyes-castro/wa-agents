-- PARAMS:
  -- waba_id            : str
  -- status             : str
  -- webhook_subscribed : bool
  -- business_token     : str
  -- secret_key         : str
  -- last_error         : str | None

WITH CTE_business AS (
  INSERT INTO wa_agents.api_businesses AS bus (
    waba_id
  )
  VALUES (
    @waba_id
  )
  ON CONFLICT ( waba_id)
  DO
    UPDATE
  SET
    last_seen_at = now()
  RETURNING
    bus.id,
    bus.waba_id
),
CTE_signup_data AS (
  INSERT INTO wa_agents.api_embedded_signup_data AS esd (
    business,
    business_token,
    connection_status,
    app_subscribed,
    subscribed_at,
    disconnected_at,
    last_error
  )
  SELECT
    bus.id,
    wa_agents.encrypt_text( @business_token, @secret_key ),
    @status::wa_agents.T_WHATSAPP_CONNECTION_STATUS,
    @webhook_subscribed,
    CASE WHEN @webhook_subscribed THEN now() ELSE NULL END,
    CASE
      WHEN @status::wa_agents.T_WHATSAPP_CONNECTION_STATUS = 'disconnected'
      THEN now()
      ELSE NULL
    END,
    @last_error
  FROM
    CTE_business AS bus
  ON CONFLICT ( business)
  DO
    UPDATE
  SET
    business_token      = EXCLUDED.business_token,
    connection_status   = EXCLUDED.connection_status,
    token_last_refreshed = now(),
    app_subscribed       = EXCLUDED.app_subscribed,
    subscribed_at       = CASE
      WHEN EXCLUDED.app_subscribed
      THEN COALESCE( esd.subscribed_at, now())
      ELSE NULL
    END,
    disconnected_at     = EXCLUDED.disconnected_at,
    last_error          = EXCLUDED.last_error,
    updated_at          = now()
  RETURNING
    esd.business,
    esd.connection_status,
    esd.app_subscribed,
    esd.last_error
)
SELECT
  bus.id                AS id,
  bus.waba_id           AS waba_id,
  esd.connection_status AS status,
  esd.app_subscribed    AS webhook_subscribed,
  esd.last_error        AS last_error
FROM
  CTE_business AS bus
JOIN
  CTE_signup_data AS esd
ON
  bus.id = esd.business;
