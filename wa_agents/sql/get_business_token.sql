-- PARAMS:
  -- phone_number_id : str
  -- secret_key      : str | None

SELECT
  esd.business AS es_business,
  esd.connection_status,
  CASE
    WHEN ( esd.connection_status = 'connected'  ) AND
         ( NULLIF( @secret_key, '') IS NOT NULL )
    THEN decrypt_text( esd.business_token, @secret_key)
  END AS business_token
FROM
  wa_agents.api_businesses AS bus
JOIN
  wa_agents.api_phone_numbers AS num ON bus.id = num.business
LEFT JOIN
  wa_agents.api_embedded_signup_data AS esd ON bus.id = esd.business
WHERE
  ( num.phone_number_id = @phone_number_id );
