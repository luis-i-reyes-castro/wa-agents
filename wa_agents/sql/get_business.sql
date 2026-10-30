-- PARAMS:
  -- business : api_phone_numbers.id

SELECT
  num.id,
  num.created_at,
  num.last_seen_at,
  num.business,
  bus.waba_id,
  num.phone_number_id,
  num.display_phone_number
FROM
  wa_agents.api_businesses    AS bus
JOIN
  wa_agents.api_phone_numbers AS num ON bus.id = num.business
WHERE
  ( num.id = @business );
