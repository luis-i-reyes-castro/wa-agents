-- PARAMS: None

SELECT
  bus.waba_id              AS waba_id,
  num.phone_number_id      AS operator_id,
  num.display_phone_number AS display_phone_number
FROM
  wa_agents.api_businesses    AS bus
JOIN
  wa_agents.api_phone_numbers AS num ON bus.id = num.business
ORDER BY
  num.last_seen_at DESC,
  num.display_phone_number,
  num.phone_number_id;
