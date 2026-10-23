-- PARAMS:
  -- business : wa_api_phone_numbers.id

SELECT
  num.id,
  num.created_at,
  num.last_seen_at,
  num.business,
  bus.waba_id,
  num.phone_number_id,
  num.display_phone_number
FROM
  public.wa_api_phone_numbers AS num
JOIN
  public.wa_api_businesses AS bus ON bus.id = num.business
WHERE
  ( num.id = @business );
