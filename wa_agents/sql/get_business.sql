-- PARAMS:
  -- business : wa_api_businesses.id

SELECT
  bus.id,
  bus.created_at,
  bus.last_seen_at,
  bus.waba_id,
  bus.phone_number_id,
  bus.display_phone_number
FROM
  public.wa_api_businesses AS bus
WHERE
  ( bus.id = @business );
