-- PARAMS:
  -- waba_id              : NumericID
  -- phone_number_id      : NumericID
  -- display_phone_number : NumericID

WITH business AS (
  INSERT INTO public.wa_api_businesses AS bus (
    waba_id
  )
  VALUES (
    @waba_id
  )
  ON CONFLICT (waba_id)
  DO
    UPDATE
  SET
    last_seen_at = now()
  RETURNING
    bus.id,
    bus.waba_id
),
phone_number AS (
  INSERT INTO public.wa_api_phone_numbers AS num (
    business,
    display_phone_number,
    phone_number_id
  )
  SELECT
    bus.id,
    @display_phone_number,
    @phone_number_id
  FROM
    business AS bus
  -- Meta can issue a new phone_number_id when the same number reconnects.
  ON CONFLICT (display_phone_number)
  DO
    UPDATE
  SET
    business        = EXCLUDED.business,
    phone_number_id = EXCLUDED.phone_number_id,
    last_seen_at    = now()
  RETURNING
    num.id,
    num.created_at,
    num.last_seen_at,
    num.business,
    num.phone_number_id,
    num.display_phone_number
)
SELECT
  num.id,
  num.created_at,
  num.last_seen_at,
  num.business,
  bus.waba_id,
  num.phone_number_id,
  num.display_phone_number
FROM
  phone_number AS num
JOIN
  business AS bus ON bus.id = num.business;
