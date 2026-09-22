-- PARAMS:
  -- waba_id              : NumericID
  -- phone_number_id      : NumericID
  -- display_phone_number : NumericID

WITH updated AS (
  UPDATE
    public.wa_api_businesses AS bus
  SET
    waba_id              = @waba_id,
    display_phone_number = @display_phone_number,
    last_seen_at         = now()
  WHERE
    ( bus.phone_number_id = @phone_number_id )
  RETURNING
    bus.id,
    bus.created_at,
    bus.last_seen_at,
    bus.waba_id,
    bus.phone_number_id,
    bus.display_phone_number
),
inserted AS (
  INSERT INTO public.wa_api_businesses (
    waba_id,
    phone_number_id,
    display_phone_number
  )
  SELECT
    @waba_id,
    @phone_number_id,
    @display_phone_number
  WHERE NOT EXISTS (
    SELECT
      1
    FROM
      updated
  )
  ON CONFLICT (phone_number_id)
  DO
    UPDATE
  SET
    waba_id              = EXCLUDED.waba_id,
    display_phone_number = EXCLUDED.display_phone_number,
    last_seen_at         = now()
  RETURNING
    id,
    created_at,
    last_seen_at,
    waba_id,
    phone_number_id,
    display_phone_number
)
(
SELECT
  upd.id,
  upd.created_at,
  upd.last_seen_at,
  upd.waba_id,
  upd.phone_number_id,
  upd.display_phone_number
FROM
  updated AS upd
)
UNION ALL
(
SELECT
  ins.id,
  ins.created_at,
  ins.last_seen_at,
  ins.waba_id,
  ins.phone_number_id,
  ins.display_phone_number
FROM
  inserted AS ins
LIMIT 1
);
