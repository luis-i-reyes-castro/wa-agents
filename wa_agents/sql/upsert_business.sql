-- PARAMS:
  -- waba_id              : NumericID
  -- phone_number_id      : NumericID
  -- display_phone_number : NumericID

INSERT INTO public.wa_api_businesses (
  waba_id,
  phone_number_id,
  display_phone_number
)
VALUES (
  @waba_id,
  @phone_number_id,
  @display_phone_number
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
  display_phone_number;
