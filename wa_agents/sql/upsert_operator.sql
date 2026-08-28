-- PARAMS:
  -- operator_id          : str
  -- display_phone_number : str | None
  -- waba_id              : str | None

INSERT INTO wa_operators (
  waba_id,
  operator_id,
  display_phone_number
)
VALUES (
  @waba_id,
  @operator_id,
  @display_phone_number
)
ON CONFLICT (operator_id)
DO
  UPDATE
SET
  waba_id              = EXCLUDED.waba_id,
  display_phone_number = EXCLUDED.display_phone_number,
  last_seen_at         = now(),
  updated_at           = now();
