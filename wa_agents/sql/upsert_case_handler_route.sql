-- PARAMS:
  -- phone_number  : wa_api_phone_numbers.id
  -- contact       : wa_api_contacts.id | None
  -- handler_key   : str
  -- handler_url   : str | None

INSERT INTO public.wa_case_handler_routes AS rou (
  phone_number,
  contact,
  handler_key,
  handler_url
)
VALUES (
  @phone_number,
  @contact,
  @handler_key,
  @handler_url
)
ON CONFLICT ( phone_number, contact)
DO
  UPDATE
SET
  handler_key   = EXCLUDED.handler_key,
  handler_url   = EXCLUDED.handler_url,
  updated_at    = now()
RETURNING
  rou.id,
  rou.phone_number,
  rou.contact,
  rou.handler_key,
  rou.handler_url,
  rou.created_at,
  rou.updated_at;
