-- PARAMS:
  -- business      : wa_api_businesses.id
  -- contact       : wa_api_contacts.id | None
  -- handler_key   : str
  -- handler_url   : str | None

INSERT INTO public.wa_case_handler_routes AS rou (
  business,
  contact,
  handler_key,
  handler_url
)
VALUES (
  @business,
  @contact,
  @handler_key,
  @handler_url
)
ON CONFLICT ( business, contact)
DO
  UPDATE
SET
  handler_key   = EXCLUDED.handler_key,
  handler_url   = EXCLUDED.handler_url,
  updated_at    = now()
RETURNING
  rou.id,
  rou.business,
  rou.contact,
  rou.handler_key,
  rou.handler_url,
  rou.created_at,
  rou.updated_at;
