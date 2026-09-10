-- PARAMS:
  -- contact     : wa_api_contacts.id
  -- owner_token : UUID

INSERT INTO public.wa_case_handler_contact_leases (
  contact,
  owner_token,
  acquired_at,
  heartbeat_at,
  expires_at
)
VALUES (
  @contact,
  @owner_token,
  now(),
  now(),
  now() + INTERVAL '90 seconds'
)
ON CONFLICT (contact)
DO
  UPDATE
SET
  owner_token  = EXCLUDED.owner_token,
  acquired_at  = EXCLUDED.acquired_at,
  heartbeat_at = EXCLUDED.heartbeat_at,
  expires_at   = EXCLUDED.expires_at
WHERE
  ( wa_case_handler_contact_leases.expires_at <= now() )
RETURNING
  contact,
  owner_token,
  acquired_at,
  heartbeat_at,
  expires_at;
