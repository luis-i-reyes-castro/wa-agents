-- PARAMS:
  -- contact     : api_contacts.id
  -- owner_token : UUID

UPDATE
  wa_agents.case_handler_contact_leases
SET
  heartbeat_at = now(),
  expires_at   = now() + INTERVAL '90 seconds'
WHERE
  ( contact     = @contact     ) AND
  ( owner_token = @owner_token ) AND
  ( expires_at  > now()        )
RETURNING
  contact,
  owner_token,
  acquired_at,
  heartbeat_at,
  expires_at;
