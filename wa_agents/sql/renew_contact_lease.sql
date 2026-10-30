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
  ( expires_at  > now()        ) AND
  EXISTS (
    SELECT
      1
    FROM
      wa_agents.api_contacts      AS con
    JOIN
      wa_agents.api_phone_numbers AS num ON num.id = con.phone_number
    WHERE
      ( con.id                  = case_handler_contact_leases.contact ) AND
      ( num.registration_status = 'active'                            )
  )
RETURNING
  contact,
  owner_token,
  acquired_at,
  heartbeat_at,
  expires_at;
