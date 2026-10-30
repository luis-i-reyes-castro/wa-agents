-- PARAMS:
  -- contact     : api_contacts.id
  -- owner_token : UUID

WITH CTE_active_contact AS MATERIALIZED (
  SELECT
    con.id
  FROM
    wa_agents.api_phone_numbers AS num
  JOIN
    wa_agents.api_contacts      AS con ON num.id = con.phone_number
  WHERE
    ( con.id                  = @contact ) AND
    ( num.registration_status = 'active' )
  FOR SHARE OF num
)
INSERT INTO wa_agents.case_handler_contact_leases (
  contact,
  owner_token,
  acquired_at,
  heartbeat_at,
  expires_at
)
SELECT
  id,
  @owner_token,
  now(),
  now(),
  now() + INTERVAL '90 seconds'
FROM
  CTE_active_contact
ON CONFLICT
  (contact)
DO
  UPDATE
SET
  owner_token  = EXCLUDED.owner_token,
  acquired_at  = EXCLUDED.acquired_at,
  heartbeat_at = EXCLUDED.heartbeat_at,
  expires_at   = EXCLUDED.expires_at
WHERE
  ( case_handler_contact_leases.expires_at <= now() )
RETURNING
  contact,
  owner_token,
  acquired_at,
  heartbeat_at,
  expires_at;
