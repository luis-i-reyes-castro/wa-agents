-- PARAMS:
  -- phone_number : api_phone_numbers.id
  -- contact      : api_contacts.id | None
  -- handler_key  : case_handler_configurations.handler_key

INSERT INTO wa_agents.case_handler_routes AS rou (
  phone_number,
  contact,
  handler_config
)
SELECT
  @phone_number,
  @contact,
  cfg.id
FROM
  wa_agents.case_handler_configurations AS cfg
WHERE
  ( cfg.handler_key = @handler_key )
ON CONFLICT
  ( phone_number, contact )
DO
  UPDATE
SET
  handler_config = EXCLUDED.handler_config,
  updated_at     = now()
RETURNING
  rou.id,
  rou.phone_number,
  rou.contact,
  rou.handler_config,
  rou.created_at,
  rou.updated_at;
