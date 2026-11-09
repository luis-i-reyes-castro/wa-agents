-- PARAMS:
  -- contact     : api_contacts.id
  -- handler_key : case_handler_configurations.handler_key

INSERT INTO wa_agents.case_handler_routes_by_contact AS rou (
  contact,
  handler_config
)
SELECT
  @contact,
  cfg.id
FROM
  wa_agents.case_handler_configurations AS cfg
WHERE
  ( cfg.handler_key = @handler_key )
ON CONFLICT
  (contact)
DO
  UPDATE
SET
  handler_config = EXCLUDED.handler_config,
  updated_at     = now()
RETURNING
  rou.id,
  rou.contact,
  rou.handler_config,
  rou.created_at,
  rou.updated_at;
