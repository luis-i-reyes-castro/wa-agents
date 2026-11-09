-- PARAMS:
  -- phone_number : api_phone_numbers.id
  -- handler_key  : case_handler_configurations.handler_key

INSERT INTO wa_agents.case_handler_routes_by_phone_number AS rou (
  phone_number,
  handler_config
)
SELECT
  @phone_number,
  cfg.id
FROM
  wa_agents.case_handler_configurations AS cfg
WHERE
  ( cfg.handler_key = @handler_key )
ON CONFLICT
  (phone_number)
DO
  UPDATE
SET
  handler_config = EXCLUDED.handler_config,
  updated_at     = now()
RETURNING
  rou.id,
  rou.phone_number,
  rou.handler_config,
  rou.created_at,
  rou.updated_at;
