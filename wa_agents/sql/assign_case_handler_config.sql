-- PARAMS:
  -- phone_number_id     : str
  -- case_handler_config : str

INSERT INTO wa_agents.case_handler_routes_by_phone_number AS rou (
  phone_number,
  handler_config
)
SELECT
  num.id,
  cfg.id
FROM
  wa_agents.api_phone_numbers           AS num
JOIN
  wa_agents.case_handler_configurations AS cfg
ON
  cfg.handler_key = @case_handler_config
WHERE
  num.phone_number_id = @phone_number_id
ON CONFLICT
  ( phone_number )
DO
  UPDATE
SET
  handler_config = EXCLUDED.handler_config,
  updated_at     = NOW()
RETURNING
  rou.id;
