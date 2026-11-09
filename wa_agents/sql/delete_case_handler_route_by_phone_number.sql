-- PARAMS:
  -- route_id : case_handler_routes_by_phone_number.id

DELETE FROM
  wa_agents.case_handler_routes_by_phone_number AS rou
USING
  wa_agents.case_handler_configurations         AS cfg
WHERE
  ( rou.id = @route_id          ) AND
  ( cfg.id = rou.handler_config )
RETURNING
  rou.id,
  rou.phone_number,
  cfg.handler_key;
