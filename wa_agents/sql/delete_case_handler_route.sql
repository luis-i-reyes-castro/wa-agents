-- PARAMS:
  -- route_id : case_handler_routes.id

DELETE FROM
  wa_agents.case_handler_routes         AS rou
USING
  wa_agents.case_handler_configurations AS cfg
WHERE
  ( rou.id = @route_id          ) AND
  ( cfg.id = rou.handler_config )
RETURNING
  rou.id,
  rou.phone_number,
  rou.contact,
  cfg.handler_key;
