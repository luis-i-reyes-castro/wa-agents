-- PARAMS:
  -- route_id : case_handler_routes.id

DELETE FROM
  wa_agents.case_handler_routes
WHERE
  ( id = @route_id )
RETURNING
  id,
  phone_number,
  contact,
  handler_key;
