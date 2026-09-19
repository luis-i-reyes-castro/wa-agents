-- PARAMS:
  -- route_id : wa_case_handler_routes.id

DELETE FROM
  public.wa_case_handler_routes
WHERE
  id = @route_id
RETURNING
  id,
  business,
  contact,
  handler_key;
