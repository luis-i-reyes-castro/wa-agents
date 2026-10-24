SELECT
  id,
  phone_number,
  contact,
  handler_key,
  handler_url,
  created_at,
  updated_at
FROM
  wa_agents.case_handler_routes
ORDER BY
  phone_number,
  contact NULLS FIRST;
