SELECT
  id,
  business,
  contact,
  handler_key,
  handler_url,
  created_at,
  updated_at
FROM
  public.wa_case_handler_routes
ORDER BY
  business,
  contact NULLS FIRST;
