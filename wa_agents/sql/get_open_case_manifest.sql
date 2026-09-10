-- PARAMS:
  -- contact : wa_api_contacts.id

SELECT
  cas.id,
  cas.contact,
  cas.created_at,
  cas.updated_at,
  cas.is_open,
  cas.machine_state
FROM
  public.wa_case_handler_case_manifests AS cas
WHERE
  ( cas.contact = @contact ) AND
  ( cas.is_open )
LIMIT 1;
