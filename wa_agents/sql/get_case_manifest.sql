-- PARAMS:
  -- case_id : wa_case_handler_case_manifests.id
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
  ( cas.id      = @case_id ) AND
  ( cas.contact = @contact );
