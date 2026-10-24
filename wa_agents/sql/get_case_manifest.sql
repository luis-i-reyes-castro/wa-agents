-- PARAMS:
  -- case_id : case_handler_case_manifests.id
  -- contact : api_contacts.id

SELECT
  cas.id,
  cas.created_at,
  cas.updated_at,
  cas.handler_id,
  cas.contact,
  cas.case_index,
  cas.is_open,
  cas.machine_state,
  cas.silenced_until
FROM
  wa_agents.case_handler_case_manifests AS cas
WHERE
  ( cas.id      = @case_id ) AND
  ( cas.contact = @contact );
