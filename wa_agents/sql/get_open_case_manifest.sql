-- PARAMS:
  -- contact : api_contacts.id

SELECT
  cas.id,
  cas.created_at,
  cas.updated_at,
  cas.route_by_phone_number,
  cas.route_by_contact,
  cas.contact,
  cas.case_index,
  cas.is_open,
  cas.machine_state,
  cas.silenced_until
FROM
  wa_agents.case_handler_case_manifests AS cas
WHERE
  ( cas.contact = @contact ) AND
  ( cas.is_open            )
LIMIT 1;
