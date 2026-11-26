-- PARAMS:
  -- case_id        : case_handler_case_manifests.id
  -- contact        : api_contacts.id
  -- invocation     : case_handler_invocations.id | None
  -- is_open        : bool
  -- machine_state  : str | None
  -- silenced_until : datetime | None

UPDATE
  wa_agents.case_handler_case_manifests
SET
  updated_at     = now(),
  is_open        = @is_open,
  machine_state  = @machine_state,
  silenced_until = @silenced_until
WHERE
  ( id         = @case_id                       ) AND
  ( contact    = @contact                       ) AND
  ( invocation IS NOT DISTINCT FROM @invocation )
RETURNING
  id,
  created_at,
  updated_at,
  route_by_phone_number,
  route_by_contact,
  contact,
  invocation,
  case_index,
  is_open,
  machine_state,
  silenced_until;
