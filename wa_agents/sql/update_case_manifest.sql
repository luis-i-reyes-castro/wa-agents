-- PARAMS:
  -- case_id        : wa_case_handler_case_manifests.id
  -- contact        : wa_api_contacts.id
  -- is_open        : bool
  -- machine_state  : str | None
  -- silenced_until : datetime | None

UPDATE
  public.wa_case_handler_case_manifests
SET
  updated_at     = now(),
  is_open        = @is_open,
  machine_state  = @machine_state,
  silenced_until = @silenced_until
WHERE
  ( id      = @case_id ) AND
  ( contact = @contact )
RETURNING
  id,
  created_at,
  updated_at,
  handler_id,
  contact,
  case_index,
  is_open,
  machine_state,
  silenced_until;
