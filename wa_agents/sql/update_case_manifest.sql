-- PARAMS:
  -- case_id       : wa_case_handler_case_manifests.id
  -- contact       : wa_api_contacts.id
  -- is_open       : bool
  -- machine_state : str | None

UPDATE
  public.wa_case_handler_case_manifests
SET
  updated_at    = now(),
  is_open       = @is_open,
  machine_state = @machine_state
WHERE
  ( id      = @case_id ) AND
  ( contact = @contact )
RETURNING
  id,
  contact,
  created_at,
  updated_at,
  is_open,
  machine_state;
