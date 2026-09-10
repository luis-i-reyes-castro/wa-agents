-- PARAMS:
  -- contact       : wa_api_contacts.id
  -- machine_state : str | None

INSERT INTO public.wa_case_handler_case_manifests (
  contact,
  machine_state
)
VALUES (
  @contact,
  @machine_state
)
ON CONFLICT (contact) WHERE is_open
DO
  NOTHING
RETURNING
  id,
  contact,
  created_at,
  updated_at,
  is_open,
  machine_state;
