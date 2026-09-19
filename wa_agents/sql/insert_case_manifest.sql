-- PARAMS:
  -- handler_id    : wa_case_handler_routes.id | None
  -- contact       : wa_api_contacts.id
  -- machine_state : str | None

INSERT INTO public.wa_case_handler_case_manifests (
  handler_id,
  contact,
  machine_state
)
VALUES (
  @handler_id,
  @contact,
  @machine_state
)
ON CONFLICT (contact) WHERE is_open
DO
  NOTHING
RETURNING
  id,
  handler_id,
  contact,
  created_at,
  updated_at,
  is_open,
  machine_state;
