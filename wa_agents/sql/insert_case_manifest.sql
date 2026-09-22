-- PARAMS:
  -- handler_id    : wa_case_handler_routes.id | None
  -- contact       : wa_api_contacts.id
  -- machine_state : str | None
  -- agent_names   : list[str]

WITH manifest AS (
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
    machine_state
),
initialized AS (
  INSERT INTO public.wa_case_handler_agent_contexts (
    agent_name,
    context_index,
    case_manifest,
    case_message
  )
  SELECT DISTINCT
    names.agent_name,
    0,
    manifest.id,
    NULL::BIGINT
  FROM
    manifest
  CROSS JOIN
    unnest( @agent_names::TEXT[] ) AS names(agent_name)
  RETURNING
    case_manifest
)
SELECT
  manifest.id,
  manifest.handler_id,
  manifest.contact,
  manifest.created_at,
  manifest.updated_at,
  manifest.is_open,
  manifest.machine_state
FROM
  manifest
LEFT JOIN (
  SELECT DISTINCT
    initialized.case_manifest
  FROM
    initialized
) AS contexts ON contexts.case_manifest = manifest.id;
