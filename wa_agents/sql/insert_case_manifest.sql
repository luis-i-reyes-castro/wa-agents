-- PARAMS:
  -- handler_id    : wa_case_handler_routes.id | None
  -- contact       : wa_api_contacts.id
  -- machine_state : str | None
  -- agent_names   : list[str]

WITH next_case AS (
  SELECT
    COALESCE( max(cas.case_index) + 1, 1 ) AS case_index
  FROM
    public.wa_case_handler_case_manifests AS cas
  WHERE
    ( cas.contact = @contact )
),
manifest AS (
  INSERT INTO public.wa_case_handler_case_manifests (
    handler_id,
    contact,
    case_index,
    machine_state
  )
  SELECT
    @handler_id,
    @contact,
    next_case.case_index,
    @machine_state
  FROM
    next_case
  ON CONFLICT (contact) WHERE is_open
  DO
    NOTHING
  RETURNING
    id,
    created_at,
    updated_at,
    handler_id,
    contact,
    case_index,
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
  manifest.created_at,
  manifest.updated_at,
  manifest.handler_id,
  manifest.contact,
  manifest.case_index,
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
