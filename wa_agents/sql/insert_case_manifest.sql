-- PARAMS:
  -- handler_id    : case_handler_routes.id | None
  -- contact       : api_contacts.id
  -- machine_state : str | None
  -- agent_names   : list[str]

WITH CTE_next_case AS (
  SELECT
    COALESCE( max(cas.case_index) + 1, 1 ) AS case_index
  FROM
    wa_agents.case_handler_case_manifests AS cas
  WHERE
    ( cas.contact = @contact )
),
CTE_manifest AS (
  INSERT INTO wa_agents.case_handler_case_manifests (
    handler_id,
    contact,
    case_index,
    machine_state
  )
  SELECT
    @handler_id,
    @contact,
    CTE_next_case.case_index,
    @machine_state
  FROM
    CTE_next_case
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
    machine_state,
    silenced_until
),
CTE_initial_state AS (
  INSERT INTO wa_agents.case_handler_state_histories (
    case_manifest,
    case_message,
    machine_state
  )
  SELECT
    CTE_manifest.id,
    NULL::BIGINT,
    CTE_manifest.machine_state
  FROM
    CTE_manifest
  RETURNING
    case_manifest
),
CTE_initialized AS (
  INSERT INTO wa_agents.case_handler_agent_contexts (
    agent_name,
    context_index,
    case_manifest,
    case_message
  )
  SELECT DISTINCT
    names.agent_name,
    0,
    CTE_manifest.id,
    NULL::BIGINT
  FROM
    CTE_manifest
  CROSS JOIN
    unnest( @agent_names::TEXT[] ) AS names(agent_name)
  RETURNING
    case_manifest
)
SELECT
  CTE_manifest.id,
  CTE_manifest.created_at,
  CTE_manifest.updated_at,
  CTE_manifest.handler_id,
  CTE_manifest.contact,
  CTE_manifest.case_index,
  CTE_manifest.is_open,
  CTE_manifest.machine_state,
  CTE_manifest.silenced_until
FROM
  CTE_manifest
JOIN
  CTE_initial_state AS history
ON
  CTE_manifest.id = history.case_manifest
LEFT JOIN
  (
  SELECT DISTINCT
    CTE_initialized.case_manifest
  FROM
    CTE_initialized
  )
  AS contexts
ON
  CTE_manifest.id = contexts.case_manifest;
