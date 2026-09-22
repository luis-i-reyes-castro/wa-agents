-- PARAMS:
  -- case_id       : wa_case_handler_case_manifests.id
  -- ts            : datetime
  -- basemodel     : Message subclass name
  -- origin        : str | None
  -- data          : dict
  -- machine_state : str | None
  -- agent_contexts_to_clear  : list[str]
  -- agent_contexts_to_append : list[str]

WITH case_manifest AS (
  UPDATE
    public.wa_case_handler_case_manifests
  SET
    updated_at         = now(),
    next_message_index = next_message_index + 1,
    machine_state      = @machine_state
  WHERE
    ( id = @case_id )
  RETURNING
    id                      AS id,
    next_message_index - 1  AS message_index,
    machine_state           AS machine_state
),
inserted_message AS (
  INSERT INTO public.wa_case_handler_messages (
    case_id,
    message_index,
    ts,
    basemodel,
    origin,
    data
  )
  SELECT
    manifest.id,
    manifest.message_index,
    @ts,
    @basemodel,
    @origin,
    @data
  FROM
    case_manifest AS manifest
  RETURNING
    id,
    ts,
    case_id,
    message_index,
    basemodel,
    origin,
    data
),
changed_agent_names AS (
  SELECT DISTINCT
    names.agent_name
  FROM
    unnest(
      @agent_contexts_to_clear::TEXT[] ||
      @agent_contexts_to_append::TEXT[]
    ) AS names(agent_name)
),
current_agent_contexts AS (
  SELECT
    names.agent_name,
    COALESCE( max(ctx.context_index), 0 ) AS context_index
  FROM
    changed_agent_names AS names
  LEFT JOIN
    public.wa_case_handler_agent_contexts AS ctx
  ON
    ( ctx.case_manifest = @case_id         ) AND
    ( ctx.agent_name    = names.agent_name )
  GROUP BY
    names.agent_name
),
cleared_agent_contexts AS (
  INSERT INTO public.wa_case_handler_agent_contexts (
    agent_name,
    context_index,
    case_manifest,
    case_message
  )
  SELECT
    contexts.agent_name,
    contexts.context_index + 1,
    @case_id,
    NULL::BIGINT
  FROM
    current_agent_contexts AS contexts
  WHERE
    ( contexts.agent_name = ANY( @agent_contexts_to_clear::TEXT[] ) )
  ON CONFLICT DO NOTHING
  RETURNING
    agent_name
),
extended_agent_contexts AS (
  INSERT INTO public.wa_case_handler_agent_contexts (
    agent_name,
    context_index,
    case_manifest,
    case_message
  )
  SELECT
    contexts.agent_name,
    contexts.context_index + CASE
      WHEN (
        contexts.agent_name = ANY( @agent_contexts_to_clear::TEXT[] )
      ) THEN 1
      ELSE 0
    END,
    @case_id,
    message.id
  FROM
    current_agent_contexts AS contexts
  CROSS JOIN
    inserted_message AS message
  WHERE
    ( contexts.agent_name = ANY( @agent_contexts_to_append::TEXT[] ) )
  ON CONFLICT DO NOTHING
  RETURNING
    agent_name
)
SELECT
  msg.id,
  msg.ts,
  msg.case_id,
  msg.message_index,
  msg.basemodel,
  msg.origin,
  msg.data,
  manifest.machine_state
FROM
  inserted_message AS msg
JOIN
  case_manifest AS manifest ON manifest.id = msg.case_id;
