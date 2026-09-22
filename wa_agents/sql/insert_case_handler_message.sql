-- PARAMS:
  -- case_id       : wa_case_handler_case_manifests.id
  -- ts            : datetime
  -- basemodel     : Message subclass name
  -- origin        : str | None
  -- data          : dict
  -- machine_state : str | None
  -- agent_contexts_to_clear  : list[str]
  -- agent_contexts_to_append : list[str]

WITH inserted_message AS (
  INSERT INTO public.wa_case_handler_messages (
    case_id,
    ts,
    basemodel,
    origin,
    data
  )
  VALUES (
    @case_id,
    @ts,
    @basemodel,
    @origin,
    @data
  )
  RETURNING
    id,
    ts,
    case_id,
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
    COALESCE( max(ctx.agent_context), 0 ) AS agent_context
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
    agent_context,
    case_manifest,
    case_message
  )
  SELECT
    contexts.agent_name,
    contexts.agent_context + 1,
    @case_id,
    NULL
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
    agent_context,
    case_manifest,
    case_message
  )
  SELECT
    contexts.agent_name,
    contexts.agent_context + CASE
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
UPDATE
  public.wa_case_handler_case_manifests AS cas
SET
  updated_at    = now(),
  machine_state = @machine_state
FROM
  inserted_message AS msg
WHERE
  ( cas.id = msg.case_id )
RETURNING
  msg.id,
  msg.ts,
  msg.case_id,
  msg.basemodel,
  msg.origin,
  msg.data,
  cas.machine_state;
