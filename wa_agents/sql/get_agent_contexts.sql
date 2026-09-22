-- PARAMS:
  -- case_manifest : wa_case_handler_case_manifests.id

WITH current_contexts AS (
  SELECT
    ctx.agent_name,
    max(ctx.context_index) AS context_index
  FROM
    public.wa_case_handler_agent_contexts AS ctx
  WHERE
    ( ctx.case_manifest = @case_manifest )
  GROUP BY
    ctx.agent_name
)
SELECT
  ctx.agent_name,
  ctx.case_message
FROM
  current_contexts AS cur
JOIN
  public.wa_case_handler_agent_contexts AS ctx
ON
  ( ctx.case_manifest = @case_manifest     ) AND
  ( ctx.agent_name    = cur.agent_name     ) AND
  ( ctx.context_index = cur.context_index  )
WHERE
  ( ctx.case_message IS NOT NULL )
ORDER BY
  ctx.agent_name ASC,
  ctx.id         ASC;
