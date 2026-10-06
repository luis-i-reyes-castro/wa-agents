-- Read-only contact state diagnostic for PostgreSQL/psql.
-- Usage:
--   psql "$DATABASE_URL" \
--     -v business_display_phone_number=593962939146 \
--     -v contact_wa_id=593995341161 \
--     -f skills/maintain-wa-agents/scripts/diagnose_case_state.sql

\if :{?business_display_phone_number}
\else
  \echo 'Missing required psql variable: business_display_phone_number'
  \quit
\endif

\if :{?contact_wa_id}
\else
  \echo 'Missing required psql variable: contact_wa_id'
  \quit
\endif

-- Every case, its current state, snapshot coverage, routing, and contact lease.
WITH target_contact AS (
  SELECT
    con.id                   AS contact_id,
    con.wa_id                AS contact_wa_id,
    con.user_id              AS contact_user_id,
    bus.id                   AS business_id,
    bus.display_phone_number AS business_phone_number
  FROM
    public.wa_api_businesses AS bus
  JOIN
    public.wa_api_contacts AS con ON con.business = bus.id
  WHERE
    ( bus.display_phone_number = :'business_display_phone_number' ) AND
    ( con.wa_id                = :'contact_wa_id'                  )
),
message_stats AS (
  SELECT
    msg.case_id,
    count(*) AS message_count
  FROM
    public.wa_case_handler_messages AS msg
  GROUP BY
    msg.case_id
),
history_stats AS (
  SELECT
    his.case_manifest,
    count(*)                                          AS snapshot_count,
    count(*) FILTER (WHERE his.case_message IS NULL) AS initial_snapshot_count
  FROM
    public.wa_case_handler_state_histories AS his
  GROUP BY
    his.case_manifest
)
SELECT
  tar.business_id,
  tar.business_phone_number,
  tar.contact_id,
  tar.contact_wa_id,
  tar.contact_user_id,
  cas.id                       AS case_manifest_id,
  cas.case_index               AS case_index,
  cas.created_at               AS case_created_at,
  cas.updated_at               AS case_updated_at,
  cas.is_open                  AS case_is_open,
  cas.machine_state            AS current_machine_state,
  cas.silenced_until           AS silenced_until,
  rou.id                       AS handler_route_id,
  rou.handler_key              AS handler_key,
  COALESCE( msg.message_count, 0)          AS message_count,
  COALESCE( his.snapshot_count, 0)         AS snapshot_count,
  COALESCE( his.initial_snapshot_count, 0) AS initial_snapshot_count,
  (
    ( COALESCE( his.initial_snapshot_count, 0) = 1                 ) AND
    ( COALESCE( his.snapshot_count, 0) = COALESCE( msg.message_count, 0) + 1 )
  )                            AS snapshot_coverage_complete,
  latest.created_at            AS latest_snapshot_at,
  latest.machine_state         AS latest_snapshot_state,
  (
    ( COALESCE( his.snapshot_count, 0) > 0 ) AND
    ( cas.machine_state IS NOT DISTINCT FROM latest.machine_state )
  )                            AS current_state_matches_latest_snapshot,
  lea.owner_token              AS lease_owner_token,
  lea.acquired_at              AS lease_acquired_at,
  lea.heartbeat_at             AS lease_heartbeat_at,
  lea.expires_at               AS lease_expires_at,
  ( lea.expires_at > now() )   AS lease_is_active
FROM
  target_contact AS tar
JOIN
  public.wa_case_handler_case_manifests AS cas ON cas.contact = tar.contact_id
LEFT JOIN
  public.wa_case_handler_routes AS rou ON rou.id = cas.handler_id
LEFT JOIN
  public.wa_case_handler_contact_leases AS lea ON lea.contact = tar.contact_id
LEFT JOIN
  message_stats AS msg ON msg.case_id = cas.id
LEFT JOIN
  history_stats AS his ON his.case_manifest = cas.id
LEFT JOIN LATERAL (
  SELECT
    state.created_at,
    state.machine_state
  FROM
    public.wa_case_handler_state_histories AS state
  WHERE
    ( state.case_manifest = cas.id )
  ORDER BY
    state.id DESC
  LIMIT 1
) AS latest ON TRUE
ORDER BY
  cas.case_index ASC,
  cas.id         ASC;

-- Every initial state and message across all cases, including missing snapshots.
WITH target_cases AS (
  SELECT
    cas.id,
    cas.case_index,
    cas.created_at,
    cas.updated_at,
    cas.is_open,
    cas.machine_state,
    cas.handler_id,
    rou.handler_key
  FROM
    public.wa_api_businesses AS bus
  JOIN
    public.wa_api_contacts AS con ON con.business = bus.id
  JOIN
    public.wa_case_handler_case_manifests AS cas ON cas.contact = con.id
  LEFT JOIN
    public.wa_case_handler_routes AS rou ON rou.id = cas.handler_id
  WHERE
    ( bus.display_phone_number = :'business_display_phone_number' ) AND
    ( con.wa_id                = :'contact_wa_id'                  )
),
case_events AS (
  SELECT
    cas.*,
    0                 AS event_index,
    his.id            AS state_history_id,
    his.created_at    AS state_recorded_at,
    his.machine_state AS event_machine_state,
    NULL::BIGINT      AS case_message_id
  FROM
    target_cases AS cas
  LEFT JOIN
    public.wa_case_handler_state_histories AS his ON
      ( his.case_manifest = cas.id ) AND
      ( his.case_message IS NULL   )

  UNION ALL

  SELECT
    cas.*,
    msg.message_index AS event_index,
    his.id            AS state_history_id,
    his.created_at    AS state_recorded_at,
    his.machine_state AS event_machine_state,
    msg.id            AS case_message_id
  FROM
    target_cases AS cas
  JOIN
    public.wa_case_handler_messages AS msg ON msg.case_id = cas.id
  LEFT JOIN
    public.wa_case_handler_state_histories AS his ON
      ( his.case_manifest = cas.id ) AND
      ( his.case_message  = msg.id )
)
SELECT
  evt.id                       AS case_manifest_id,
  evt.case_index               AS case_index,
  evt.created_at               AS case_created_at,
  evt.updated_at               AS case_updated_at,
  evt.is_open                  AS case_is_open,
  evt.machine_state            AS current_machine_state,
  evt.handler_id               AS handler_route_id,
  evt.handler_key              AS handler_key,
  evt.event_index              AS event_index,
  CASE
    WHEN evt.case_message_id IS NULL THEN 'initial'
    ELSE 'message'
  END                          AS snapshot_kind,
  ( evt.state_history_id IS NULL ) AS state_snapshot_missing,
  evt.state_history_id         AS state_history_id,
  evt.state_recorded_at        AS state_recorded_at,
  evt.event_machine_state      AS event_machine_state,
  msg.id                       AS case_message_id,
  msg.message_index            AS case_message_index,
  msg.ts                       AS case_message_at,
  msg.basemodel                AS case_message_model,
  msg.origin                   AS case_message_origin,
  msg.data                     AS case_message_data,
  map.id                       AS api_mapping_id,
  CASE
    WHEN map.api_inbound_msg_id IS NOT NULL THEN 'inbound'
    WHEN map.api_outbound_msg_id IS NOT NULL THEN 'outbound'
  END                          AS api_direction,
  COALESCE( api_in.id, api_out.id)             AS api_message_row_id,
  COALESCE( api_in.msg_id, api_out.msg_id)     AS whatsapp_message_id,
  COALESCE( api_in.msg_ts, api_out.sent_at)    AS api_message_at,
  COALESCE( api_in.msg_type, api_out.msg_type) AS api_message_type,
  COALESCE( api_in.msg_data, api_out.msg_data) AS api_message_data,
  que.id                       AS queue_id,
  que.msg_status               AS queue_status,
  que.created_at               AS queue_created_at,
  que.updated_at               AS queue_updated_at,
  que.last_error_at            AS queue_last_error_at
FROM
  case_events AS evt
LEFT JOIN
  public.wa_case_handler_messages AS msg ON
    ( msg.case_id = evt.id              ) AND
    ( msg.id      = evt.case_message_id )
LEFT JOIN
  public.wa_case_handler_to_api AS map ON map.case_handler_msg_id = msg.id
LEFT JOIN
  public.wa_api_inbound_messages AS api_in ON api_in.id = map.api_inbound_msg_id
LEFT JOIN
  public.wa_api_outbound_messages AS api_out ON api_out.id = map.api_outbound_msg_id
LEFT JOIN
  public.wa_api_to_case_handler_queue AS que ON que.msg_id = api_in.msg_id
ORDER BY
  evt.case_index  ASC,
  evt.id          ASC,
  evt.event_index ASC,
  map.id          ASC NULLS FIRST;

-- Every agent-context generation across all cases and its ordered messages.
WITH target_cases AS (
  SELECT
    cas.id,
    cas.case_index
  FROM
    public.wa_api_businesses AS bus
  JOIN
    public.wa_api_contacts AS con ON con.business = bus.id
  JOIN
    public.wa_case_handler_case_manifests AS cas ON cas.contact = con.id
  WHERE
    ( bus.display_phone_number = :'business_display_phone_number' ) AND
    ( con.wa_id                = :'contact_wa_id'                  )
),
contexts AS (
  SELECT
    cas.case_index,
    ctx.*,
    max(ctx.context_index) OVER (
      PARTITION BY
        ctx.case_manifest,
        ctx.agent_name
    ) AS current_context_index
  FROM
    target_cases AS cas
  JOIN
    public.wa_case_handler_agent_contexts AS ctx ON ctx.case_manifest = cas.id
)
SELECT
  ctx.case_manifest,
  ctx.case_index,
  ctx.agent_name,
  ctx.context_index,
  ( ctx.context_index = ctx.current_context_index ) AS is_current_context,
  ctx.id                       AS context_membership_id,
  ctx.created_at               AS context_membership_created_at,
  msg.id                       AS case_message_id,
  msg.message_index            AS case_message_index,
  msg.ts                       AS case_message_at,
  msg.basemodel                AS case_message_model,
  msg.origin                   AS case_message_origin,
  msg.data                     AS case_message_data,
  his.machine_state            AS message_machine_state
FROM
  contexts AS ctx
LEFT JOIN
  public.wa_case_handler_messages AS msg ON
    ( msg.case_id = ctx.case_manifest ) AND
    ( msg.id      = ctx.case_message  )
LEFT JOIN
  public.wa_case_handler_state_histories AS his ON
    ( his.case_manifest = ctx.case_manifest ) AND
    ( his.case_message  = ctx.case_message  )
ORDER BY
  ctx.case_index     ASC,
  ctx.case_manifest ASC,
  ctx.agent_name     ASC,
  ctx.context_index  ASC,
  msg.message_index  ASC NULLS FIRST,
  ctx.id             ASC;
