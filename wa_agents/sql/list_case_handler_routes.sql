SELECT
  'phone_number'                 AS route_scope,
  rou.id                         AS id,
  rou.phone_number               AS phone_number,
  NULL::BIGINT                   AS contact,
  cfg.handler_key                AS handler_key,
  cfg.handler_url                AS handler_url,
  cfg.case_stale_hours           AS case_stale_hours,
  cfg.response_delay_seconds     AS response_delay_seconds,
  cfg.max_response_delay_seconds AS max_response_delay_seconds,
  cfg.burst_decay_minutes        AS burst_decay_minutes,
  cfg.silence_timeout_minutes    AS silence_timeout_minutes,
  rou.created_at                 AS created_at,
  rou.updated_at                 AS updated_at
FROM
  wa_agents.case_handler_routes_by_phone_number AS rou
JOIN
  wa_agents.case_handler_configurations         AS cfg ON rou.handler_config = cfg.id
UNION ALL
SELECT
  'contact'                      AS route_scope,
  rou.id                         AS id,
  con.phone_number               AS phone_number,
  rou.contact                    AS contact,
  cfg.handler_key                AS handler_key,
  cfg.handler_url                AS handler_url,
  cfg.case_stale_hours           AS case_stale_hours,
  cfg.response_delay_seconds     AS response_delay_seconds,
  cfg.max_response_delay_seconds AS max_response_delay_seconds,
  cfg.burst_decay_minutes        AS burst_decay_minutes,
  cfg.silence_timeout_minutes    AS silence_timeout_minutes,
  rou.created_at                 AS created_at,
  rou.updated_at                 AS updated_at
FROM
  wa_agents.case_handler_routes_by_contact AS rou
JOIN
  wa_agents.api_contacts                   AS con ON rou.contact = con.id
JOIN
  wa_agents.case_handler_configurations    AS cfg ON rou.handler_config = cfg.id
ORDER BY
  phone_number,
  contact NULLS FIRST;
