SELECT
  rou.id                         AS id,
  rou.phone_number               AS phone_number,
  rou.contact                    AS contact,
  cfg.handler_key                AS handler_key,
  cfg.handler_url                AS handler_url,
  cfg.case_stale_hours           AS case_stale_hours,
  cfg.response_delay_seconds     AS response_delay_seconds,
  cfg.max_response_delay_seconds AS max_response_delay_seconds,
  cfg.burst_decay_seconds        AS burst_decay_seconds,
  cfg.silence_timeout_minutes    AS silence_timeout_minutes,
  rou.created_at                 AS created_at,
  rou.updated_at                 AS updated_at
FROM
  wa_agents.case_handler_routes         AS rou
JOIN
  wa_agents.case_handler_configurations AS cfg ON rou.handler_config = cfg.id
ORDER BY
  rou.phone_number,
  rou.contact NULLS FIRST;
