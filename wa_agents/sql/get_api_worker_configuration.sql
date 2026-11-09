-- PARAMS:
  -- server_name : api_worker_configurations.server_name

SELECT
  awc.id                 AS id,
  awc.server_name        AS server_name,
  awc.poll_interval_busy AS poll_interval_busy,
  awc.poll_interval_idle AS poll_interval_idle
FROM
  wa_agents.api_worker_configurations AS awc
WHERE
  ( awc.server_name = @server_name );
