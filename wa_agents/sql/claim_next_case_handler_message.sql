-- PARAMS:
  -- owner_token          : UUID
  -- handler_keys         : list[str]
  -- fallback_handler_key : str | None

WITH CTE_candidate AS MATERIALIZED (
  SELECT
    msg.contact                       AS contact,
    que.id                            AS id,
    que.handler_id                    AS handler_id,
    que.msg_id                        AS msg_id,
    COALESCE(
      cfg.handler_key,
      fbk.handler_key
    )                                 AS handler_key,
    COALESCE(
      cfg.case_stale_hours,
      fbk.case_stale_hours
    )                                 AS case_stale_hours,
    COALESCE(
      cfg.response_delay_seconds,
      fbk.response_delay_seconds
    )                                 AS response_delay_seconds,
    COALESCE(
      cfg.max_response_delay_seconds,
      fbk.max_response_delay_seconds
    )                                 AS max_response_delay_seconds,
    COALESCE(
      cfg.burst_decay_seconds,
      fbk.burst_decay_seconds
    )                                 AS burst_decay_seconds,
    COALESCE(
      cfg.silence_timeout_minutes,
      fbk.silence_timeout_minutes
    )                                 AS silence_timeout_minutes
  FROM
    wa_agents.api_phone_numbers         AS num
  JOIN
    wa_agents.api_contacts              AS con      ON num.id     = con.phone_number
  JOIN
    wa_agents.api_inbound_messages      AS msg      ON con.id     = msg.contact
  JOIN
    wa_agents.api_to_case_handler_queue AS que      ON msg.msg_id = que.msg_id
  LEFT JOIN
    wa_agents.case_handler_routes          AS rou ON que.handler_id     = rou.id
  LEFT JOIN
    wa_agents.case_handler_configurations  AS cfg ON rou.handler_config = cfg.id
  LEFT JOIN
    wa_agents.case_handler_configurations  AS fbk ON
      ( rou.id          IS NULL                       ) AND
      ( fbk.handler_key = @fallback_handler_key::text )
  WHERE
    ( num.registration_status = 'active'  ) AND
    ( que.msg_status          = 'pending' ) AND
    (
      COALESCE( cfg.handler_key, fbk.handler_key) = ANY( @handler_keys )
    ) AND
    NOT EXISTS (
      SELECT
        1
      FROM
        wa_agents.case_handler_contact_leases AS old
      WHERE
        ( old.contact    = msg.contact ) AND
        ( old.expires_at > now()       )
    )
  ORDER BY
    que.created_at ASC,
    que.id         ASC
  LIMIT 1
  FOR UPDATE OF que, num SKIP LOCKED
),
CTE_leased AS (
  INSERT INTO wa_agents.case_handler_contact_leases (
    contact,
    owner_token,
    acquired_at,
    heartbeat_at,
    expires_at
  )
  SELECT
    can.contact,
    @owner_token,
    now(),
    now(),
    now() + INTERVAL '90 seconds'
  FROM
    CTE_candidate AS can
  ON CONFLICT (contact)
  DO
    UPDATE
  SET
    owner_token  = EXCLUDED.owner_token,
    acquired_at  = EXCLUDED.acquired_at,
    heartbeat_at = EXCLUDED.heartbeat_at,
    expires_at   = EXCLUDED.expires_at
  WHERE
    ( case_handler_contact_leases.expires_at <= now() )
  RETURNING
    contact,
    owner_token
)
UPDATE
  wa_agents.api_to_case_handler_queue AS que
SET
  msg_status = 'processing',
  updated_at = now()
FROM
  CTE_candidate AS can
JOIN
  CTE_leased AS lea ON
    ( lea.contact = can.contact )
WHERE
  ( que.id = can.id )
RETURNING
  can.contact                     AS contact,
  lea.owner_token                 AS owner_token,
  que.id                          AS row_id,
  can.handler_id                  AS handler_id,
  can.handler_key                 AS handler_key,
  can.case_stale_hours            AS case_stale_hours,
  can.response_delay_seconds      AS response_delay_seconds,
  can.max_response_delay_seconds  AS max_response_delay_seconds,
  can.burst_decay_seconds         AS burst_decay_seconds,
  can.silence_timeout_minutes     AS silence_timeout_minutes,
  que.msg_id                      AS msg_id,
  que.msg_status                  AS msg_status;
