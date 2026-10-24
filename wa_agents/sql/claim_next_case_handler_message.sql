-- PARAMS:
  -- owner_token          : UUID
  -- handler_keys         : list[str]
  -- fallback_handler_key : str

WITH candidate AS MATERIALIZED (
  SELECT
    msg.contact,
    que.id,
    que.handler_id,
    que.msg_id,
    COALESCE( rou.handler_key, @fallback_handler_key ) AS handler_key,
    settings.silence_timeout_minutes
  FROM
    wa_agents.api_inbound_messages      AS msg
  JOIN
    wa_agents.api_to_case_handler_queue AS que ON que.msg_id = msg.msg_id
  LEFT JOIN
    wa_agents.case_handler_routes       AS rou ON rou.id = que.handler_id
  LEFT JOIN
    wa_agents.case_handler_settings     AS settings ON
      settings.handler_key = COALESCE( rou.handler_key, @fallback_handler_key )
  WHERE
    ( que.msg_status = 'pending' ) AND
    (
      COALESCE( rou.handler_key, @fallback_handler_key ) = ANY( @handler_keys )
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
  FOR UPDATE OF que SKIP LOCKED
),
leased AS (
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
    candidate AS can
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
  candidate AS can
JOIN
  leased AS lea ON
    ( lea.contact = can.contact )
WHERE
  ( que.id = can.id )
RETURNING
  can.contact                 AS contact,
  lea.owner_token             AS owner_token,
  que.id                      AS row_id,
  can.handler_id              AS handler_id,
  can.handler_key             AS handler_key,
  can.silence_timeout_minutes AS silence_timeout_minutes,
  que.msg_id                  AS msg_id,
  que.msg_status              AS msg_status;
