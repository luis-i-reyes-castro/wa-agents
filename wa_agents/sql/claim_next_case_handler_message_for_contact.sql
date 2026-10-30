-- PARAMS:
  -- contact             : api_contacts.id
  -- owner_token         : UUID
  -- after_row_id        : api_to_case_handler_queue.id
  -- handler_keys        : list[str]
  -- fallback_handler_key : str

WITH CTE_active_lease AS MATERIALIZED (
  SELECT
    lea.contact,
    lea.owner_token
  FROM
    wa_agents.case_handler_contact_leases AS lea
  WHERE
    ( lea.contact     = @contact     ) AND
    ( lea.owner_token = @owner_token ) AND
    ( lea.expires_at  > now()        )
  FOR UPDATE
),
CTE_candidate AS MATERIALIZED (
  SELECT
    msg.contact                                        AS contact,
    que.id                                             AS id,
    que.handler_id                                     AS handler_id,
    que.msg_id                                         AS msg_id,
    COALESCE( rou.handler_key, @fallback_handler_key ) AS handler_key,
    settings.silence_timeout_minutes                   AS silence_timeout_minutes
  FROM
    wa_agents.api_phone_numbers         AS num
  JOIN
    wa_agents.api_contacts              AS con      ON num.id     = con.phone_number
  JOIN
    CTE_active_lease                        AS lea      ON con.id     = lea.contact
  JOIN
    wa_agents.api_inbound_messages      AS msg      ON con.id     = msg.contact
  JOIN
    wa_agents.api_to_case_handler_queue AS que      ON msg.msg_id = que.msg_id
  LEFT JOIN
    wa_agents.case_handler_routes       AS rou      ON rou.id     = que.handler_id
  LEFT JOIN
    wa_agents.case_handler_settings     AS settings ON
      settings.handler_key = COALESCE( rou.handler_key, @fallback_handler_key )
  WHERE
    ( msg.contact             = @contact      ) AND
    ( que.id                  > @after_row_id ) AND
    ( que.msg_status          = 'pending'     ) AND
    ( num.registration_status = 'active'      )
  ORDER BY
    que.created_at ASC,
    que.id         ASC
  LIMIT 1
  FOR UPDATE OF que, num SKIP LOCKED
),
CTE_renewed AS (
  UPDATE
    wa_agents.case_handler_contact_leases AS lea
  SET
    heartbeat_at = now(),
    expires_at   = now() + INTERVAL '90 seconds'
  FROM
    CTE_candidate AS can
  WHERE
    ( lea.contact     = can.contact         ) AND
    ( lea.owner_token = @owner_token        ) AND
    ( can.handler_key = ANY( @handler_keys) )
  RETURNING
    lea.contact,
    lea.owner_token
)
UPDATE
  wa_agents.api_to_case_handler_queue AS que
SET
  msg_status = 'processing',
  updated_at = now()
FROM
  CTE_candidate AS can
JOIN
  CTE_renewed AS lea ON lea.contact = can.contact
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
