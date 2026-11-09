-- PARAMS:
  -- queue_row_id       : api_to_case_handler_queue.id
  -- response_delay     : float, base seconds
  -- max_response_delay : float, maximum seconds
  -- decay_seconds      : float, seconds per burst-score point
-- Run after lock_burst_score_contact.sql in the same transaction.

WITH CTE_existing AS MATERIALIZED (
  SELECT
    history.*
  FROM
    wa_agents.case_handler_contact_burst_score_histories AS history
  WHERE
    ( history.queue_row = @queue_row_id )
),
CTE_arrival AS (
  SELECT
    msg.contact                AS contact,
    que.id                     AS queue_row,
    GREATEST(
      que.created_at,
      score.last_received_at
    )                          AS received_at,
    COALESCE( score.score, 0 ) AS score_before,
    LEAST(
      COALESCE( score.score, 0 ),
      GREATEST(
        0,
        EXTRACT( EPOCH FROM ( que.created_at - score.last_received_at ) )
          / @decay_seconds::DOUBLE PRECISION
      )
    )                          AS decayed_points,
    CASE
      WHEN score.contact IS NULL THEN 0
      WHEN EXISTS (
        SELECT
          1
        FROM
          wa_agents.api_outbound_messages AS outbound
        WHERE
          ( outbound.contact = msg.contact             ) AND
          ( outbound.sent_at >= score.last_received_at ) AND
          ( outbound.sent_at <= que.created_at         )
      ) THEN 0
      WHEN EXISTS (
        SELECT
          1
        FROM
          wa_agents.api_inbound_messages      AS echo
        JOIN
          wa_agents.api_to_case_handler_queue AS echo_queue ON
            echo.msg_id = echo_queue.msg_id
        WHERE
          ( echo.contact  = msg.contact          ) AND
          ( echo.is_echo                         ) AND
          ( echo_queue.id > score.last_queue_row ) AND
          ( echo_queue.id < que.id               )
      ) THEN 0
      ELSE 1
    END                        AS increment
  FROM
    wa_agents.api_inbound_messages              AS msg
  JOIN
    wa_agents.api_to_case_handler_queue         AS que   ON msg.msg_id  = que.msg_id
  LEFT JOIN
    wa_agents.case_handler_contact_burst_scores AS score ON msg.contact = score.contact
  WHERE
    ( que.id            = @queue_row_id ) AND
    ( NOT msg.is_echo                   ) AND
    ( msg.history_chunk IS NULL         ) AND
    NOT EXISTS (
      SELECT
        1
      FROM
        CTE_existing
    ) AND
    NOT EXISTS (
      SELECT
        1
      FROM
        wa_agents.case_handler_case_manifests AS manifest
      WHERE
        ( manifest.contact        = msg.contact                       ) AND
        ( manifest.is_open                                            ) AND
        ( manifest.route_by_phone_number
                                  IS NOT DISTINCT FROM
                                    que.route_by_phone_number         ) AND
        ( manifest.route_by_contact
                                  IS NOT DISTINCT FROM
                                    que.route_by_contact              ) AND
        ( manifest.silenced_until > now()                             )
    )
),
CTE_scored AS (
  SELECT
    arrival.contact        AS contact,
    arrival.queue_row      AS queue_row,
    arrival.received_at    AS received_at,
    arrival.score_before   AS score_before,
    arrival.decayed_points AS decayed_points,
    arrival.increment      AS increment,
    LEAST(
      10,
      arrival.score_before - arrival.decayed_points + arrival.increment
    )                      AS score
  FROM
    CTE_arrival AS arrival
),
CTE_history AS (
  INSERT INTO wa_agents.case_handler_contact_burst_score_histories (
    contact,
    queue_row,
    received_at,
    score_before,
    decayed_points,
    increment,
    score,
    response_delay
  )
  SELECT
    scored.contact        AS contact,
    scored.queue_row      AS queue_row,
    scored.received_at    AS received_at,
    scored.score_before   AS score_before,
    scored.decayed_points AS decayed_points,
    scored.increment      AS increment,
    scored.score          AS score,
    CASE
      WHEN scored.score >= (
        LN(@max_response_delay::DOUBLE PRECISION)
        - LN(@response_delay::DOUBLE PRECISION)
      ) / LN(2.0) THEN @max_response_delay::DOUBLE PRECISION
      ELSE LEAST(
        @max_response_delay::DOUBLE PRECISION,
        @response_delay::DOUBLE PRECISION * POWER( 2.0::DOUBLE PRECISION, scored.score)
      )
    END                   AS response_delay
  FROM
    CTE_scored AS scored
  RETURNING
    *
),
CTE_updated AS (
  INSERT INTO wa_agents.case_handler_contact_burst_scores (
    contact,
    score,
    last_queue_row,
    last_received_at
  )
  SELECT
    history.contact     AS contact,
    history.score       AS score,
    history.queue_row   AS last_queue_row,
    history.received_at AS last_received_at
  FROM
    CTE_history AS history
  ON CONFLICT (contact)
  DO
    UPDATE
  SET
    score            = EXCLUDED.score,
    last_queue_row   = EXCLUDED.last_queue_row,
    last_received_at = EXCLUDED.last_received_at,
    updated_at       = now()
)
SELECT
  *
FROM
  CTE_history
UNION ALL
SELECT
  *
FROM
  CTE_existing;
