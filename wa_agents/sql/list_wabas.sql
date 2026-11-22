-- PARAMS:
  -- id_biz : int

WITH CTE_latest_signup AS (
  SELECT DISTINCT ON ( ses.business)
    ses.business                              AS business,
    ( ses.application_data->>'id_biz' )::BIGINT AS id_biz
  FROM
    wa_agents.api_embedded_signup_sessions AS ses
  WHERE
    ( ses.session_status                          = 'completed' ) AND
    ( ses.business                                IS NOT NULL   ) AND
    ( JSONB_TYPEOF( ses.application_data->'id_biz' ) = 'number'    ) AND
    ( ( ses.application_data->>'id_biz' )::BIGINT   = @id_biz     )
  ORDER BY
    ses.business,
    ses.finished_at DESC,
    ses.id DESC
)
SELECT
  bus.id                AS id,
  bus.waba_id           AS waba_id,
  esd.connection_status AS status,
  esd.app_subscribed    AS webhook_subscribed,
  esd.last_error        AS last_error,
  bus.created_at        AS created_at,
  esd.updated_at        AS updated_at
FROM
  wa_agents.api_businesses          AS bus
JOIN
  wa_agents.api_embedded_signup_data AS esd ON bus.id = esd.business
JOIN
  CTE_latest_signup                AS own ON bus.id = own.business
WHERE
  own.id_biz = @id_biz
ORDER BY
  bus.id DESC;
