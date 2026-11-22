-- PARAMS:
  -- id_biz      : int
  -- waba_id     : str
  -- secret_key  : str

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
  bus.id                                                    AS id,
  own.id_biz                                                AS id_biz,
  bus.waba_id                                               AS waba_id,
  esd.connection_status                                     AS status,
  esd.app_subscribed                                        AS webhook_subscribed,
  esd.last_error                                            AS last_error,
  wa_agents.decrypt_text( esd.business_token, @secret_key ) AS business_token
FROM
  wa_agents.api_businesses          AS bus
JOIN
  wa_agents.api_embedded_signup_data AS esd ON bus.id = esd.business
JOIN
  CTE_latest_signup                AS own ON bus.id = own.business
WHERE
  ( own.id_biz  = @id_biz  ) AND
  ( bus.waba_id = @waba_id )
LIMIT 1;
