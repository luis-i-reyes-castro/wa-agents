-- PARAMS:
  -- id_waba                  : int
  -- phone_number_id          : str
  -- display_phone_number     : str | None
  -- verified_name            : str | None
  -- platform_type            : str | None
  -- metadata                 : Jsonb | None

INSERT INTO wa_agents.api_phone_numbers AS num (
  business,
  phone_number_id,
  display_phone_number,
  verified_name,
  platform_type,
  is_on_biz_app,
  metadata
)
VALUES (
  @id_waba,
  @phone_number_id::wa_agents.T_NO_WS_STR,
  COALESCE(
    @display_phone_number::TEXT,
    @phone_number_id::TEXT
  )::wa_agents.T_NO_WS_STR,
  @verified_name,
  @platform_type,
  ( @metadata::JSONB->>'is_on_biz_app' )::BOOLEAN,
  @metadata
)
-- Never transfer an existing registration to a different WABA or telephone number.
ON CONFLICT
  ( phone_number_id )
DO
  UPDATE
SET
  verified_name            = EXCLUDED.verified_name,
  platform_type            = EXCLUDED.platform_type,
  is_on_biz_app            = EXCLUDED.is_on_biz_app,
  metadata                 = COALESCE( EXCLUDED.metadata, num.metadata ),
  last_seen_at             = now()
WHERE
  ( num.business             = EXCLUDED.business             ) AND
  ( num.display_phone_number = EXCLUDED.display_phone_number )
RETURNING
  num.id,
  num.phone_number_id,
  num.platform_type,
  num.is_on_biz_app;
