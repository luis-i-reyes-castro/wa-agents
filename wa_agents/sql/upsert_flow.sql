-- PARAMS:
  -- waba_id          : NumericID
  -- flow_key         : stable application flow key
  -- flow_id          : Meta Flow ID
  -- flow_name        : Meta Flow name
  -- handler_key      : registered case-handler key
  -- flow_status      : Meta Flow status
  -- flow_json_version : Flow JSON version
  -- data_api_version : endpoint data API version
  -- endpoint_uri     : endpoint URI

WITH flow_waba AS (
  SELECT
    fw.id
  FROM
    public.wa_api_flow_wabas AS fw
  WHERE
    ( fw.waba_id = @waba_id )
)
INSERT INTO public.wa_api_flows (
  flow_waba,
  flow_key,
  flow_id,
  flow_name,
  handler_key,
  flow_status,
  flow_json_version,
  data_api_version,
  endpoint_uri
)
SELECT
  flow_waba.id,
  @flow_key,
  @flow_id,
  @flow_name,
  @handler_key,
  @flow_status,
  @flow_json_version,
  @data_api_version,
  @endpoint_uri
FROM
  flow_waba
ON CONFLICT (flow_id)
DO
  UPDATE
SET
  updated_at        = now(),
  flow_key          = EXCLUDED.flow_key,
  flow_name         = EXCLUDED.flow_name,
  handler_key       = EXCLUDED.handler_key,
  flow_status       = EXCLUDED.flow_status,
  flow_json_version = EXCLUDED.flow_json_version,
  data_api_version  = EXCLUDED.data_api_version,
  endpoint_uri      = EXCLUDED.endpoint_uri,
  is_active         = TRUE
RETURNING *;
