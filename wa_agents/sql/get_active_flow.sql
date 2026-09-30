-- PARAMS:
  -- waba_id  : NumericID
  -- flow_key : stable application flow key

SELECT
  flow.*
FROM
  public.wa_api_flows AS flow
INNER JOIN
  public.wa_api_flow_wabas AS fw
  ON ( fw.id = flow.flow_waba )
WHERE
  ( fw.waba_id = @waba_id ) AND
  ( flow.flow_key = @flow_key ) AND
  flow.is_active
ORDER BY
  flow.updated_at DESC,
  flow.id DESC
LIMIT 1;
