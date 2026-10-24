-- PARAMS:
  -- waba_id  : NumericID
  -- flow_key : stable application flow key

SELECT
  flow.*
FROM
  wa_agents.api_flows AS flow
INNER JOIN
  wa_agents.api_flow_encryption_keys AS fw
  ON ( fw.id = flow.flow_waba )
INNER JOIN
  wa_agents.api_businesses AS bus
  ON ( bus.id = fw.business )
WHERE
  ( bus.waba_id = @waba_id ) AND
  ( flow.flow_key = @flow_key ) AND
  flow.is_active
ORDER BY
  flow.updated_at DESC,
  flow.id DESC
LIMIT 1;
