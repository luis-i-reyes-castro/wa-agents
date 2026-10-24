-- PARAMS:
  -- payload    : api_inbound_payloads.id
  -- waba_id    : NumericID | null
  -- flow_id    : Meta Flow ID | null
  -- event_type : event name
  -- event_data : JSON
  -- event_at   : timestamp | null

INSERT INTO wa_agents.api_flow_events (
  payload,
  flow_waba,
  flow_id,
  event_type,
  event_data,
  event_at
)
SELECT
  @payload,
  fw.id,
  @flow_id,
  @event_type,
  @event_data,
  @event_at
FROM
  ( SELECT 1 ) AS singleton
LEFT JOIN
  wa_agents.api_flow_encryption_keys AS fw
  ON (
    fw.business = (
      SELECT
        bus.id
      FROM
        wa_agents.api_businesses AS bus
      WHERE
        ( bus.waba_id = @waba_id )
    )
  )
RETURNING *;
