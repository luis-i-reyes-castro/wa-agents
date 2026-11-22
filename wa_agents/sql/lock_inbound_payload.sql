-- PARAMS:
  -- payload_id : api_inbound_payloads.id

SELECT validated
FROM wa_agents.api_inbound_payloads
WHERE id = @payload_id
FOR UPDATE;
