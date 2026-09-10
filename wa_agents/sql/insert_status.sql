-- PARAMS:
  -- payload      : wa_api_inbound_payload_metadata.id
  -- msg_id       : WhatsAppMessageID
  -- msg_status   : WhatsAppStatus
  -- status_ts    : datetime
  -- conversation : dict | None
  -- pricing      : dict | None
  -- errors       : list[dict] | None

INSERT INTO public.wa_api_statuses (
  payload,
  msg_id,
  msg_status,
  status_ts,
  conversation,
  pricing,
  errors
)
VALUES (
  @payload,
  @msg_id,
  @msg_status,
  @status_ts,
  @conversation,
  @pricing,
  @errors
)
RETURNING
  id,
  payload,
  msg_id,
  msg_status,
  status_ts,
  conversation,
  pricing,
  errors;
