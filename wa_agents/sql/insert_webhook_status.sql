-- PARAMS:
  -- payload_id           : int
  -- operator_id          : str
  -- display_phone_number : str
  -- waba_id              : str
  -- recipient_id         : str
  -- message_id           : str
  -- status               : str
  -- timestamp            : datetime | None
  -- conversation_id      : str | None
  -- pricing_category     : str | None
  -- payload              : dict

INSERT INTO wa_webhook_statuses (
  payload_id,
  operator_id,
  display_phone_number,
  waba_id,
  recipient_id,
  message_id,
  status,
  timestamp,
  conversation_id,
  pricing_category,
  payload
)
VALUES (
  @payload_id,
  @operator_id,
  @display_phone_number,
  @waba_id,
  @recipient_id,
  @message_id,
  @status,
  @timestamp,
  @conversation_id,
  @pricing_category,
  @payload
);
