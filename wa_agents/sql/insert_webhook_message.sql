-- PARAMS:
  -- payload_id           : int
  -- operator_id          : str
  -- display_phone_number : str
  -- waba_id              : str
  -- user_id              : str
  -- message_id           : str
  -- message_type         : str
  -- timestamp            : datetime | None
  -- media_id             : str | None
  -- media_mime_type      : str | None
  -- context_message_id   : str | None
  -- payload              : dict

INSERT INTO wa_webhook_messages (
  payload_id,
  operator_id,
  display_phone_number,
  waba_id,
  user_id,
  message_id,
  message_type,
  timestamp,
  media_id,
  media_mime_type,
  context_message_id,
  payload
)
VALUES (
  @payload_id,
  @operator_id,
  @display_phone_number,
  @waba_id,
  @user_id,
  @message_id,
  @message_type,
  @timestamp,
  @media_id,
  @media_mime_type,
  @context_message_id,
  @payload
);
