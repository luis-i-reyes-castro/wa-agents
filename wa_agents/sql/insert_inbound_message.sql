-- PARAMS:
  -- payload  : wa_api_inbound_payload_metadata.id
  -- is_echo  : bool | None
  -- msg_id   : WhatsAppMessageID
  -- msg_ts   : datetime
  -- msg_type : WhatsAppMessageType
  -- msg_data : dict

INSERT INTO public.wa_api_inbound_messages (
  payload,
  is_echo,
  msg_id,
  msg_ts,
  msg_type,
  msg_data
)
VALUES (
  @payload,
  COALESCE( @is_echo, FALSE),
  @msg_id,
  @msg_ts,
  @msg_type,
  @msg_data
)
ON CONFLICT (msg_id)
DO
  NOTHING
RETURNING
  id,
  payload,
  is_echo,
  msg_id,
  msg_ts,
  msg_type,
  msg_data;
