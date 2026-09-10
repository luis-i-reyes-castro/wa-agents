-- PARAMS:
  -- contact  : wa_api_contacts.id
  -- msg_id   : WhatsAppMessageID
  -- msg_type : WhatsAppMessageType
  -- msg_data : dict

INSERT INTO public.wa_api_outbound_messages (
  contact,
  msg_id,
  msg_type,
  msg_data
)
VALUES (
  @contact,
  @msg_id,
  @msg_type,
  @msg_data
)
ON CONFLICT (msg_id)
DO
  NOTHING
RETURNING
  id,
  sent_at,
  contact,
  msg_id,
  msg_type,
  msg_data;
