-- PARAMS:
  -- contact  : api_contacts.id
  -- msg_id   : WhatsAppMessageID
  -- msg_type : WhatsAppMessageType
  -- msg_data : dict

INSERT INTO wa_agents.api_outbound_messages (
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
