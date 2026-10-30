-- PARAMS:
  -- contact         : api_contacts.id
  -- payload         : api_inbound_payloads.id
  -- is_echo         : bool | None
  -- msg_id          : WhatsAppMessageID
  -- msg_ts          : datetime
  -- msg_type        : WhatsAppMessageType
  -- msg_data        : dict
  -- history_chunk   : api_history_chunks.id | None
  -- history_context : dict | None

INSERT INTO wa_agents.api_inbound_messages AS msg (
  contact,
  payload,
  is_echo,
  msg_id,
  msg_ts,
  msg_type,
  msg_data,
  history_chunk,
  history_context
)
VALUES (
  @contact,
  @payload,
  COALESCE( @is_echo, FALSE),
  @msg_id,
  @msg_ts,
  @msg_type,
  @msg_data,
  @history_chunk,
  @history_context
)
ON CONFLICT (msg_id)
DO
  UPDATE
SET
  contact         = EXCLUDED.contact,
  payload         = EXCLUDED.payload,
  is_echo         = EXCLUDED.is_echo,
  msg_ts          = EXCLUDED.msg_ts,
  msg_type        = EXCLUDED.msg_type,
  msg_data        = EXCLUDED.msg_data,
  history_chunk   = EXCLUDED.history_chunk,
  history_context = EXCLUDED.history_context
WHERE
  ( msg.contact = EXCLUDED.contact     ) AND
  ( msg.history_chunk IS NOT NULL      ) AND
  ( EXCLUDED.history_chunk IS NOT NULL ) AND
  ( msg.msg_type = 'media_placeholder' ) AND
  ( EXCLUDED.msg_type <> 'media_placeholder' )
RETURNING
  msg.id,
  msg.contact,
  msg.payload,
  msg.is_echo,
  msg.msg_id,
  msg.msg_ts,
  msg.msg_type,
  msg.msg_data,
  msg.history_chunk,
  msg.history_context;
