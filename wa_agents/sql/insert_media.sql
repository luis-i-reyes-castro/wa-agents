-- PARAMS:
  -- inbound_msg_id  : api_inbound_messages.id  | None
  -- outbound_msg_id : api_outbound_messages.id | None
  -- mime_type       : MIME_Type
  -- size            : int
  -- object_key      : str
  -- caption         : str | None
  -- filename        : str | None

INSERT INTO wa_agents.api_media (
  inbound_msg_id,
  outbound_msg_id,
  mime_type,
  size,
  object_key,
  caption,
  filename
)
VALUES (
  @inbound_msg_id,
  @outbound_msg_id,
  @mime_type,
  @size,
  @object_key,
  @caption,
  @filename
)
ON CONFLICT
DO
  NOTHING
RETURNING
  id,
  inbound_msg_id,
  outbound_msg_id,
  mime_type,
  size,
  object_key,
  caption,
  filename;
