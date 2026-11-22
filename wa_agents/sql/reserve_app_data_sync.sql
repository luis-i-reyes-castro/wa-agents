-- PARAMS:
  -- phone_number : int
  -- sync_type    : str

INSERT INTO wa_agents.api_embedded_signup_syncs (
  phone_number,
  sync_type,
  sync_status
)
VALUES (
  @phone_number,
  @sync_type::wa_agents.T_WHATSAPP_APP_DATA_SYNC_TYPE,
  'requesting'
)
ON CONFLICT ( phone_number, sync_type)
DO NOTHING
RETURNING
  id,
  sync_type;
