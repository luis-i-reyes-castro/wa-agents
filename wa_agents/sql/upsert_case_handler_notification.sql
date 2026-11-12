-- PARAMS:
  -- notification_key : str
  -- case_manifest    : case_handler_case_manifests.id
  -- case_message     : case_handler_messages.id | None
  -- subject_contact  : api_contacts.id
  -- msg_id           : WhatsAppMessageID

INSERT INTO wa_agents.case_handler_notifications (
  notification_key,
  case_manifest,
  case_message,
  subject_contact,
  recipient_contact,
  outbound_msg_id
)
SELECT
  @notification_key,
  @case_manifest,
  @case_message,
  @subject_contact,
  out.contact,
  out.id
FROM
  wa_agents.api_outbound_messages AS out
WHERE
  ( out.msg_id = @msg_id )
ON CONFLICT (outbound_msg_id)
DO
  NOTHING
RETURNING
  id,
  created_at,
  notification_key,
  case_manifest,
  case_message,
  subject_contact,
  recipient_contact,
  outbound_msg_id;
