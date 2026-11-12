-- PARAMS:
  -- business         : api_phone_numbers.id
  -- recipient_wa_id  : NumericID
  -- notification_key : str
  -- case_manifest    : case_handler_case_manifests.id
  -- case_message     : case_handler_messages.id | None
  -- subject_contact  : api_contacts.id
  -- msg_id           : WhatsAppMessageID
  -- msg_type         : WhatsAppMessageType
  -- msg_data         : dict

WITH CTE_updated AS (
  UPDATE
    wa_agents.api_contacts AS con
  SET
    wa_id        = COALESCE( con.wa_id, @recipient_wa_id ),
    last_seen_at = now()
  WHERE
    ( con.phone_number = @business        ) AND
    ( con.wa_id        = @recipient_wa_id )
  RETURNING
    con.id
),
CTE_inserted AS (
  INSERT INTO wa_agents.api_contacts (
    phone_number,
    wa_id
  )
  SELECT
    @business,
    @recipient_wa_id
  WHERE NOT EXISTS (
    SELECT
      1
    FROM
      CTE_updated
  )
  ON CONFLICT
  DO
    NOTHING
  RETURNING
    id
),
CTE_recipient AS (
  SELECT
    upd.id AS id
  FROM
    CTE_updated AS upd
  UNION ALL
  SELECT
    ins.id AS id
  FROM
    CTE_inserted AS ins
  LIMIT
    1
),
CTE_outbound AS (
  INSERT INTO wa_agents.api_outbound_messages (
    contact,
    msg_id,
    msg_type,
    msg_data
  )
  SELECT
    recipient.id,
    @msg_id,
    @msg_type::T_WHATSAPP_MESSAGE,
    @msg_data
  FROM
    CTE_recipient AS recipient
  ON CONFLICT (msg_id)
  DO
    NOTHING
  RETURNING
    id,
    contact
),
CTE_notification AS (
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
    outbound.contact,
    outbound.id
  FROM
    CTE_outbound AS outbound
  RETURNING
    id,
    created_at,
    notification_key,
    case_manifest,
    case_message,
    subject_contact,
    recipient_contact,
    outbound_msg_id
)
SELECT
  ntf.id                 AS id,
  ntf.created_at         AS created_at,
  ntf.notification_key   AS notification_key,
  ntf.case_manifest      AS case_manifest,
  ntf.case_message       AS case_message,
  ntf.subject_contact    AS subject_contact,
  ntf.recipient_contact  AS recipient_contact,
  ntf.outbound_msg_id    AS outbound_msg_id
FROM
  CTE_notification AS ntf;
