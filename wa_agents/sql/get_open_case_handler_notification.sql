-- PARAMS:
  -- msg_id            : WhatsAppMessageID
  -- recipient_contact : api_contacts.id
  -- notification_key  : str

SELECT
  ntf.id                AS id,
  ntf.notification_key  AS notification_key,
  subject.wa_id         AS subject_wa_id,
  pro.profile_name      AS subject_name
FROM
  wa_agents.api_contacts               AS subject
LEFT JOIN LATERAL (
  SELECT
    prf.profile_name
  FROM
    wa_agents.api_contact_profiles AS prf
  WHERE
    ( subject.id = prf.contact )
  ORDER BY
    prf.last_seen_at DESC,
    prf.id           DESC
  LIMIT
    1
)                                      AS pro ON TRUE
JOIN
  wa_agents.api_outbound_messages      AS out ON out.msg_id = @msg_id
JOIN
  wa_agents.case_handler_notifications AS ntf ON
    ( subject.id = ntf.subject_contact ) AND
    ( out.id     = ntf.outbound_msg_id  )
WHERE
  ( ntf.recipient_contact = @recipient_contact ) AND
  ( ntf.notification_key  = @notification_key  ) AND
  ( ntf.resolved_at       IS NULL              );
