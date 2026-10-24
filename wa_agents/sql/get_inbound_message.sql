-- PARAMS:
  -- msg_id : WhatsAppMessageID

SELECT
  num.id                    AS business,
  bus.waba_id               AS waba_id,
  num.phone_number_id       AS phone_number_id,
  num.display_phone_number  AS display_phone_number,
  con.wa_id                 AS wa_id,
  con.user_id               AS user_id,
  msg.contact               AS contact,
  msg.id                    AS id,
  msg.payload               AS payload,
  msg.is_echo               AS is_echo,
  msg.msg_id                AS msg_id,
  msg.msg_ts                AS msg_ts,
  msg.msg_type              AS msg_type,
  msg.msg_data              AS msg_data,
  msg.history_chunk         AS history_chunk,
  msg.history_context       AS history_context,
  pro.profile_name          AS profile_name,
  pro.profile_username      AS profile_username
FROM
  wa_agents.api_inbound_messages AS msg
JOIN
  wa_agents.api_contacts AS con ON con.id = msg.contact
JOIN
  wa_agents.api_phone_numbers AS num ON num.id = con.phone_number
JOIN
  wa_agents.api_businesses AS bus ON bus.id = num.business
LEFT JOIN LATERAL (
  SELECT
    prf.profile_name,
    prf.profile_username
  FROM
    wa_agents.api_contact_profiles AS prf
  WHERE
    ( prf.contact = con.id )
  ORDER BY
    prf.last_seen_at DESC,
    prf.id           DESC
  LIMIT 1
) AS pro ON TRUE
WHERE
  ( msg.msg_id = @msg_id );
