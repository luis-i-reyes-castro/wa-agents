-- PARAMS:
  -- msg_id : WhatsAppMessageID

SELECT
  bus.id                    AS business,
  bus.waba_id               AS waba_id,
  bus.phone_number_id       AS phone_number_id,
  bus.display_phone_number  AS display_phone_number,
  con.wa_id                 AS wa_id,
  con.user_id               AS user_id,
  pay.contact               AS contact,
  msg.id                    AS id,
  msg.payload               AS payload,
  msg.is_echo               AS is_echo,
  msg.msg_id                AS msg_id,
  msg.msg_ts                AS msg_ts,
  msg.msg_type              AS msg_type,
  msg.msg_data              AS msg_data,
  pro.profile_name          AS profile_name,
  pro.profile_username      AS profile_username
FROM
  public.wa_api_businesses       AS bus
JOIN
  public.wa_api_contacts         AS con ON con.business = bus.id
JOIN
  public.wa_api_inbound_payloads AS pay ON pay.contact = con.id
JOIN
  public.wa_api_inbound_messages AS msg ON msg.payload = pay.id
LEFT JOIN LATERAL (
  SELECT
    prf.profile_name,
    prf.profile_username
  FROM
    public.wa_api_contact_profiles AS prf
  WHERE
    ( prf.contact = con.id )
  ORDER BY
    prf.last_seen_at DESC,
    prf.id           DESC
  LIMIT 1
) AS pro ON TRUE
WHERE
  ( msg.msg_id = @msg_id );
