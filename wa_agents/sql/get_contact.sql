-- PARAMS:
  -- contact : wa_api_contacts.id

SELECT
  con.id,
  con.created_at,
  con.last_seen_at,
  con.business,
  con.wa_id,
  con.user_id,
  pro.profile_name,
  pro.profile_username
FROM
  public.wa_api_contacts AS con
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
  ( con.id = @contact );
