-- PARAMS:
  -- contact          : wa_api_contacts.id
  -- profile_name     : str
  -- profile_username : WhatsAppUsername | None

WITH updated AS (
  UPDATE
    public.wa_api_contact_profiles AS pro
  SET
    last_seen_at = now()
  WHERE
    ( pro.contact      = @contact      ) AND
    ( pro.profile_name = @profile_name ) AND
    (
      pro.profile_username IS NOT DISTINCT FROM (@profile_username)
    )
  RETURNING
    pro.id,
    pro.last_seen_at,
    pro.contact,
    pro.profile_name,
    pro.profile_username
),
inserted AS (
  INSERT INTO public.wa_api_contact_profiles (
    contact,
    profile_name,
    profile_username
  )
  SELECT
    @contact,
    @profile_name,
    @profile_username
  WHERE NOT EXISTS (
    SELECT
      1
    FROM
      updated
  )
  ON CONFLICT ON CONSTRAINT wa_api_contact_profile_unique
  DO
    UPDATE
  SET
    last_seen_at = now()
  RETURNING
    id,
    last_seen_at,
    contact,
    profile_name,
    profile_username
)
(
SELECT
  upd.id,
  upd.last_seen_at,
  upd.contact,
  upd.profile_name,
  upd.profile_username
FROM
  updated AS upd
)
UNION ALL
(
SELECT
  ins.id,
  ins.last_seen_at,
  ins.contact,
  ins.profile_name,
  ins.profile_username
FROM
  inserted AS ins
LIMIT 1
);
