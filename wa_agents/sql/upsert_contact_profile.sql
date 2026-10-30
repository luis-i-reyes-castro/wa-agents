-- PARAMS:
  -- contact          : api_contacts.id
  -- profile_name     : str
  -- profile_username : WhatsAppUsername | None

WITH CTE_updated AS (
  UPDATE
    wa_agents.api_contact_profiles AS pro
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
CTE_inserted AS (
  INSERT INTO wa_agents.api_contact_profiles (
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
      CTE_updated
  )
  ON CONFLICT ON CONSTRAINT api_contact_profile_unique
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
  CTE_updated AS upd
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
  CTE_inserted AS ins
LIMIT 1
);
