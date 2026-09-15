-- PARAMS:
  -- contact          : wa_api_contacts.id
  -- profile_name     : str
  -- profile_username : WhatsAppUsername | None

INSERT INTO public.wa_api_contact_profiles (
  contact,
  profile_name,
  profile_username
)
VALUES (
  @contact,
  @profile_name,
  @profile_username
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
  profile_username;
