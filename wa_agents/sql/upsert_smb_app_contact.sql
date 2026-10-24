-- PARAMS:
  -- phone_number : wa_api_phone_numbers.id
  -- contact      : wa_api_contacts.id
  -- full_name    : str | None
  -- first_name   : str | None
  -- is_active    : bool
  -- changed_at   : datetime

WITH sync AS (
  INSERT INTO public.wa_api_embedded_signup_syncs AS syn (
    phone_number,
    sync_type,
    sync_status,
    started_at
  )
  VALUES (
    @phone_number,
    'smb_app_state_sync',
    'receiving',
    now()
  )
  ON CONFLICT ( phone_number, sync_type)
  DO
    UPDATE
  SET
    updated_at  = now(),
    sync_status = (
      CASE
        WHEN syn.sync_status = 'completed' THEN 'completed'
        ELSE 'receiving'
      END
    )::T_WHATSAPP_APP_DATA_SYNC_STATUS,
    started_at  = COALESCE( syn.started_at, now()),
    last_error  = NULL
  RETURNING
    syn.id
),
upserted AS (
  INSERT INTO public.wa_api_smb_app_contacts AS smb (
    contact,
    full_name,
    first_name,
    is_active,
    changed_at
  )
  SELECT
    @contact,
    @full_name,
    @first_name,
    @is_active,
    @changed_at
  FROM
    sync
  ON CONFLICT (contact)
  DO
    UPDATE
  SET
    updated_at = now(),
    full_name  = EXCLUDED.full_name,
    first_name = EXCLUDED.first_name,
    is_active  = EXCLUDED.is_active,
    changed_at = EXCLUDED.changed_at
  WHERE
    smb.changed_at <= EXCLUDED.changed_at
  RETURNING
    smb.contact,
    smb.full_name,
    smb.first_name,
    smb.is_active,
    smb.changed_at
)
(
  SELECT
    upd.contact,
    upd.full_name,
    upd.first_name,
    upd.is_active,
    upd.changed_at
  FROM
    upserted AS upd
)
UNION ALL
(
  SELECT
    smb.contact,
    smb.full_name,
    smb.first_name,
    smb.is_active,
    smb.changed_at
  FROM
    public.wa_api_smb_app_contacts AS smb
  WHERE
    smb.contact = @contact
    AND
    NOT EXISTS ( SELECT 1 FROM upserted )
  LIMIT 1
);
