-- PARAMS:
  -- phone_number : wa_api_phone_numbers.id

UPDATE
  public.wa_api_embedded_signup_syncs AS syn
SET
  updated_at   = now(),
  sync_status  = 'completed',
  started_at   = COALESCE( syn.started_at, now()),
  completed_at = COALESCE( syn.completed_at, now()),
  last_error   = NULL
WHERE
  syn.phone_number = @phone_number
  AND
  syn.sync_type = 'smb_app_state_sync'
RETURNING
  syn.id,
  syn.sync_status,
  syn.completed_at;
