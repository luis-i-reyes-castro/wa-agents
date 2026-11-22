-- PARAMS:
  -- id         : int
  -- request_id : str

UPDATE
  wa_agents.api_embedded_signup_syncs AS syn
SET
  updated_at   = now(),
  sync_status  = 'accepted',
  request_id   = @request_id,
  requested_at = now(),
  last_error   = NULL
WHERE
  ( syn.id          = @id          ) AND
  ( syn.sync_status = 'requesting' )
RETURNING
  syn.id;
