-- PARAMS:
  -- id         : int
  -- last_error : Jsonb

UPDATE
  wa_agents.api_embedded_signup_syncs AS syn
SET
  updated_at   = now(),
  sync_status  = 'failed',
  completed_at = now(),
  last_error   = @last_error
WHERE
  ( syn.id          = @id          ) AND
  ( syn.sync_status = 'requesting' )
RETURNING
  syn.id;
