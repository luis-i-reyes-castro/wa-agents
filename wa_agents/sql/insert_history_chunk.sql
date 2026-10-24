-- PARAMS:
  -- phone_number : api_phone_numbers.id
  -- payload      : api_inbound_payloads.id
  -- history_index : int
  -- phase        : int | None
  -- chunk_order  : int | None
  -- progress     : int | None
  -- errors       : list[dict] | None

WITH sync AS (
  INSERT INTO wa_agents.api_embedded_signup_syncs AS syn (
    phone_number,
    sync_type,
    sync_status,
    started_at,
    completed_at,
    last_error
  )
  VALUES (
    @phone_number,
    'history',
    (
      CASE
        WHEN @errors::JSONB IS NOT NULL THEN 'declined'
        WHEN @progress = 100             THEN 'completed'
        ELSE                                  'receiving'
      END
    )::T_WHATSAPP_APP_DATA_SYNC_STATUS,
    now(),
    CASE WHEN @progress = 100 THEN now() ELSE NULL END,
    CASE
      WHEN @errors::JSONB IS NULL THEN NULL
      ELSE jsonb_build_object( 'errors', @errors::JSONB)
    END
  )
  ON CONFLICT ( phone_number, sync_type)
  DO
    UPDATE
  SET
    updated_at   = now(),
    sync_status  = CASE
      WHEN EXCLUDED.sync_status = 'declined'  THEN 'declined'
      WHEN EXCLUDED.sync_status = 'completed' THEN 'completed'
      WHEN syn.sync_status IN ( 'completed', 'declined' ) THEN syn.sync_status
      ELSE 'receiving'
    END,
    started_at   = COALESCE( syn.started_at, EXCLUDED.started_at),
    completed_at = COALESCE( syn.completed_at, EXCLUDED.completed_at),
    last_error   = COALESCE( EXCLUDED.last_error, syn.last_error)
  RETURNING
    syn.id
)
INSERT INTO wa_agents.api_history_chunks AS chunk (
  sync,
  payload,
  history_index,
  phase,
  chunk_order,
  progress,
  errors
)
SELECT
  syn.id,
  @payload,
  @history_index,
  @phase,
  @chunk_order,
  @progress,
  @errors
FROM
  sync AS syn
ON CONFLICT ( payload, history_index)
DO
  UPDATE
SET
  sync        = EXCLUDED.sync,
  phase       = EXCLUDED.phase,
  chunk_order = EXCLUDED.chunk_order,
  progress    = EXCLUDED.progress,
  errors      = EXCLUDED.errors
RETURNING
  chunk.id,
  chunk.sync,
  chunk.payload,
  chunk.history_index,
  chunk.phase,
  chunk.chunk_order,
  chunk.progress,
  chunk.errors;
