-- PARAMS:
  -- fallback_handler_key : str | None

UPDATE
  wa_agents.api_to_case_handler_queue AS que
SET
  msg_status = 'ignored'::T_ENQUEUED_MESSAGE_STATUS,
  updated_at = now()
WHERE
  ( @fallback_handler_key::text IS NULL ) AND
  ( que.msg_status = 'pending'          ) AND
  ( que.handler_id IS NULL              );
