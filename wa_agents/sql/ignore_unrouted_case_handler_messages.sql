-- PARAMS:
  -- fallback_handler_key : str | None

UPDATE
  wa_agents.api_to_case_handler_queue AS que
SET
  msg_status = 'ignored'::T_ENQUEUED_MESSAGE_STATUS,
  updated_at = now()
WHERE
  ( que.msg_status = 'pending' ) AND
  ( que.handler_id IS NULL     ) AND
  (
    ( @fallback_handler_key::text IS NULL ) OR
    NOT EXISTS (
      SELECT
        1
      FROM
        wa_agents.case_handler_configurations AS cfg
      WHERE
        ( cfg.handler_key = @fallback_handler_key::text )
    )
  );
