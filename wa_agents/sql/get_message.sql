-- PARAMS:
  -- operator_id : str
  -- user_id     : str
  -- case_id     : int
  -- message_id  : str

SELECT
  payload
FROM
  wa_messages
WHERE
  ( operator_id = @operator_id ) AND
  ( user_id     = @user_id     ) AND
  ( case_id     = @case_id     ) AND
  ( message_id  = @message_id  );
