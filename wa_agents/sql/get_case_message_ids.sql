-- PARAMS:
  -- operator_id : str
  -- user_id     : str
  -- case_id     : int

SELECT
  message_id
FROM
  wa_messages
WHERE
  ( operator_id = @operator_id ) AND
  ( user_id     = @user_id     ) AND
  ( case_id     = @case_id     )
ORDER BY
  time_created  ASC,
  time_received ASC,
  id            ASC;
