-- PARAMS:
  -- operator_id : str
  -- user_id     : str
  -- case_id     : int

SELECT
  case_id,
  model,
  status,
  time_opened,
  time_last_message,
  time_closed
FROM
  wa_cases
WHERE
  ( operator_id = @operator_id ) AND
  ( user_id     = @user_id     ) AND
  ( case_id     = @case_id     );
