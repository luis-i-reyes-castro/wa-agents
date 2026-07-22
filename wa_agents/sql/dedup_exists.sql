-- PARAMS:
  -- operator_id     : str
  -- user_id         : str
  -- idempotency_key : str

SELECT
  1
FROM
  wa_messages
WHERE
  ( operator_id     = @operator_id     ) AND
  ( user_id         = @user_id         ) AND
  ( idempotency_key = @idempotency_key )
LIMIT 1;
