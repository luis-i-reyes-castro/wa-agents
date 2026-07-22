-- PARAMS:
  -- operator_id : str
  -- user_id     : str

UPDATE
  wa_users
SET
  next_case_id = next_case_id + 1,
  updated_at   = now()
WHERE
  ( operator_id = @operator_id ) AND
  ( user_id     = @user_id     )
RETURNING
  next_case_id - 1 AS case_id;
