-- PARAMS:
  -- operator_id : str
  -- user_id     : str
  -- data        : dict

INSERT INTO wa_users (
  operator_id,
  user_id,
  data
)
VALUES (
  @operator_id,
  @user_id,
  @data
)
ON CONFLICT (operator_id, user_id) DO UPDATE
SET
  data       = EXCLUDED.data,
  updated_at = now();
