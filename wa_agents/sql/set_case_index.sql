-- PARAMS:
  -- operator_id  : str
  -- user_id      : str
  -- data         : dict
  -- open_case_id : int | None

INSERT INTO wa_users (
    operator_id,
    user_id,
    data,
    open_case_id
)
VALUES (
    @operator_id,
    @user_id,
    @data,
    @open_case_id
)
ON CONFLICT (operator_id, user_id) DO UPDATE
SET
    open_case_id = EXCLUDED.open_case_id,
    updated_at   = now();
