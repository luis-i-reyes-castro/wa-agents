-- PARAMS:
  -- operator_id : str
  -- user_id     : str

SELECT
    data
FROM
    wa_users
WHERE
    ( operator_id = @operator_id ) AND
    ( user_id     = @user_id     );
