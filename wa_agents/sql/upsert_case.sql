-- PARAMS:
  -- operator_id       : str
  -- user_id           : str
  -- case_id           : int
  -- model             : str | None
  -- status            : str
  -- time_opened       : datetime
  -- time_last_message : datetime | None
  -- time_closed       : datetime | None

INSERT INTO wa_cases (
  operator_id,
  user_id,
  case_id,
  model,
  status,
  time_opened,
  time_last_message,
  time_closed
)
VALUES (
  @operator_id,
  @user_id,
  @case_id,
  @model,
  @status,
  @time_opened,
  @time_last_message,
  @time_closed
)
ON CONFLICT (operator_id, user_id, case_id)
DO
  UPDATE
SET
  model             = EXCLUDED.model,
  status            = EXCLUDED.status,
  time_opened       = EXCLUDED.time_opened,
  time_last_message = EXCLUDED.time_last_message,
  time_closed       = EXCLUDED.time_closed,
  updated_at        = now();
