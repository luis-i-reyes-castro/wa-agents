-- PARAMS:
  -- operator_id     : str
  -- user_id         : str
  -- case_id         : int
  -- message_id      : str
  -- idempotency_key : str
  -- basemodel       : str
  -- role            : str
  -- time_created    : datetime
  -- time_received   : datetime
  -- payload         : dict

INSERT INTO wa_messages (
    operator_id,
    user_id,
    case_id,
    message_id,
    idempotency_key,
    basemodel,
    role,
    time_created,
    time_received,
    payload
)
VALUES (
    @operator_id,
    @user_id,
    @case_id,
    @message_id,
    @idempotency_key,
    @basemodel,
    @role,
    @time_created,
    @time_received,
    @payload
)
ON CONFLICT DO NOTHING;
