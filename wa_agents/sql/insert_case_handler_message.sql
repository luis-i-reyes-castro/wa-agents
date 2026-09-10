-- PARAMS:
  -- case_id       : wa_case_handler_case_manifests.id
  -- ts            : datetime
  -- basemodel     : Message subclass name
  -- origin        : str | None
  -- data          : dict
  -- machine_state : str | None

WITH inserted AS (
  INSERT INTO public.wa_case_handler_messages (
    case_id,
    ts,
    basemodel,
    origin,
    data
  )
  VALUES (
    @case_id,
    @ts,
    @basemodel,
    @origin,
    @data
  )
  RETURNING
    id,
    ts,
    case_id,
    basemodel,
    origin,
    data
)
UPDATE
  public.wa_case_handler_case_manifests AS cas
SET
  updated_at    = now(),
  machine_state = @machine_state
FROM
  inserted AS msg
WHERE
  ( cas.id = msg.case_id )
RETURNING
  msg.id,
  msg.ts,
  msg.case_id,
  msg.basemodel,
  msg.origin,
  msg.data,
  cas.machine_state;
