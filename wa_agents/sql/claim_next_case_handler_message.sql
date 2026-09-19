-- PARAMS:
  -- owner_token          : UUID
  -- handler_keys         : list[str]
  -- fallback_handler_key : str

WITH candidate AS MATERIALIZED (
  SELECT
    pay.contact,
    que.id,
    que.handler_id,
    que.msg_id,
    COALESCE( rou.handler_key, @fallback_handler_key ) AS handler_key
  FROM
    public.wa_api_businesses            AS bus
  JOIN
    public.wa_api_contacts              AS con ON bus.id = con.business
  JOIN
    public.wa_api_inbound_payloads      AS pay ON con.id = pay.contact
  JOIN
    public.wa_api_inbound_messages      AS msg ON pay.id = msg.payload
  JOIN
    public.wa_api_to_case_handler_queue AS que ON que.msg_id = msg.msg_id
  LEFT JOIN
    public.wa_case_handler_routes       AS rou ON rou.id = que.handler_id
  WHERE
    ( que.msg_status = 'pending' ) AND
    (
      COALESCE( rou.handler_key, @fallback_handler_key ) = ANY( @handler_keys )
    ) AND
    NOT EXISTS (
      SELECT
        1
      FROM
        public.wa_case_handler_contact_leases AS old
      WHERE
        ( old.contact    = pay.contact ) AND
        ( old.expires_at > now()       )
    )
  ORDER BY
    que.created_at ASC,
    que.id         ASC
  LIMIT 1
  FOR UPDATE OF que SKIP LOCKED
),
leased AS (
  INSERT INTO public.wa_case_handler_contact_leases (
    contact,
    owner_token,
    acquired_at,
    heartbeat_at,
    expires_at
  )
  SELECT
    can.contact,
    @owner_token,
    now(),
    now(),
    now() + INTERVAL '90 seconds'
  FROM
    candidate AS can
  ON CONFLICT (contact)
  DO
    UPDATE
  SET
    owner_token  = EXCLUDED.owner_token,
    acquired_at  = EXCLUDED.acquired_at,
    heartbeat_at = EXCLUDED.heartbeat_at,
    expires_at   = EXCLUDED.expires_at
  WHERE
    ( wa_case_handler_contact_leases.expires_at <= now() )
  RETURNING
    contact,
    owner_token
)
UPDATE
  public.wa_api_to_case_handler_queue AS que
SET
  msg_status = 'processing',
  updated_at = now()
FROM
  candidate AS can
JOIN
  leased AS lea ON
    ( lea.contact = can.contact )
WHERE
  ( que.id = can.id )
RETURNING
  can.contact     AS contact,
  lea.owner_token AS owner_token,
  que.id          AS row_id,
  can.handler_id  AS handler_id,
  can.handler_key AS handler_key,
  que.msg_id      AS msg_id,
  que.msg_status  AS msg_status;
