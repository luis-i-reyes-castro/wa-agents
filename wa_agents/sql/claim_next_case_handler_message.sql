-- PARAMS:
  -- owner_token : UUID

WITH candidate AS MATERIALIZED (
  SELECT
    que.id,
    que.msg_id,
    pmd.contact
  FROM
    public.wa_api_to_case_handler_queue AS que
  JOIN
    public.wa_api_inbound_messages AS msg ON
      ( msg.msg_id = que.msg_id )
  JOIN
    public.wa_api_inbound_payload_metadata AS pmd ON
      ( pmd.id = msg.payload )
  WHERE
    ( que.msg_status = 'pending' ) AND
    NOT EXISTS (
      SELECT
        1
      FROM
        public.wa_case_handler_contact_leases AS old
      WHERE
        ( old.contact    = pmd.contact ) AND
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
  que.id         AS row_id,
  que.msg_id     AS msg_id,
  que.msg_status AS msg_status,
  can.contact    AS contact,
  lea.owner_token;
