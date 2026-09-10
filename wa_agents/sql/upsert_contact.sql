-- PARAMS:
  -- business : wa_api_businesses.id
  -- wa_id    : NumericID | None
  -- user_id  : WhatsAppBSUID | None

WITH updated AS (
  UPDATE
    public.wa_api_contacts AS con
  SET
    wa_id        = COALESCE( con.wa_id,   @wa_id   ),
    user_id      = COALESCE( con.user_id, @user_id ),
    last_seen_at = now()
  WHERE
    ( con.business = @business ) AND
    (
      ( con.wa_id   = @wa_id   ) OR
      ( con.user_id = @user_id )
    )
  RETURNING
    con.id,
    con.created_at,
    con.last_seen_at,
    con.business,
    con.wa_id,
    con.user_id
),
inserted AS (
  INSERT INTO public.wa_api_contacts (
    business,
    wa_id,
    user_id
  )
  SELECT
    @business,
    @wa_id,
    @user_id
  WHERE NOT EXISTS (
    SELECT
      1
    FROM
      updated
  )
  ON CONFLICT
  DO
    NOTHING
  RETURNING
    id,
    created_at,
    last_seen_at,
    business,
    wa_id,
    user_id
)
(
SELECT
  upd.id,
  upd.created_at,
  upd.last_seen_at,
  upd.business,
  upd.wa_id,
  upd.user_id
FROM
  updated AS upd
)
UNION ALL
(
SELECT
  ins.id,
  ins.created_at,
  ins.last_seen_at,
  ins.business,
  ins.wa_id,
  ins.user_id
FROM
  inserted AS ins
LIMIT 1
);
