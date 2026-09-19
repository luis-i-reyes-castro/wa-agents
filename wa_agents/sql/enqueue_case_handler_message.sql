-- PARAMS:
  -- msg_id : WhatsAppMessageID

INSERT INTO public.wa_api_to_case_handler_queue (
  msg_id,
  handler_id
)
(
SELECT
  msg.msg_id,
  rou.id
FROM
  public.wa_api_businesses       AS bus
JOIN
  public.wa_api_contacts         AS con ON con.business = bus.id
JOIN
  public.wa_api_inbound_payloads AS pay ON pay.contact = con.id
JOIN
  public.wa_api_inbound_messages AS msg ON msg.payload = pay.id
LEFT JOIN LATERAL (
  SELECT
    rte.id
  FROM
    public.wa_case_handler_routes AS rte
  WHERE
    ( rte.business = bus.id ) AND
    (
      ( rte.contact = con.id ) OR
      ( rte.contact IS NULL  )
    )
  ORDER BY
    ( rte.contact IS NOT NULL ) DESC
  LIMIT 1
) AS rou ON TRUE
WHERE
  msg.msg_id = @msg_id
)
ON CONFLICT
  (msg_id)
DO
  NOTHING
RETURNING
  id,
  created_at,
  updated_at,
  last_error_at,
  handler_id,
  msg_id,
  msg_status;
