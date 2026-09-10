-- PARAMS:
  -- item_idx     : int      | None
  -- item_ts      : datetime | None
  -- change_idx   : int      | None
  -- payload_id   : wa_api_inbound_payloads.id
  -- contact      : wa_api_contacts.id

INSERT INTO public.wa_api_inbound_payload_metadata (
  item_idx,
  item_ts,
  change_idx,
  payload_id,
  contact
)
VALUES (
  COALESCE( @item_idx,   0    ),
  COALESCE( @item_ts,    now()),
  COALESCE( @change_idx, 0    ),
  @payload_id,
  @contact
)
RETURNING
  id,
  received_at,
  item_idx,
  item_ts,
  change_idx,
  payload_id,
  contact;
