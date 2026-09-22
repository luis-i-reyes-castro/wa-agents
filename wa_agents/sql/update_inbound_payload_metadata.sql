-- PARAMS:
  -- payload_id   : wa_api_inbound_payloads.id
  -- item_index   : int      | None
  -- item_ts      : datetime | None
  -- change_index : int      | None
  -- contact      : wa_api_contacts.id

UPDATE
  public.wa_api_inbound_payloads AS pay
SET
  item_index   = COALESCE( @item_index,   0     ),
  item_ts      = COALESCE( @item_ts,      now() ),
  change_index = COALESCE( @change_index, 0     ),
  contact      = @contact
WHERE
  ( pay.id = @payload_id )
RETURNING
  pay.id,
  pay.received_at,
  pay.item_index,
  pay.item_ts,
  pay.change_index,
  pay.contact,
  pay.data_hash,
  pay.validated,
  pay.errors;
