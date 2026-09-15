-- PARAMS:
  -- payload_id : wa_api_inbound_payloads.id
  -- item_idx   : int      | None
  -- item_ts    : datetime | None
  -- change_idx : int      | None
  -- contact    : wa_api_contacts.id

UPDATE
  public.wa_api_inbound_payloads AS pay
SET
  item_idx   = COALESCE( @item_idx,   0     ),
  item_ts    = COALESCE( @item_ts,    now() ),
  change_idx = COALESCE( @change_idx, 0     ),
  contact    = @contact
WHERE
  ( pay.id = @payload_id )
RETURNING
  pay.id,
  pay.received_at,
  pay.item_idx,
  pay.item_ts,
  pay.change_idx,
  pay.contact,
  pay.data_hash,
  pay.validated,
  pay.errors;
