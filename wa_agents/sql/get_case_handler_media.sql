-- PARAMS:
  -- case_handler_msg_id : wa_case_handler_messages.id

SELECT
  med.id,
  med.inbound_msg_id,
  med.outbound_msg_id,
  med.mime_type,
  med.size,
  med.object_key,
  med.caption,
  med.filename
FROM
  public.wa_case_handler_to_api AS map
JOIN
  public.wa_api_media AS med ON
    ( med.inbound_msg_id  = map.api_inbound_msg_id  ) OR
    ( med.outbound_msg_id = map.api_outbound_msg_id )
WHERE
  ( map.case_handler_msg_id = @case_handler_msg_id )
ORDER BY
  med.id ASC;
