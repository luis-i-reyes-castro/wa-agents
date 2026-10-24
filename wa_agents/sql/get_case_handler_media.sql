-- PARAMS:
  -- case_handler_msg_id : case_handler_messages.id

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
  wa_agents.case_handler_to_api_bridge AS map
JOIN
  wa_agents.api_media AS med ON
    ( med.inbound_msg_id  = map.api_inbound_msg_id  ) OR
    ( med.outbound_msg_id = map.api_outbound_msg_id )
WHERE
  ( map.case_handler_msg_id = @case_handler_msg_id )
ORDER BY
  med.id ASC;
