-- PARAMS:
  -- contact     : wa_api_contacts.id
  -- after_row_id : wa_api_to_case_handler_queue.id

SELECT
  que.id
FROM
  public.wa_api_inbound_messages      AS msg
JOIN
  public.wa_api_to_case_handler_queue AS que ON msg.msg_id = que.msg_id
WHERE
  ( msg.contact    = @contact      ) AND
  ( msg.is_echo                    ) AND
  ( que.id         > @after_row_id ) AND
  ( que.msg_status = 'pending'     )
ORDER BY
  que.id ASC
LIMIT 1;
