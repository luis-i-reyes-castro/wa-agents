-- PARAMS:
  -- contact         : api_contacts.id
  -- inbound_message : api_inbound_messages.id

SELECT
  session.*
FROM
  wa_agents.api_inbound_payloads AS payload
JOIN
  wa_agents.api_inbound_messages AS message ON payload.id      = message.payload
JOIN
  wa_agents.api_flow_sessions    AS session ON message.contact = session.contact
WHERE
  ( session.contact = @contact                ) AND
  ( message.id      = @inbound_message        ) AND
  ( session.created_at <= payload.received_at ) AND
  (
    payload.received_at < CASE
      WHEN ( session.session_status = 'completed' )
        THEN LEAST( session.completed_at, session.expires_at)
      WHEN ( session.session_status IN ( 'failed', 'superseded') )
        THEN LEAST( session.updated_at, session.expires_at)
      ELSE
        session.expires_at
    END
  )
ORDER BY
  session.created_at DESC,
  session.id         DESC
LIMIT 1;
