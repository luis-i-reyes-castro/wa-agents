-- WARNING:
-- This permanently deletes all database state associated with every WhatsApp
-- contact under one phone number while preserving the contacts and their routes.
--
-- Replace DISPLAY_PHONE_NUMBER before running this script manually in the
-- Supabase SQL editor.
--
-- Preserved:
--   - WhatsApp businesses
--   - WhatsApp phone numbers
--   - all contacts
--   - WhatsApp Business app contact metadata
--   - all case-handler routes
--   - Flow WABA provisioning keys
--   - Flow definitions
--   - WABA-level Flow health exchanges and webhook events
--
-- Deleted:
--   - contact profiles
--   - inbound/outbound message rows, statuses, media metadata, and queue rows
--   - case manifests, messages, state histories, contexts, and leases
--   - Flow sessions and their session-bound data exchanges
--
-- This does not delete media bytes from external object storage or messages
-- from users' WhatsApp clients.

DO $$
DECLARE
  v_display_phone_number public.T_NO_WS_STR := '<DISPLAY_PHONE_NUMBER>';
  v_contacts             BIGINT[];
  v_matching_contacts    BIGINT;
BEGIN
  IF v_display_phone_number = '<DISPLAY_PHONE_NUMBER>' THEN
    RAISE EXCEPTION 'Replace the display phone number placeholder before running';
  END IF;
  
  IF NOT EXISTS (
    SELECT 1
    FROM wa_agents.api_phone_numbers AS phone
    WHERE ( phone.display_phone_number = v_display_phone_number )
  ) THEN
    RAISE EXCEPTION 'Expected to find phone number %, found none',
      v_display_phone_number;
  END IF;
  
  SELECT
    array_agg(contact.id ORDER BY contact.id),
    count(*)
  INTO
    v_contacts,
    v_matching_contacts
  FROM
    wa_agents.api_contacts AS contact
  JOIN
    wa_agents.api_phone_numbers AS phone ON phone.id = contact.phone_number
  WHERE
    ( phone.display_phone_number = v_display_phone_number );
  
  IF v_matching_contacts = 0 THEN
    RAISE EXCEPTION
      'Expected to find at least one contact for phone number %, found none',
      v_display_phone_number;
  END IF;
  
  DELETE FROM wa_agents.api_flow_sessions
  WHERE ( contact = ANY( v_contacts) );
  
  DELETE FROM wa_agents.case_handler_case_manifests
  WHERE ( contact = ANY( v_contacts) );
  
  DELETE FROM wa_agents.case_handler_contact_leases
  WHERE ( contact = ANY( v_contacts) );
  
  DELETE FROM wa_agents.api_statuses AS status
  WHERE
    EXISTS (
      SELECT 1
      FROM wa_agents.api_inbound_messages AS message
      WHERE
        ( message.contact = ANY( v_contacts) ) AND
        ( message.msg_id  = status.msg_id   )
    ) OR
    EXISTS (
      SELECT 1
      FROM wa_agents.api_outbound_messages AS message
      WHERE
        ( message.contact = ANY( v_contacts) ) AND
        ( message.msg_id  = status.msg_id   )
    );
  
  DELETE FROM wa_agents.api_outbound_messages
  WHERE ( contact = ANY( v_contacts) );
  
  DELETE FROM wa_agents.api_inbound_messages
  WHERE ( contact = ANY( v_contacts) );
  
  DELETE FROM wa_agents.api_inbound_payloads
  WHERE ( contact = ANY( v_contacts) );
  
  DELETE FROM wa_agents.api_contact_profiles
  WHERE ( contact = ANY( v_contacts) );
END;
$$;
