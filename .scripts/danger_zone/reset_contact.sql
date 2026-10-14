-- WARNING:
-- This permanently deletes all database state associated with matching WhatsApp
-- contacts while preserving the contacts and their case-handler routes.
--
-- Replace the contact placeholder and set WABA_ID to one WABA ID or `all` before
-- running this script manually in the Supabase SQL editor.
--
-- Preserved:
--   - WhatsApp businesses
--   - all contacts
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
-- from a user's WhatsApp client.

DO $$
DECLARE
  v_waba_id            public.T_NO_WS_STR := '<WABA_ID>';
  v_contact_identifier public.T_NO_WS_STR := '<WA_ID_OR_USER_ID>';
  v_contacts           BIGINT[];
  v_matching_contacts  BIGINT;
BEGIN
  IF
    ( v_waba_id            = '<WABA_ID>'          ) OR
    ( v_contact_identifier = '<WA_ID_OR_USER_ID>' )
  THEN
    RAISE EXCEPTION 'Replace the WABA and contact placeholders before running';
  END IF;
  
  SELECT
    array_agg(contact.id ORDER BY contact.id),
    count(*)
  INTO
    v_contacts,
    v_matching_contacts
  FROM
    public.wa_api_contacts   AS contact
  JOIN
    public.wa_api_businesses AS business ON business.id = contact.business
  WHERE
    (
      ( v_waba_id = 'all' ) OR
      ( business.waba_id = v_waba_id )
    ) AND
    (
      ( contact.wa_id   = v_contact_identifier ) OR
      ( contact.user_id = v_contact_identifier )
    );
  
  IF v_matching_contacts = 0 THEN
    RAISE EXCEPTION 'Expected to find at least one contact, found none';
  END IF;
  
  IF ( v_waba_id <> 'all' ) AND ( v_matching_contacts <> 1 ) THEN
    RAISE EXCEPTION
      'Expected to find one contact, found %',
      v_matching_contacts;
  END IF;
  
  DELETE FROM public.wa_api_flow_sessions
  WHERE ( contact = ANY( v_contacts) );
  
  DELETE FROM public.wa_case_handler_case_manifests
  WHERE ( contact = ANY( v_contacts) );
  
  DELETE FROM public.wa_case_handler_contact_leases
  WHERE ( contact = ANY( v_contacts) );
  
  DELETE FROM public.wa_api_outbound_messages
  WHERE ( contact = ANY( v_contacts) );
  
  DELETE FROM public.wa_api_inbound_payloads
  WHERE ( contact = ANY( v_contacts) );
  
  DELETE FROM public.wa_api_contact_profiles
  WHERE ( contact = ANY( v_contacts) );
END;
$$;
