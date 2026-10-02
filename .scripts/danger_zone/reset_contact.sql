-- WARNING:
-- This permanently deletes one WhatsApp contact and all database state that
-- references it through ON DELETE CASCADE.
--
-- Replace both placeholders before running this script manually in the
-- Supabase SQL editor.
--
-- Preserved:
--   - WhatsApp businesses
--   - business-wide case-handler routes
--   - Flow WABA provisioning keys
--   - Flow definitions
--   - WABA-level Flow health exchanges and webhook events
--
-- Deleted:
--   - the contact and its profiles
--   - contact-specific case-handler routes
--   - inbound/outbound message rows, statuses, media metadata, and queue rows
--   - case manifests, messages, state histories, contexts, and leases
--   - Flow sessions and their session-bound data exchanges
--
-- This does not delete media bytes from external object storage or messages
-- from a user's WhatsApp client.

DO $$
DECLARE
  v_waba_id          public.T_NO_WS_STR := '<WABA_ID>';
  v_contact_id       public.T_NO_WS_STR := '<WA_ID_OR_USER_ID>';
  v_deleted_contacts INT;
BEGIN
  IF
    ( v_waba_id    = '<WABA_ID>'          ) OR
    ( v_contact_id = '<WA_ID_OR_USER_ID>' )
  THEN
    RAISE EXCEPTION 'Replace the WABA and contact placeholders before running';
  END IF;
  
  DELETE FROM
    public.wa_api_contacts   AS contact
  USING
    public.wa_api_businesses AS business
  WHERE
    ( business.id      = contact.business ) AND
    ( business.waba_id = v_waba_id        ) AND
    (
      ( contact.wa_id   = v_contact_id ) OR
      ( contact.user_id = v_contact_id )
    );
  
  GET DIAGNOSTICS v_deleted_contacts = ROW_COUNT;
  
  IF v_deleted_contacts <> 1 THEN
    RAISE EXCEPTION
      'Expected to delete one contact, deleted %',
      v_deleted_contacts;
  END IF;
END;
$$;
