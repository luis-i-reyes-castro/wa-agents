-- Close open cases for matching WhatsApp contacts without deleting any data.
--
-- Replace the contact placeholder and set BUSINESS_DISPLAY_PHONE_NUMBER to one
-- business display phone number or `all` before running this script manually in
-- the Supabase SQL editor. The contact identifier matches wa_id or user_id.
--
-- Only open case manifests are changed: is_open becomes FALSE and updated_at
-- is refreshed. Open case-handler notifications for those cases are marked
-- resolved, with no inbound reply attached. Case history, messages, contexts,
-- profiles, routes, leases, and Flow sessions are preserved. Already closed
-- cases and their resolved notifications are left unchanged.
-- Run while case-handler processing for the contact is idle so an in-flight
-- handler cannot overwrite the manual closure.

DO $$
DECLARE
  v_business_display_phone_number public.T_NO_WS_STR := '<BUSINESS_DISPLAY_PHONE_NUMBER>';
  v_contact_identifier            public.T_NO_WS_STR := '<WA_ID_OR_USER_ID>';
  v_contacts                      BIGINT[];
  v_matching_contacts             BIGINT;
  v_closed_cases                  BIGINT;
  v_resolved_notifications        BIGINT;
BEGIN
  IF
    ( v_business_display_phone_number = '<BUSINESS_DISPLAY_PHONE_NUMBER>' ) OR
    ( v_contact_identifier            = '<WA_ID_OR_USER_ID>'              )
  THEN
    RAISE EXCEPTION
      'Replace the business display phone number and contact placeholders before running';
  END IF;
  
  SELECT
    array_agg(contact.id ORDER BY contact.id),
    count(*)
  INTO
    v_contacts,
    v_matching_contacts
  FROM
    wa_agents.api_businesses    AS business
  JOIN
    wa_agents.api_phone_numbers AS phone_number ON business.id = phone_number.business
  JOIN
    wa_agents.api_contacts      AS contact      ON phone_number.id = contact.phone_number
  WHERE
    (
      ( v_business_display_phone_number   = 'all'                           ) OR
      ( phone_number.display_phone_number = v_business_display_phone_number )
    )
    AND
    (
      ( contact.wa_id   = v_contact_identifier ) OR
      ( contact.user_id = v_contact_identifier )
    );
  
  IF v_matching_contacts = 0 THEN
    RAISE EXCEPTION 'Expected to find at least one contact, found none';
  END IF;
  
  IF
    ( v_business_display_phone_number <> 'all' ) AND
    ( v_matching_contacts             <> 1     )
  THEN
    RAISE EXCEPTION
      'Expected to find one contact, found %',
      v_matching_contacts;
  END IF;

  UPDATE wa_agents.case_handler_notifications AS ntf
  SET
    resolved_at = now()
  WHERE
    ( ntf.resolved_at IS NULL ) AND
    EXISTS (
      SELECT 1
      FROM wa_agents.case_handler_case_manifests AS manifest
      WHERE
        ( manifest.id      = ntf.case_manifest ) AND
        ( manifest.contact = ANY(v_contacts)   ) AND
        manifest.is_open
    );

  GET DIAGNOSTICS v_resolved_notifications = ROW_COUNT;

  UPDATE wa_agents.case_handler_case_manifests
  SET
    is_open    = FALSE,
    updated_at = now()
  WHERE
    ( contact = ANY(v_contacts) ) AND is_open;

  GET DIAGNOSTICS v_closed_cases = ROW_COUNT;
  RAISE NOTICE
    'Closed % open case(s) and resolved % notification(s) for % matching contact(s)',
    v_closed_cases, v_resolved_notifications, v_matching_contacts;

END;
$$;
