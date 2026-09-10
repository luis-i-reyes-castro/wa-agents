-- WARNING:
-- This deletes all wa-agents data from the configured Supabase database and
-- resets identity counters. Run manually only when you intentionally want a
-- clean test database.

TRUNCATE TABLE
  public.wa_case_handler_to_api,
  public.wa_case_handler_messages,
  public.wa_case_handler_contact_leases,
  public.wa_case_handler_case_manifests,
  public.wa_api_to_case_handler_queue,
  public.wa_api_statuses,
  public.wa_api_media,
  public.wa_api_outbound_messages,
  public.wa_api_inbound_messages,
  public.wa_api_inbound_payload_metadata,
  public.wa_api_inbound_payloads,
  public.wa_api_contact_profiles,
  public.wa_api_contacts,
  public.wa_api_businesses
RESTART IDENTITY
CASCADE;
