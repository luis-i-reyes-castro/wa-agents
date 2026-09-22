-- WARNING:
-- This truncates all wa-agents tables in the configured Supabase database.
-- Run manually only when you intentionally want to reset the database.

TRUNCATE TABLE
  public.wa_case_handler_agent_contexts,
  public.wa_case_handler_to_api,
  public.wa_case_handler_messages,
  public.wa_case_handler_case_manifests,
  public.wa_case_handler_contact_leases,
  public.wa_api_to_case_handler_queue,
  public.wa_case_handler_routes,
  public.wa_api_statuses,
  public.wa_api_media,
  public.wa_api_outbound_messages,
  public.wa_api_inbound_messages,
  public.wa_api_inbound_payloads,
  public.wa_api_contact_profiles,
  public.wa_api_contacts,
  public.wa_api_businesses
RESTART IDENTITY
CASCADE;
