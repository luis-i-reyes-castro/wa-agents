-- WARNING:
-- This deletes all wa-agents tables from the configured Supabase database.
-- Run manually only when you intentionally want to drop all tables.

DROP TABLE IF EXISTS
  public.wa_case_handler_to_api,
  public.wa_case_handler_messages,
  public.wa_case_handler_case_manifests,
  public.wa_case_handler_contact_leases,
  public.wa_api_to_case_handler_queue,
  public.wa_api_statuses,
  public.wa_api_media,
  public.wa_api_outbound_messages,
  public.wa_api_inbound_messages,
  public.wa_api_inbound_payloads,
  public.wa_api_contact_profiles,
  public.wa_api_contacts,
  public.wa_api_businesses
CASCADE;

-- Drop ENUM types
DROP TYPE IF EXISTS T_WHATSAPP_MESSAGE CASCADE;
DROP TYPE IF EXISTS T_WHATSAPP_STATUS CASCADE;
DROP TYPE IF EXISTS T_ENQUEUED_MESSAGE_STATUS CASCADE;

-- Drop DOMAIN types
DROP DOMAIN IF EXISTS T_NE_STR CASCADE;
DROP DOMAIN IF EXISTS T_NE_VAR_NAME CASCADE;
DROP DOMAIN IF EXISTS T_NUMERIC_ID CASCADE;
DROP DOMAIN IF EXISTS T_SHA256_HEX_HASH CASCADE;
DROP DOMAIN IF EXISTS T_WHATSAPP_BSUID CASCADE;
DROP DOMAIN IF EXISTS T_WHATSAPP_MESSAGE_ID CASCADE;
DROP DOMAIN IF EXISTS T_WHATSAPP_USERNAME CASCADE;
