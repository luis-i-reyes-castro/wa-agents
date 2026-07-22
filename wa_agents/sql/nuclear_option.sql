-- WARNING:
-- This deletes all wa-agents data from the configured Supabase database and
-- resets identity counters. Run manually only when you intentionally want a
-- clean test database.

TRUNCATE TABLE
  public.wa_incoming_queue,
  public.wa_webhook_statuses,
  public.wa_webhook_messages,
  public.wa_webhook_payloads,
  public.wa_messages,
  public.wa_cases,
  public.wa_users
RESTART IDENTITY
CASCADE;
