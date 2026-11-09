-- WARNING:
-- This truncates all wa-agents tables in the configured Supabase database.
-- Run manually only when you intentionally want to reset the database.

TRUNCATE TABLE
  wa_agents.api_flow_data_exchanges,
  wa_agents.api_flow_sessions,
  wa_agents.api_flow_events,
  wa_agents.api_media,
  wa_agents.case_handler_openrouter_usage,
  wa_agents.case_handler_agent_contexts,
  wa_agents.case_handler_state_histories,
  wa_agents.case_handler_to_api_bridge,
  wa_agents.case_handler_contact_burst_score_histories,
  wa_agents.case_handler_contact_burst_scores,
  wa_agents.api_to_case_handler_queue,
  wa_agents.case_handler_messages,
  wa_agents.api_history_chunks,
  wa_agents.api_statuses,
  wa_agents.api_inbound_messages,
  wa_agents.api_outbound_messages,
  wa_agents.case_handler_case_manifests,
  wa_agents.case_handler_contact_leases,
  wa_agents.api_inbound_payloads,
  wa_agents.api_contact_profiles,
  wa_agents.api_smb_app_contacts,
  wa_agents.api_embedded_signup_syncs,
  wa_agents.api_contacts,
  wa_agents.api_flows,
  wa_agents.api_flow_encryption_keys,
  wa_agents.api_phone_numbers,
  wa_agents.case_handler_routes_by_contact,
  wa_agents.case_handler_routes_by_phone_number,
  wa_agents.case_handler_configurations,
  wa_agents.api_worker_configurations,
  wa_agents.api_embedded_signup_sessions,
  wa_agents.api_embedded_signup_data,
  wa_agents.api_businesses
RESTART IDENTITY
CASCADE;
