/*
  =========================================================================================
  WHATSAPP API
  =========================================================================================
*/

-- TYPES

-- Non-empty string containing no whitespace
-- NOTE: The regex next to each column indicates the usual/expected format
CREATE DOMAIN T_NO_WS_STR AS VARCHAR CHECK (
  VALUE ~ '^\S+$'
);
CREATE TYPE T_WHATSAPP_MESSAGE AS ENUM (
  'text',
  'interactive',
  'template',
  'image',
  'video',
  'audio',
  'document',
  'sticker',
  'reaction',
  'contacts',
  'location',
  'media_placeholder',
  'revoke',
  'unsupported'
);
CREATE TYPE T_WHATSAPP_STATUS AS ENUM (
  'delivered',
  'failed',
  'played',
  'read',
  'sent'
);
CREATE TYPE T_WHATSAPP_CONNECTION_STATUS AS ENUM (
  'pending',
  'connected',
  'attention_needed',
  'disconnected'
);
CREATE TYPE T_WHATSAPP_SIGNUP_SESSION_STATUS AS ENUM (
  'pending',
  'completed',
  'failed',
  'cancelled',
  'expired'
);
CREATE TYPE T_WHATSAPP_APP_DATA_SYNC_TYPE AS ENUM (
  'smb_app_state_sync',
  'history'
);
CREATE TYPE T_WHATSAPP_APP_DATA_SYNC_STATUS AS ENUM (
  'eligible',
  'requesting',
  'accepted',
  'receiving',
  'completed',
  'declined',
  'failed'
);
CREATE TYPE T_WHATSAPP_FLOW_STATUS AS ENUM (
  'DRAFT',
  'PUBLISHED',
  'DEPRECATED',
  'BLOCKED',
  'THROTTLED'
);
CREATE TYPE T_WHATSAPP_FLOW_SESSION_STATUS AS ENUM (
  'created',
  'sent',
  'active',
  'completed',
  'failed',
  'expired',
  'superseded'
);
CREATE TYPE T_WHATSAPP_FLOW_EXCHANGE_STATUS AS ENUM (
  'processing',
  'succeeded',
  'failed'
);

-- FUNCTIONS

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION public.encrypt_text( data TEXT, key TEXT)
RETURNS BYTEA
LANGUAGE SQL
AS $$
  SELECT pgp_sym_encrypt(
    data,
    key,
    'cipher-algo=aes256,compress-algo=0'::TEXT
  );
$$;

CREATE OR REPLACE FUNCTION public.decrypt_text( data BYTEA, key TEXT)
RETURNS TEXT
LANGUAGE SQL
AS $$
  SELECT pgp_sym_decrypt( data, key);
$$;


-- ========================================================================================
-- BUSINESSES

CREATE TABLE IF NOT EXISTS public.wa_api_businesses (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  waba_id       T_NO_WS_STR NOT NULL, -- '^[0-9]+$'
  waba_name     TEXT        DEFAULT NULL,
  
  CONSTRAINT wa_api_businesses_waba_id_unique
    UNIQUE (waba_id)

);

ALTER TABLE public.wa_api_businesses
  ENABLE ROW LEVEL SECURITY;

CREATE TABLE IF NOT EXISTS public.wa_api_phone_numbers (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  business              BIGINT      NOT NULL,
  display_phone_number  T_NO_WS_STR NOT NULL, -- '^[0-9]+$'
  phone_number_id       T_NO_WS_STR NOT NULL, -- '^[0-9]+$'
  verified_name         TEXT        DEFAULT NULL,
  platform_type         T_NO_WS_STR DEFAULT NULL,
  is_on_biz_app         BOOLEAN     DEFAULT NULL,
  metadata              JSONB       DEFAULT NULL,
  
  CONSTRAINT wa_api_phone_numbers_business_fkey
    FOREIGN KEY (business)
    REFERENCES public.wa_api_businesses(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_phone_numbers_display_phone_number_unique
    UNIQUE (display_phone_number),
  
  CONSTRAINT wa_api_phone_numbers_phone_number_id_unique
    UNIQUE (phone_number_id),
  
  CONSTRAINT wa_api_phone_numbers_metadata_object_check
    CHECK (
      ( metadata IS NULL ) OR
      ( jsonb_typeof(metadata) = 'object' )
    )

);

ALTER TABLE public.wa_api_phone_numbers
  ENABLE ROW LEVEL SECURITY;

-- CONTACTS

CREATE TABLE IF NOT EXISTS public.wa_api_contacts (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  phone_number  BIGINT      NOT NULL,
  wa_id         T_NO_WS_STR DEFAULT NULL, -- '^[0-9]+$'
  user_id       T_NO_WS_STR DEFAULT NULL, -- '^[A-Z]{2}\.[A-Za-z0-9]{1,128}$'
  
  CONSTRAINT wa_api_contacts_phone_number_fkey
    FOREIGN KEY (phone_number)
    REFERENCES public.wa_api_phone_numbers(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_contacts_phone_number_id_unique
    UNIQUE ( phone_number, id),
  
  CONSTRAINT wa_api_contacts_phone_number_wa_id_unique
    UNIQUE ( phone_number, wa_id),
  
  CONSTRAINT wa_api_contacts_phone_number_user_id_unique
    UNIQUE ( phone_number, user_id),
  
  CONSTRAINT wa_api_contacts_wa_id_or_user_id
    CHECK(
      ( wa_id IS NOT NULL ) OR ( user_id IS NOT NULL )
    )

);

ALTER TABLE public.wa_api_contacts
  ENABLE ROW LEVEL SECURITY;

-- CONTACT PROFILES

CREATE TABLE IF NOT EXISTS public.wa_api_contact_profiles (
  
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  last_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  contact           BIGINT      NOT NULL,
  profile_name      TEXT        NOT NULL,
  profile_username  T_NO_WS_STR DEFAULT NULL, -- '^[A-Za-z0-9\.\_]{3,35}$'
  
  CONSTRAINT wa_api_contact_profiles_contact_fkey
    FOREIGN KEY (contact)
    REFERENCES public.wa_api_contacts(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_contact_profile_unique
    UNIQUE NULLS NOT DISTINCT (
      contact,
      profile_name,
      profile_username
    )

);

CREATE INDEX IF NOT EXISTS wa_api_contact_profiles_contact_time_idx
  ON public.wa_api_contact_profiles (
    contact,
    last_seen_at,
    id
  );

ALTER TABLE public.wa_api_contact_profiles
  ENABLE ROW LEVEL SECURITY;

-- ========================================================================================
-- EMBEDDED SIGNUP

CREATE TABLE IF NOT EXISTS public.wa_api_embedded_signup_data (
  
  business      BIGINT      PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  business_token       BYTEA                        NOT NULL,
  connection_status    T_WHATSAPP_CONNECTION_STATUS NOT NULL DEFAULT 'pending',
  token_last_refreshed TIMESTAMPTZ                  NOT NULL DEFAULT now(),
  app_subscribed       BOOLEAN                      NOT NULL DEFAULT FALSE,
  subscribed_at        TIMESTAMPTZ                  DEFAULT NULL,
  disconnected_at      TIMESTAMPTZ                  DEFAULT NULL,
  last_error           TEXT                         DEFAULT NULL,
  
  CONSTRAINT wa_api_embedded_signup_data_business_fkey
    FOREIGN KEY (business)
    REFERENCES public.wa_api_businesses(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_embedded_signup_data_subscription_time_check
    CHECK ( ( NOT app_subscribed ) OR ( subscribed_at IS NOT NULL ) )

);

ALTER TABLE public.wa_api_embedded_signup_data
  ENABLE ROW LEVEL SECURITY;

CREATE TABLE IF NOT EXISTS public.wa_api_embedded_signup_sessions (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  business          BIGINT                           DEFAULT NULL,
  session_state     T_NO_WS_STR                      NOT NULL,
  session_status    T_WHATSAPP_SIGNUP_SESSION_STATUS NOT NULL DEFAULT 'pending',
  expires_at        TIMESTAMPTZ                      NOT NULL,
  finished_at       TIMESTAMPTZ                      DEFAULT NULL,
  code_received_at  TIMESTAMPTZ                      DEFAULT NULL,
  event_type        T_NO_WS_STR                      DEFAULT NULL,
  event_name        T_NO_WS_STR                      DEFAULT NULL,
  event_version     SMALLINT                         DEFAULT NULL,
  event_data        JSONB                            DEFAULT NULL,
  application_data  JSONB                            NOT NULL DEFAULT '{}'::JSONB,
  last_error        TEXT                             DEFAULT NULL,
  
  CONSTRAINT wa_api_embedded_signup_sessions_business_fkey
    FOREIGN KEY (business)
    REFERENCES public.wa_api_businesses(id)
    ON UPDATE CASCADE
    ON DELETE SET NULL,
  
  CONSTRAINT wa_api_embedded_signup_sessions_state_unique
    UNIQUE (session_state),
  
  CONSTRAINT wa_api_embedded_signup_sessions_expiry_check
    CHECK ( expires_at > created_at ),
  
  CONSTRAINT wa_api_embedded_signup_sessions_finished_check
    CHECK (
      ( session_status = 'pending' ) = ( finished_at IS NULL )
    ),
  
  CONSTRAINT wa_api_embedded_signup_sessions_completed_business_check
    CHECK (
      ( session_status <> 'completed' ) OR
      ( business       IS NOT NULL    )
    ),
  
  CONSTRAINT wa_api_embedded_signup_sessions_event_data_object_check
    CHECK (
      ( event_data               IS NULL    ) OR
      ( jsonb_typeof(event_data) = 'object' )
    ),
  
  CONSTRAINT wa_api_embedded_signup_sessions_application_data_object_check
    CHECK (
      jsonb_typeof(application_data) = 'object'
    )

);

CREATE INDEX IF NOT EXISTS wa_api_embedded_signup_sessions_status_expiry_idx
  ON public.wa_api_embedded_signup_sessions (
    session_status,
    expires_at
  );

CREATE INDEX IF NOT EXISTS wa_api_embedded_signup_sessions_business_time_idx
  ON public.wa_api_embedded_signup_sessions (
    business,
    created_at DESC
  )
  WHERE ( business IS NOT NULL );

ALTER TABLE public.wa_api_embedded_signup_sessions
  ENABLE ROW LEVEL SECURITY;

CREATE TABLE IF NOT EXISTS public.wa_api_embedded_signup_syncs (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  phone_number  BIGINT                              NOT NULL,
  sync_type     T_WHATSAPP_APP_DATA_SYNC_TYPE       NOT NULL,
  sync_status   T_WHATSAPP_APP_DATA_SYNC_STATUS     NOT NULL DEFAULT 'eligible',
  request_id    T_NO_WS_STR                         DEFAULT NULL,
  requested_at  TIMESTAMPTZ                         DEFAULT NULL,
  started_at    TIMESTAMPTZ                         DEFAULT NULL,
  completed_at  TIMESTAMPTZ                         DEFAULT NULL,
  last_error    JSONB                               DEFAULT NULL,
  
  CONSTRAINT wa_api_embedded_signup_syncs_phone_number_fkey
    FOREIGN KEY (phone_number)
    REFERENCES public.wa_api_phone_numbers(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_embedded_signup_syncs_phone_number_type_unique
    UNIQUE ( phone_number, sync_type),
  
  CONSTRAINT wa_api_embedded_signup_syncs_request_id_unique
    UNIQUE (request_id),
  
  CONSTRAINT wa_api_embedded_signup_syncs_error_object_check
    CHECK (
      ( last_error IS NULL ) OR
      ( jsonb_typeof(last_error) = 'object' )
    )

);

CREATE INDEX IF NOT EXISTS wa_api_embedded_signup_syncs_status_time_idx
  ON public.wa_api_embedded_signup_syncs (
    sync_status,
    updated_at
  );

ALTER TABLE public.wa_api_embedded_signup_syncs
  ENABLE ROW LEVEL SECURITY;

-- WHATSAPP BUSINESS APP CONTACTS

CREATE TABLE IF NOT EXISTS public.wa_api_smb_app_contacts (
  
  contact       BIGINT      PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  full_name     TEXT        DEFAULT NULL,
  first_name    TEXT        DEFAULT NULL,
  is_active     BOOLEAN     NOT NULL DEFAULT TRUE,
  changed_at    TIMESTAMPTZ NOT NULL,
  
  CONSTRAINT wa_api_smb_app_contacts_contact_fkey
    FOREIGN KEY (contact)
    REFERENCES public.wa_api_contacts(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE

);

ALTER TABLE public.wa_api_smb_app_contacts
  ENABLE ROW LEVEL SECURITY;

-- ========================================================================================
-- PAYLOADS

CREATE TABLE IF NOT EXISTS public.wa_api_inbound_payloads (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  item_index    SMALLINT    NOT NULL DEFAULT 0,
  item_ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
  change_index  SMALLINT    NOT NULL DEFAULT 0,
  contact       BIGINT      DEFAULT NULL,
  
  data_raw      JSONB       NOT NULL,
  data_hash     T_NO_WS_STR NOT NULL, -- '^[A-Fa-f0-9]{64}$'
  validated     BOOLEAN     NOT NULL DEFAULT FALSE,
  errors        JSONB       DEFAULT NULL,
  
  CONSTRAINT wa_api_inbound_payloads_contact_fkey
    FOREIGN KEY (contact)
    REFERENCES public.wa_api_contacts(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_inbound_payloads_data_hash_unique
    UNIQUE (data_hash),
  
  CONSTRAINT wa_api_inbound_payloads_valid_errors_check
    CHECK ( ( NOT validated ) OR ( errors IS NULL ) )

);

CREATE INDEX IF NOT EXISTS wa_api_inbound_payloads_received_at_idx
  ON public.wa_api_inbound_payloads (received_at);

CREATE INDEX IF NOT EXISTS wa_api_inbound_payloads_contact_idx
  ON public.wa_api_inbound_payloads (contact);

CREATE INDEX IF NOT EXISTS wa_api_inbound_payloads_validation_errors_idx
  ON public.wa_api_inbound_payloads (received_at)
  WHERE ( ( NOT validated ) AND ( errors IS NOT NULL ) );

ALTER TABLE public.wa_api_inbound_payloads
  ENABLE ROW LEVEL SECURITY;

-- MESSAGE HISTORY CHUNKS

CREATE TABLE IF NOT EXISTS public.wa_api_history_chunks (
  
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  
  sync            BIGINT      NOT NULL,
  payload         BIGINT      NOT NULL,
  history_index   SMALLINT    NOT NULL DEFAULT 0,
  phase           SMALLINT    DEFAULT NULL,
  chunk_order     INTEGER     DEFAULT NULL,
  progress        SMALLINT    DEFAULT NULL,
  errors          JSONB       DEFAULT NULL,
  
  CONSTRAINT wa_api_history_chunks_sync_fkey
    FOREIGN KEY (sync)
    REFERENCES public.wa_api_embedded_signup_syncs(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_history_chunks_payload_fkey
    FOREIGN KEY (payload)
    REFERENCES public.wa_api_inbound_payloads(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_history_chunks_payload_index_unique
    UNIQUE ( payload, history_index),
  
  CONSTRAINT wa_api_history_chunks_index_nonnegative
    CHECK ( history_index >= 0 ),
  
  CONSTRAINT wa_api_history_chunks_phase_check
    CHECK ( ( phase IS NULL ) OR ( phase BETWEEN 0 AND 2 ) ),
  
  CONSTRAINT wa_api_history_chunks_order_nonnegative
    CHECK ( ( chunk_order IS NULL ) OR ( chunk_order >= 0 ) ),
  
  CONSTRAINT wa_api_history_chunks_progress_check
    CHECK ( ( progress IS NULL ) OR ( progress BETWEEN 0 AND 100 ) ),
  
  CONSTRAINT wa_api_history_chunks_result_check
    CHECK (
      (
        ( phase       IS NOT NULL ) AND
        ( chunk_order IS NOT NULL ) AND
        ( progress    IS NOT NULL )
      ) OR
      ( errors IS NOT NULL )
    ),
  
  CONSTRAINT wa_api_history_chunks_errors_array_check
    CHECK (
      ( errors IS NULL ) OR
      ( jsonb_typeof(errors) = 'array' )
    )

);

CREATE INDEX IF NOT EXISTS wa_api_history_chunks_sync_progress_idx
  ON public.wa_api_history_chunks (
    sync,
    progress,
    phase,
    chunk_order
  );

ALTER TABLE public.wa_api_history_chunks
  ENABLE ROW LEVEL SECURITY;


-- ========================================================================================
-- MESSAGES: INBOUND

CREATE TABLE IF NOT EXISTS public.wa_api_inbound_messages (
  
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  contact           BIGINT              NOT NULL,
  payload           BIGINT              NOT NULL,
  
  is_echo           BOOLEAN             NOT NULL DEFAULT FALSE,
  msg_id            T_NO_WS_STR         NOT NULL, -- '^wamid\.[A-Za-z0-9+/=]+$'
  msg_ts            TIMESTAMPTZ         NOT NULL,
  msg_type          T_WHATSAPP_MESSAGE  NOT NULL,
  msg_data          JSONB               NOT NULL,
  
  history_chunk     BIGINT              DEFAULT NULL,
  history_context   JSONB               DEFAULT NULL,
  
  CONSTRAINT wa_api_inbound_messages_payload_fkey
    FOREIGN KEY (payload)
    REFERENCES public.wa_api_inbound_payloads(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_inbound_messages_contact_fkey
    FOREIGN KEY (contact)
    REFERENCES public.wa_api_contacts(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_inbound_messages_history_chunk_fkey
    FOREIGN KEY (history_chunk)
    REFERENCES public.wa_api_history_chunks(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_inbound_messages_msg_id_unique
    UNIQUE (msg_id),
  
  CONSTRAINT wa_api_inbound_messages_history_context_check
    CHECK (
      ( history_context IS NULL ) OR
      (
        ( history_chunk IS NOT NULL                ) AND
        ( jsonb_typeof(history_context) = 'object' )
      )
    )

);

CREATE INDEX IF NOT EXISTS wa_api_inbound_messages_payload_idx
  ON public.wa_api_inbound_messages (payload);

CREATE INDEX IF NOT EXISTS wa_api_inbound_messages_contact_time_idx
  ON public.wa_api_inbound_messages (
    contact,
    msg_ts,
    id
  );

CREATE INDEX IF NOT EXISTS wa_api_inbound_messages_history_chunk_idx
  ON public.wa_api_inbound_messages (history_chunk)
  WHERE ( history_chunk IS NOT NULL );

ALTER TABLE public.wa_api_inbound_messages
  ENABLE ROW LEVEL SECURITY;

-- MESSAGES: OUTBOUND

CREATE TABLE IF NOT EXISTS public.wa_api_outbound_messages (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sent_at       TIMESTAMPTZ         NOT NULL DEFAULT now(),
  
  contact       BIGINT              NOT NULL,
  msg_id        T_NO_WS_STR         NOT NULL, -- '^wamid\.[A-Za-z0-9+/=]+$'
  msg_type      T_WHATSAPP_MESSAGE  NOT NULL,
  msg_data      JSONB               NOT NULL,
  
  CONSTRAINT wa_api_outbound_messages_contact_fkey
    FOREIGN KEY (contact)
    REFERENCES public.wa_api_contacts(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_outbound_messages_msg_id_unique
    UNIQUE (msg_id)

);

CREATE INDEX IF NOT EXISTS wa_api_outbound_messages_contact_idx
  ON public.wa_api_outbound_messages (contact);

ALTER TABLE public.wa_api_outbound_messages
  ENABLE ROW LEVEL SECURITY;

-- MESSAGES: MEDIA

CREATE TABLE IF NOT EXISTS public.wa_api_media (
  
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  inbound_msg_id    BIGINT      DEFAULT NULL,
  outbound_msg_id   BIGINT      DEFAULT NULL,
  
  mime_type         T_NO_WS_STR NOT NULL, -- MIME type without codec
  size              BIGINT      NOT NULL,
  object_key        T_NO_WS_STR NOT NULL, -- '^[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)+$'
  caption           TEXT        DEFAULT NULL,
  filename          TEXT        DEFAULT NULL,
  
  CONSTRAINT wa_api_media_inbound_msg_id_fkey
    FOREIGN KEY (inbound_msg_id)
    REFERENCES public.wa_api_inbound_messages(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_media_outbound_msg_id_fkey
    FOREIGN KEY (outbound_msg_id)
    REFERENCES public.wa_api_outbound_messages(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_media_inbound_msg_id_unique
    UNIQUE (inbound_msg_id),
  
  CONSTRAINT wa_api_media_outbound_msg_id_unique
    UNIQUE (outbound_msg_id),
  
  CONSTRAINT wa_api_media_inbound_xor_outbound
    CHECK (
      ( inbound_msg_id IS NOT NULL ) <> ( outbound_msg_id IS NOT NULL )
    ),
  
  CONSTRAINT wa_api_media_size_nonnegative
    CHECK ( size >= 0 )

);

ALTER TABLE public.wa_api_media
  ENABLE ROW LEVEL SECURITY;


-- ========================================================================================
-- STATUSES OF SENT MESSAGES

CREATE TABLE IF NOT EXISTS public.wa_api_statuses (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payload       BIGINT            NOT NULL,
  
  msg_id        T_NO_WS_STR       NOT NULL, -- '^wamid\.[A-Za-z0-9+/=]+$'
  msg_status    T_WHATSAPP_STATUS NOT NULL,
  status_ts     TIMESTAMPTZ       NOT NULL,
  
  /*
  DESIGN NOTES:
  - Column `msg_id` is the WhatsApp API Message ID (wamid) of either:
    - An outbound message sent by the chatbot,
      with ID in `wa_api_outbound_messages.msg_id`
    - An inbound message echo sent by the human server,
      with ID in `wa_api_inbound_messages.msg_id`
  - A fully normalized foreign key schema would require two nullable references
    with an XOR constraint: one referencing inbound messages and one referencing
    outbound messages. However, this approach would introduce a missing reference
    problem when the message echo status reaches us before its corresponding payload.
  */
  
  conversation  JSONB DEFAULT NULL,
  pricing       JSONB DEFAULT NULL,
  errors        JSONB DEFAULT NULL,
  
  CONSTRAINT wa_api_statuses_payload_fkey
    FOREIGN KEY (payload)
    REFERENCES public.wa_api_inbound_payloads(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE

);

CREATE INDEX IF NOT EXISTS wa_api_statuses_payload_idx
  ON public.wa_api_statuses (payload);

CREATE INDEX IF NOT EXISTS wa_api_statuses_msg_id_idx
  ON public.wa_api_statuses (msg_id);

ALTER TABLE public.wa_api_statuses
  ENABLE ROW LEVEL SECURITY;


/*
  =========================================================================================
  CASE HANDLER ROUTING & SETTINGS
  =========================================================================================
*/

CREATE TABLE IF NOT EXISTS public.wa_case_handler_routes (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  phone_number  BIGINT      NOT NULL,
  contact       BIGINT      DEFAULT NULL, -- For contact-specific handler
  handler_key   T_NO_WS_STR NOT NULL,
  handler_url   T_NO_WS_STR DEFAULT NULL, -- For remote handler URL
  
  CONSTRAINT wa_case_handler_routes_phone_number_fkey
    FOREIGN KEY (phone_number)
    REFERENCES public.wa_api_phone_numbers(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_case_handler_routes_contact_fkey
    FOREIGN KEY ( phone_number, contact)
    REFERENCES public.wa_api_contacts( phone_number, id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_case_handler_routes_phone_number_contact_unique
    UNIQUE NULLS NOT DISTINCT ( phone_number, contact)

);

CREATE INDEX IF NOT EXISTS wa_case_handler_routes_handler_key_idx
  ON public.wa_case_handler_routes (handler_key);

ALTER TABLE public.wa_case_handler_routes
  ENABLE ROW LEVEL SECURITY;

CREATE TABLE IF NOT EXISTS public.wa_case_handler_settings (
  
  handler_key             T_NO_WS_STR PRIMARY KEY,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  silence_timeout_minutes INTEGER     NOT NULL,
  
  CONSTRAINT wa_case_handler_settings_silence_timeout_positive
    CHECK ( silence_timeout_minutes > 0 )

);

ALTER TABLE public.wa_case_handler_settings
  ENABLE ROW LEVEL SECURITY;


/*
  =========================================================================================
  INBOUND MESSAGES QUEUE
  =========================================================================================
*/

CREATE TYPE T_ENQUEUED_MESSAGE_STATUS AS ENUM (
  'done',
  'error',
  'pending',
  'processing'
);

CREATE TABLE IF NOT EXISTS public.wa_api_to_case_handler_queue (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at    TIMESTAMPTZ               NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ               DEFAULT NULL,
  last_error_at TIMESTAMPTZ               DEFAULT NULL,
  
  /*
  DESIGN NOTES:
  - We made column `handler_id` nullable for those who don't need handler routing
  - In the FK constraint `handler_id` -> `wa_case_handler_routes.id` we changed
    the usual `ON DELETE CASCADE` to `ON DELETE SET NULL` so that data is not lost
    when removing case handler routes.
  */
  
  handler_id    BIGINT                    DEFAULT NULL,
  msg_id        T_NO_WS_STR               NOT NULL, -- '^wamid\.[A-Za-z0-9+/=]+$'
  msg_status    T_ENQUEUED_MESSAGE_STATUS NOT NULL DEFAULT 'pending',
  
  CONSTRAINT wa_api_to_case_handler_queue_handler_id_fkey
    FOREIGN KEY (handler_id)
    REFERENCES public.wa_case_handler_routes(id)
    ON UPDATE CASCADE
    ON DELETE SET NULL,
  
  CONSTRAINT wa_api_to_case_handler_queue_msg_id_fkey
    FOREIGN KEY (msg_id)
    REFERENCES public.wa_api_inbound_messages(msg_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_to_case_handler_queue_msg_id_unique
    UNIQUE (msg_id)

);

CREATE INDEX IF NOT EXISTS wa_api_to_case_handler_queue_handler_status_idx
  ON public.wa_api_to_case_handler_queue (
    handler_id,
    msg_status,
    created_at,
    id
  );

ALTER TABLE public.wa_api_to_case_handler_queue
  ENABLE ROW LEVEL SECURITY;


/*
  =========================================================================================
  CASE HANDLER
  =========================================================================================
*/

-- CONTACT LEASES

CREATE TABLE IF NOT EXISTS public.wa_case_handler_contact_leases (
  
  contact       BIGINT      NOT NULL,
  owner_token   UUID        NOT NULL,
  acquired_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  heartbeat_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at    TIMESTAMPTZ NOT NULL DEFAULT now() + INTERVAL '90 seconds',
  
  CONSTRAINT wa_case_handler_contact_leases_pkey
    PRIMARY KEY (contact),
  
  CONSTRAINT wa_case_handler_contact_leases_contact_fkey
    FOREIGN KEY (contact)
    REFERENCES public.wa_api_contacts(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_case_handler_contact_leases_expiry_check
    CHECK ( expires_at > heartbeat_at )

);

CREATE INDEX IF NOT EXISTS wa_case_handler_contact_leases_expiry_idx
  ON public.wa_case_handler_contact_leases (expires_at);

ALTER TABLE public.wa_case_handler_contact_leases
  ENABLE ROW LEVEL SECURITY;

-- CASE MANIFESTS

CREATE TABLE IF NOT EXISTS public.wa_case_handler_case_manifests (
  
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ DEFAULT NULL,
  
  /*
  DESIGN NOTES:
  - Column `handler_id` has the same constraints as in `wa_api_to_case_handler_queue`
  */
  
  handler_id         BIGINT       DEFAULT NULL,
  contact            BIGINT       NOT NULL,
  case_index         INT          NOT NULL,
  next_message_index INT          NOT NULL DEFAULT 1,
  is_open            BOOLEAN      NOT NULL DEFAULT TRUE,
  machine_state      T_NO_WS_STR  DEFAULT NULL,
  silenced_until     TIMESTAMPTZ  DEFAULT NULL,
  
  CONSTRAINT wa_case_handler_case_manifests_handler_id_fkey
    FOREIGN KEY (handler_id)
    REFERENCES public.wa_case_handler_routes(id)
    ON UPDATE CASCADE
    ON DELETE SET NULL,
  
  CONSTRAINT wa_case_handler_case_manifests_contact_fkey
    FOREIGN KEY (contact)
    REFERENCES public.wa_api_contacts(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_case_handler_case_manifests_contact_case_index_unique
    UNIQUE ( contact, case_index),
  
  CONSTRAINT wa_case_handler_case_manifests_case_index_positive
    CHECK ( case_index >= 1 ),
  
  CONSTRAINT wa_case_handler_case_manifests_next_message_index_positive
    CHECK ( next_message_index >= 1 )

);

CREATE INDEX IF NOT EXISTS wa_case_handler_case_manifests_contact_idx
  ON public.wa_case_handler_case_manifests (contact);

CREATE UNIQUE INDEX IF NOT EXISTS wa_case_handler_case_manifests_contact_open_idx
  ON public.wa_case_handler_case_manifests (contact)
  WHERE is_open;

ALTER TABLE public.wa_case_handler_case_manifests
  ENABLE ROW LEVEL SECURITY;

-- MESSAGES

CREATE TABLE IF NOT EXISTS public.wa_case_handler_messages (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
  case_id       BIGINT      NOT NULL,
  message_index INT         NOT NULL,
  basemodel     T_NO_WS_STR NOT NULL,
  origin        T_NO_WS_STR DEFAULT NULL,
  data          JSONB       NOT NULL,
  
  CONSTRAINT wa_case_handler_messages_case_id_fkey
    FOREIGN KEY (case_id)
    REFERENCES public.wa_case_handler_case_manifests(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_case_handler_messages_case_message_index_unique
    UNIQUE ( case_id, message_index),
  
  CONSTRAINT wa_case_handler_messages_message_index_positive
    CHECK ( message_index >= 1 ),
  
  CONSTRAINT wa_case_handler_messages_data_object
    CHECK ( jsonb_typeof(data) = 'object' )

);

CREATE UNIQUE INDEX IF NOT EXISTS wa_case_handler_messages_case_id_id_idx
  ON public.wa_case_handler_messages (
    case_id,
    id
  );

ALTER TABLE public.wa_case_handler_messages
  ENABLE ROW LEVEL SECURITY;

-- STATE HISTORIES

CREATE TABLE IF NOT EXISTS public.wa_case_handler_state_histories (

  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  case_manifest   BIGINT       NOT NULL,
  case_message    BIGINT       DEFAULT NULL,
  machine_state   T_NO_WS_STR  DEFAULT NULL,

  CONSTRAINT wa_case_handler_state_histories_case_manifest_fkey
    FOREIGN KEY (case_manifest)
    REFERENCES public.wa_case_handler_case_manifests(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

  CONSTRAINT wa_case_handler_state_histories_case_message_fkey
    FOREIGN KEY ( case_manifest, case_message)
    REFERENCES public.wa_case_handler_messages( case_id, id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

  CONSTRAINT wa_case_handler_state_histories_snapshot_unique
    UNIQUE NULLS NOT DISTINCT (
      case_manifest,
      case_message
    )

);

CREATE INDEX IF NOT EXISTS wa_case_handler_state_histories_case_timeline_idx
  ON public.wa_case_handler_state_histories (
    case_manifest,
    id
  );

ALTER TABLE public.wa_case_handler_state_histories
  ENABLE ROW LEVEL SECURITY;

-- WHATSAPP MESSAGE IDS

CREATE TABLE IF NOT EXISTS public.wa_case_handler_to_api (
  
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at  TIMESTAMPTZ NOT NULL   DEFAULT now(),
  
  case_handler_msg_id   BIGINT NOT NULL,
  api_inbound_msg_id    BIGINT DEFAULT NULL,
  api_outbound_msg_id   BIGINT DEFAULT NULL,
  
  CONSTRAINT wa_case_handler_to_api_case_handler_msg_id_fkey
    FOREIGN KEY (case_handler_msg_id)
    REFERENCES public.wa_case_handler_messages(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_case_handler_to_api_api_inbound_msg_id_fkey
    FOREIGN KEY (api_inbound_msg_id)
    REFERENCES public.wa_api_inbound_messages(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_case_handler_to_api_api_outbound_msg_id_fkey
    FOREIGN KEY (api_outbound_msg_id)
    REFERENCES public.wa_api_outbound_messages(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  /*
  DESIGN NOTES:
  - Here we don't add a unique constraint for `case_handler_msg_id` intentionally.
    The reason is that LLM responses may exceed the Whatspp text message max length,
    resulting in message chunking, i.e., some single case handler messages
    may be sent using more than one outbound WhatsApp messages.
  - On the contrary, inbound API messages map to at most one case handler message,
    so we add a unique index on case handler messages of inbound API messages.
  */
  
  CONSTRAINT wa_case_handler_to_api_api_inbound_msg_id_unique
    UNIQUE (api_inbound_msg_id),
  
  CONSTRAINT wa_case_handler_to_api_api_outbound_msg_id_unique
    UNIQUE (api_outbound_msg_id),
  
  CONSTRAINT wa_case_handler_to_api_inbound_xor_outbound
    CHECK (
      ( api_inbound_msg_id IS NOT NULL ) <> ( api_outbound_msg_id IS NOT NULL )
    )

);

CREATE UNIQUE INDEX IF NOT EXISTS wa_case_handler_to_api_inbound_uniqueness_idx
  ON public.wa_case_handler_to_api (case_handler_msg_id)
  WHERE ( api_inbound_msg_id IS NOT NULL );

CREATE INDEX IF NOT EXISTS wa_case_handler_to_api_case_handler_msg_id_idx
  ON public.wa_case_handler_to_api (case_handler_msg_id);

ALTER TABLE public.wa_case_handler_to_api
  ENABLE ROW LEVEL SECURITY;

-- AGENT CONTEXTS

CREATE TABLE IF NOT EXISTS public.wa_case_handler_agent_contexts (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL   DEFAULT now(),
  
  /*
  TODO: Document semantics here
  */
  
  agent_name    T_NO_WS_STR NOT NULL,
  context_index INT         NOT NULL,
  case_manifest BIGINT      NOT NULL,
  case_message  BIGINT      DEFAULT NULL,
  
  CONSTRAINT wa_case_handler_agent_contexts_context_index_nonnegative
    CHECK ( context_index >= 0 ),

  CONSTRAINT wa_case_handler_agent_contexts_case_manifest_fkey
    FOREIGN KEY (case_manifest)
    REFERENCES public.wa_case_handler_case_manifests(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_case_handler_agent_contexts_case_message_fkey
    FOREIGN KEY ( case_manifest, case_message)
    REFERENCES public.wa_case_handler_messages( case_id, id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

  CONSTRAINT wa_case_handler_agent_contexts_membership_unique
    UNIQUE NULLS NOT DISTINCT (
      agent_name,
      context_index,
      case_manifest,
      case_message
    )

);

CREATE INDEX IF NOT EXISTS wa_case_handler_agent_contexts_current_idx
  ON public.wa_case_handler_agent_contexts (
    case_manifest,
    agent_name,
    context_index DESC,
    id
  );

CREATE INDEX IF NOT EXISTS wa_case_handler_agent_contexts_message_idx
  ON public.wa_case_handler_agent_contexts (
    case_manifest,
    case_message
  )
  WHERE ( case_message IS NOT NULL );

ALTER TABLE public.wa_case_handler_agent_contexts
  ENABLE ROW LEVEL SECURITY;


/*
  =========================================================================================
  WHATSAPP FLOWS
  =========================================================================================
*/

-- WABA ENCRYPTION KEYS

CREATE TABLE IF NOT EXISTS public.wa_api_flow_encryption_keys (
  
  id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  business                BIGINT      NOT NULL,
  public_key              TEXT        NOT NULL,
  public_key_fingerprint  T_NO_WS_STR NOT NULL, -- '^[A-Fa-f0-9]{64}$'
  private_key_encrypted   BYTEA       NOT NULL,
  
  CONSTRAINT wa_api_flow_encryption_keys_business_fkey
    FOREIGN KEY (business)
    REFERENCES public.wa_api_businesses(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_flow_encryption_keys_business_unique
    UNIQUE (business),
  
  CONSTRAINT wa_api_flow_encryption_keys_public_key_fingerprint_unique
    UNIQUE (public_key_fingerprint)

);

ALTER TABLE public.wa_api_flow_encryption_keys
  ENABLE ROW LEVEL SECURITY;

-- FLOW DEFINITIONS

CREATE TABLE IF NOT EXISTS public.wa_api_flows (
  
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at        TIMESTAMPTZ            NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ            NOT NULL DEFAULT now(),
  
  flow_waba         BIGINT                 NOT NULL,
  flow_key          T_NO_WS_STR            NOT NULL,
  flow_id           T_NO_WS_STR            NOT NULL, -- '^[0-9]+$'
  flow_name         T_NO_WS_STR            NOT NULL,
  handler_key       T_NO_WS_STR            NOT NULL,
  flow_status       T_WHATSAPP_FLOW_STATUS NOT NULL DEFAULT 'DRAFT',
  flow_json_version T_NO_WS_STR            NOT NULL DEFAULT '7.1',
  data_api_version  T_NO_WS_STR            NOT NULL DEFAULT '3.0',
  endpoint_uri      TEXT                   DEFAULT NULL,
  is_active         BOOLEAN                NOT NULL DEFAULT TRUE,
  
  CONSTRAINT wa_api_flows_flow_waba_fkey
    FOREIGN KEY (flow_waba)
    REFERENCES public.wa_api_flow_encryption_keys(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_flows_flow_id_unique
    UNIQUE (flow_id)

);

CREATE UNIQUE INDEX IF NOT EXISTS wa_api_flows_waba_key_active_idx
  ON public.wa_api_flows (
    flow_waba,
    flow_key
  )
  WHERE is_active;

CREATE INDEX IF NOT EXISTS wa_api_flows_handler_key_idx
  ON public.wa_api_flows (handler_key);

ALTER TABLE public.wa_api_flows
  ENABLE ROW LEVEL SECURITY;

-- FLOW SESSIONS

CREATE TABLE IF NOT EXISTS public.wa_api_flow_sessions (
  
  id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at            TIMESTAMPTZ                      NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ                      NOT NULL DEFAULT now(),
  expires_at            TIMESTAMPTZ                      NOT NULL,
  activated_at          TIMESTAMPTZ                      DEFAULT NULL,
  completed_at          TIMESTAMPTZ                      DEFAULT NULL,
  
  flow                  BIGINT                           NOT NULL,
  phone_number          BIGINT                           NOT NULL,
  contact               BIGINT                           NOT NULL,
  case_handler_message  BIGINT                           DEFAULT NULL,
  outbound_message      BIGINT                           DEFAULT NULL,
  completion_message    BIGINT                           DEFAULT NULL,
  flow_token_encrypted  BYTEA                            NOT NULL,
  flow_token_hash       T_NO_WS_STR                      NOT NULL,
  session_state         BYTEA                            DEFAULT NULL,
  completion_data       BYTEA                            DEFAULT NULL,
  session_status        T_WHATSAPP_FLOW_SESSION_STATUS   NOT NULL DEFAULT 'created',
  last_error            JSONB                            DEFAULT NULL,
  
  CONSTRAINT wa_api_flow_sessions_flow_fkey
    FOREIGN KEY (flow)
    REFERENCES public.wa_api_flows(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_flow_sessions_phone_number_fkey
    FOREIGN KEY (phone_number)
    REFERENCES public.wa_api_phone_numbers(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_flow_sessions_contact_fkey
    FOREIGN KEY ( phone_number, contact)
    REFERENCES public.wa_api_contacts( phone_number, id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_flow_sessions_case_handler_message_fkey
    FOREIGN KEY (case_handler_message)
    REFERENCES public.wa_case_handler_messages(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_flow_sessions_outbound_message_fkey
    FOREIGN KEY (outbound_message)
    REFERENCES public.wa_api_outbound_messages(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_flow_sessions_completion_message_fkey
    FOREIGN KEY (completion_message)
    REFERENCES public.wa_api_inbound_messages(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_flow_sessions_flow_token_hash_unique
    UNIQUE (flow_token_hash),
  
  CONSTRAINT wa_api_flow_sessions_expiry_check
    CHECK ( expires_at > created_at )

);

CREATE INDEX IF NOT EXISTS wa_api_flow_sessions_contact_status_idx
  ON public.wa_api_flow_sessions (
    contact,
    session_status,
    created_at DESC,
    id
  );

CREATE INDEX IF NOT EXISTS wa_api_flow_sessions_expiry_idx
  ON public.wa_api_flow_sessions (expires_at)
  WHERE session_status IN ( 'created', 'sent', 'active');

ALTER TABLE public.wa_api_flow_sessions
  ENABLE ROW LEVEL SECURITY;

-- FLOW DATA EXCHANGES

CREATE TABLE IF NOT EXISTS public.wa_api_flow_data_exchanges (
  
  id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at          TIMESTAMPTZ                       NOT NULL DEFAULT now(),
  completed_at        TIMESTAMPTZ                       DEFAULT NULL,
  
  flow_waba           BIGINT                            NOT NULL,
  session             BIGINT                            DEFAULT NULL,
  request_hash        T_NO_WS_STR                       NOT NULL,
  request_action      T_NO_WS_STR                       NOT NULL,
  request_screen      T_NO_WS_STR                       DEFAULT NULL,
  request_data        BYTEA                             NOT NULL,
  response_data       BYTEA                             DEFAULT NULL,
  exchange_status     T_WHATSAPP_FLOW_EXCHANGE_STATUS   NOT NULL DEFAULT 'processing',
  http_status         SMALLINT                          DEFAULT NULL,
  latency_ms          INT                               DEFAULT NULL,
  error_code          T_NO_WS_STR                       DEFAULT NULL,
  error_message       TEXT                              DEFAULT NULL,
  
  CONSTRAINT wa_api_flow_data_exchanges_flow_waba_fkey
    FOREIGN KEY (flow_waba)
    REFERENCES public.wa_api_flow_encryption_keys(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_flow_data_exchanges_session_fkey
    FOREIGN KEY (session)
    REFERENCES public.wa_api_flow_sessions(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_flow_data_exchanges_latency_nonnegative
    CHECK ( ( latency_ms IS NULL ) OR ( latency_ms >= 0 ) )

);

CREATE UNIQUE INDEX IF NOT EXISTS wa_api_flow_data_exchanges_session_request_idx
  ON public.wa_api_flow_data_exchanges (
    session,
    request_hash
  )
  WHERE ( session IS NOT NULL );

CREATE INDEX IF NOT EXISTS wa_api_flow_data_exchanges_waba_time_idx
  ON public.wa_api_flow_data_exchanges (
    flow_waba,
    created_at DESC,
    id
  );

ALTER TABLE public.wa_api_flow_data_exchanges
  ENABLE ROW LEVEL SECURITY;

-- FLOW WEBHOOK EVENTS

CREATE TABLE IF NOT EXISTS public.wa_api_flow_events (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  event_at      TIMESTAMPTZ DEFAULT NULL,
  
  payload       BIGINT      NOT NULL,
  flow_waba     BIGINT      DEFAULT NULL,
  flow_id       T_NO_WS_STR DEFAULT NULL, -- '^[0-9]+$'
  event_type    T_NO_WS_STR NOT NULL,
  event_data    JSONB       NOT NULL,
  
  CONSTRAINT wa_api_flow_events_payload_fkey
    FOREIGN KEY (payload)
    REFERENCES public.wa_api_inbound_payloads(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_flow_events_flow_waba_fkey
    FOREIGN KEY (flow_waba)
    REFERENCES public.wa_api_flow_encryption_keys(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE

);

CREATE INDEX IF NOT EXISTS wa_api_flow_events_flow_time_idx
  ON public.wa_api_flow_events (
    flow_id,
    received_at DESC,
    id
  );

ALTER TABLE public.wa_api_flow_events
  ENABLE ROW LEVEL SECURITY;
