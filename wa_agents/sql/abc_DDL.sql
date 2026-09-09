/*
  =========================================================================================
  WHATSAPP MESSAGE PERSISTENCE SCHEMA
  =========================================================================================
*/

-- TYPES

CREATE DOMAIN T_NE_STR AS VARCHAR CHECK (
  VALUE ~ '^[^\s].*$'
);
CREATE DOMAIN T_NE_VAR_NAME AS VARCHAR CHECK (
  VALUE ~ '^[A-Za-z_]\w+$'
);
CREATE DOMAIN T_NUMERIC_ID AS VARCHAR CHECK (
  VALUE ~ '^[0-9]+$'
);
CREATE DOMAIN T_SHA256_HEX_HASH AS VARCHAR CHECK (
  VALUE ~ '^[A-Fa-f0-9]{64}$'
);
CREATE DOMAIN T_WHATSAPP_BSUID AS VARCHAR CHECK (
  VALUE ~ '^[A-Z]{2}\.[A-Za-z0-9]{1,128}$'
);
CREATE DOMAIN T_WHATSAPP_MESSAGE_ID AS VARCHAR CHECK (
  VALUE ~ '^wamid\.[A-Za-z0-9\+\/\=]+$'
);
CREATE DOMAIN T_WHATSAPP_USERNAME AS VARCHAR CHECK (
  VALUE ~ '^[A-Za-z0-9\.\_]{3,35}$'
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
  'unsupported'
);
CREATE TYPE T_WHATSAPP_STATUS AS ENUM (
  'delivered',
  'failed',
  'played',
  'read',
  'sent'
);


-- ========================================================================================
-- BUSINESSES

CREATE TABLE IF NOT EXISTS public.wa_api_businesses (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  waba_id               T_NUMERIC_ID NOT NULL,
  phone_number_id       T_NUMERIC_ID NOT NULL,
  display_phone_number  T_NUMERIC_ID NOT NULL,
  
  CONSTRAINT wa_api_businesses_phone_number_id_unique
    UNIQUE (phone_number_id),
  
  CONSTRAINT wa_api_businesses_display_phone_number_unique
    UNIQUE (display_phone_number)

);

ALTER TABLE public.wa_api_businesses
  ENABLE ROW LEVEL SECURITY;

-- CONTACTS

CREATE TABLE IF NOT EXISTS public.wa_api_contacts (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  business      BIGINT            NOT NULL,
  wa_id         T_NUMERIC_ID      DEFAULT NULL,
  user_id       T_WHATSAPP_BSUID  DEFAULT NULL,
  
  CONSTRAINT wa_api_contacts_business_fkey
    FOREIGN KEY (business)
    REFERENCES public.wa_api_businesses(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_contacts_either_wa_id_or_user_id
    CHECK(
      ( wa_id IS NOT NULL ) OR ( user_id IS NOT NULL )
    ),
  
  CONSTRAINT wa_api_contacts_business_wa_id_unique
    UNIQUE ( business, wa_id),
  
  CONSTRAINT wa_api_contacts_business_user_id_unique
    UNIQUE ( business, user_id)

);

ALTER TABLE public.wa_api_contacts
  ENABLE ROW LEVEL SECURITY;

-- CONTACT PROFILES

CREATE TABLE IF NOT EXISTS public.wa_api_contact_profiles (
  
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  last_seen_at      TIMESTAMPTZ           NOT NULL DEFAULT now(),
  
  contact           BIGINT                NOT NULL,
  profile_name      TEXT                  NOT NULL,
  profile_username  T_WHATSAPP_USERNAME   DEFAULT NULL,
  
  CONSTRAINT wa_api_contact_profiles_contact_fkey
    FOREIGN KEY (contact)
    REFERENCES public.wa_api_contacts(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE

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
-- PAYLOADS

CREATE TABLE IF NOT EXISTS public.wa_api_inbound_payload_metadata (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  received_at   TIMESTAMPTZ       NOT NULL DEFAULT now(),
  
  item_idx      SMALLINT          DEFAULT 0,
  item_ts       TIMESTAMPTZ       DEFAULT now(),
  change_idx    SMALLINT          DEFAULT 0,
  
  contact       BIGINT            NOT NULL,
  payload_hash  T_SHA256_HEX_HASH NOT NULL,
  
  CONSTRAINT wa_api_inbound_payload_metadata_contact_fkey
    FOREIGN KEY (contact)
    REFERENCES public.wa_api_contacts(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_inbound_payload_metadata_hash_unique
    UNIQUE (payload_hash)

);

CREATE INDEX IF NOT EXISTS wa_api_inbound_payload_metadata_received_at_idx
  ON public.wa_api_inbound_payload_metadata (received_at);

CREATE INDEX IF NOT EXISTS wa_api_inbound_payload_metadata_contact_idx
  ON public.wa_api_inbound_payload_metadata (contact);

ALTER TABLE public.wa_api_inbound_payload_metadata
  ENABLE ROW LEVEL SECURITY;


-- ========================================================================================
-- MESSAGES: INBOUND

CREATE TABLE IF NOT EXISTS public.wa_api_inbound_messages (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payload       BIGINT                NOT NULL,
  
  is_echo       BOOLEAN               NOT NULL DEFAULT FALSE,
  msg_id        T_WHATSAPP_MESSAGE_ID NOT NULL,
  msg_ts        TIMESTAMPTZ           NOT NULL,
  msg_type      T_WHATSAPP_MESSAGE    NOT NULL,
  msg_data      JSONB                 NOT NULL,
  
  CONSTRAINT wa_api_inbound_messages_payload_fkey
    FOREIGN KEY (payload)
    REFERENCES public.wa_api_inbound_payload_metadata(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_inbound_messages_msg_id_unique
    UNIQUE (msg_id)

);

CREATE INDEX IF NOT EXISTS wa_api_inbound_messages_payload_idx
  ON public.wa_api_inbound_messages (payload);

ALTER TABLE public.wa_api_inbound_messages
  ENABLE ROW LEVEL SECURITY;

-- MESSAGES: OUTBOUND

CREATE TABLE IF NOT EXISTS public.wa_api_outbound_messages (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sent_at       TIMESTAMPTZ           NOT NULL DEFAULT now(),
  
  contact       BIGINT                NOT NULL,
  msg_id        T_WHATSAPP_MESSAGE_ID NOT NULL,
  msg_type      T_WHATSAPP_MESSAGE    NOT NULL,
  msg_data      JSONB                 NOT NULL,
  
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
  inbound_msg_id    BIGINT    DEFAULT NULL,
  outbound_msg_id   BIGINT    DEFAULT NULL,
  
  mime_type         TEXT      NOT NULL,
  size              BIGINT    NOT NULL,
  prefix            T_NE_STR  NOT NULL,
  caption           TEXT      DEFAULT NULL,
  filename          TEXT      DEFAULT NULL,
  
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
  
  CONSTRAINT wa_api_media_either_inbound_or_outbound
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
  payload       BIGINT                NOT NULL,
  
  msg_id        T_WHATSAPP_MESSAGE_ID NOT NULL,
  msg_status    T_WHATSAPP_STATUS     NOT NULL,
  status_ts     TIMESTAMPTZ           NOT NULL,
  
  conversation  JSONB DEFAULT NULL,
  pricing       JSONB DEFAULT NULL,
  errors        JSONB DEFAULT NULL,
  
  CONSTRAINT wa_api_statuses_payload_fkey
    FOREIGN KEY (payload)
    REFERENCES public.wa_api_inbound_payload_metadata(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_statuses_msg_id_fkey
    FOREIGN KEY (msg_id)
    REFERENCES public.wa_api_outbound_messages(msg_id)
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
  
  msg_id        T_WHATSAPP_MESSAGE_ID     NOT NULL,
  msg_status    T_ENQUEUED_MESSAGE_STATUS NOT NULL DEFAULT 'pending',
  
  CONSTRAINT wa_api_to_case_handler_queue_msg_id_fkey
    FOREIGN KEY (msg_id)
    REFERENCES public.wa_api_inbound_messages(msg_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_to_case_handler_queue_msg_id_unique
    UNIQUE (msg_id)

);

CREATE INDEX IF NOT EXISTS wa_api_to_case_handler_queue_status_idx
  ON public.wa_api_to_case_handler_queue (
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

-- CASE MANIFESTS

CREATE TABLE IF NOT EXISTS public.wa_case_handler_case_manifests (
  
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  contact         BIGINT        NOT NULL,
  created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ   DEFAULT NULL,
  is_open         BOOLEAN       NOT NULL DEFAULT TRUE,
  machine_state   T_NE_STR      DEFAULT NULL,
  
  CONSTRAINT wa_case_handler_case_manifests_contact_fkey
    FOREIGN KEY (contact)
    REFERENCES public.wa_api_contacts(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE

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
  
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ts          TIMESTAMPTZ   NOT NULL DEFAULT now(),
  case_id     BIGINT        NOT NULL,
  basemodel   T_NE_STR      NOT NULL,
  origin      T_NE_STR      DEFAULT NULL,
  data        JSONB         NOT NULL,
  
  CONSTRAINT wa_case_handler_messages_case_id_fkey
    FOREIGN KEY (case_id)
    REFERENCES public.wa_case_handler_case_manifests(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_case_handler_messages_data_object
    CHECK ( jsonb_typeof(data) = 'object' )

);

CREATE INDEX IF NOT EXISTS wa_case_handler_messages_case_order_idx
  ON public.wa_case_handler_messages (
    case_id,
    ts,
    id
  );

ALTER TABLE public.wa_case_handler_messages
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
  
  CONSTRAINT wa_case_handler_to_api_case_handler_msg_id_unique
    UNIQUE (case_handler_msg_id),
  
  CONSTRAINT wa_case_handler_to_api_api_inbound_msg_id_unique
    UNIQUE (api_inbound_msg_id),
  
  CONSTRAINT wa_case_handler_to_api_api_outbound_msg_id_unique
    UNIQUE (api_outbound_msg_id),
  
  CONSTRAINT wa_case_handler_to_api_either_inbound_or_outbound
    CHECK (
      ( api_inbound_msg_id IS NOT NULL ) <> ( api_outbound_msg_id IS NOT NULL )
    )

);

ALTER TABLE public.wa_case_handler_to_api
  ENABLE ROW LEVEL SECURITY;
