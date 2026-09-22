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
  
  waba_id               T_NO_WS_STR NOT NULL, -- '^[0-9]+$'
  phone_number_id       T_NO_WS_STR NOT NULL, -- '^[0-9]+$'
  display_phone_number  T_NO_WS_STR NOT NULL, -- '^[0-9]+$'
  
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
  
  business      BIGINT      NOT NULL,
  wa_id         T_NO_WS_STR DEFAULT NULL, -- '^[0-9]+$'
  user_id       T_NO_WS_STR DEFAULT NULL, -- '^[A-Z]{2}\.[A-Za-z0-9]{1,128}$'
  
  CONSTRAINT wa_api_contacts_business_fkey
    FOREIGN KEY (business)
    REFERENCES public.wa_api_businesses(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_api_contacts_business_id_unique
    UNIQUE ( business, id),
  
  CONSTRAINT wa_api_contacts_business_wa_id_unique
    UNIQUE ( business, wa_id),
  
  CONSTRAINT wa_api_contacts_business_user_id_unique
    UNIQUE ( business, user_id),
  
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


-- ========================================================================================
-- MESSAGES: INBOUND

CREATE TABLE IF NOT EXISTS public.wa_api_inbound_messages (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payload       BIGINT              NOT NULL,
  
  is_echo       BOOLEAN             NOT NULL DEFAULT FALSE,
  msg_id        T_NO_WS_STR         NOT NULL, -- '^wamid\.[A-Za-z0-9+/=]+$'
  msg_ts        TIMESTAMPTZ         NOT NULL,
  msg_type      T_WHATSAPP_MESSAGE  NOT NULL,
  msg_data      JSONB               NOT NULL,
  
  CONSTRAINT wa_api_inbound_messages_payload_fkey
    FOREIGN KEY (payload)
    REFERENCES public.wa_api_inbound_payloads(id)
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
  CASE HANDLER ROUTING
  =========================================================================================
*/

CREATE TABLE IF NOT EXISTS public.wa_case_handler_routes (
  
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  business      BIGINT      NOT NULL,
  contact       BIGINT      DEFAULT NULL, -- For contact-specific handler
  handler_key   T_NO_WS_STR NOT NULL,
  handler_url   T_NO_WS_STR DEFAULT NULL, -- For remote handler URL
  
  CONSTRAINT wa_case_handler_routes_business_fkey
    FOREIGN KEY (business)
    REFERENCES public.wa_api_businesses(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_case_handler_routes_contact_fkey
    FOREIGN KEY ( business, contact)
    REFERENCES public.wa_api_contacts( business, id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_case_handler_routes_business_contact_unique
    UNIQUE NULLS NOT DISTINCT ( business, contact)

);

CREATE INDEX IF NOT EXISTS wa_case_handler_routes_handler_key_idx
  ON public.wa_case_handler_routes (handler_key);

ALTER TABLE public.wa_case_handler_routes
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
