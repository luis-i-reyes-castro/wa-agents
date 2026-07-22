CREATE TABLE IF NOT EXISTS public.wa_users (
  
  operator_id  TEXT NOT NULL,
  user_id      TEXT NOT NULL,
  
  data         JSONB    NOT NULL,
  next_case_id INTEGER  NOT NULL  DEFAULT 1,
  open_case_id INTEGER            DEFAULT NULL,
  
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  CONSTRAINT wa_users_pkey
    PRIMARY KEY (
      operator_id,
      user_id
    ),
  CONSTRAINT wa_users_next_case_id_positive
    CHECK (
      next_case_id > 0
    )
);

CREATE TABLE IF NOT EXISTS public.wa_cases (
  
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  
  operator_id       TEXT NOT NULL,
  user_id           TEXT NOT NULL,
  case_id           INTEGER NOT NULL,
  
  model             TEXT,
  status            TEXT NOT NULL DEFAULT 'open',
  time_opened       TIMESTAMPTZ NOT NULL,
  time_last_message TIMESTAMPTZ,
  time_closed       TIMESTAMPTZ,
  
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  CONSTRAINT wa_cases_user_fkey
    FOREIGN KEY (
      operator_id,
      user_id
    )
    REFERENCES public.wa_users (
      operator_id,
      user_id
    )
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  
  CONSTRAINT wa_cases_unique
    UNIQUE (
      operator_id,
      user_id,
      case_id
    ),
  CONSTRAINT wa_cases_case_id_positive
    CHECK (
      case_id > 0
    )
);

CREATE TABLE IF NOT EXISTS public.wa_messages (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  operator_id     TEXT NOT NULL,
  user_id         TEXT NOT NULL,
  case_id         INTEGER NOT NULL,
  message_id      TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  basemodel       TEXT NOT NULL,
  role            TEXT NOT NULL,
  time_created    TIMESTAMPTZ NOT NULL,
  time_received   TIMESTAMPTZ NOT NULL,
  payload         JSONB NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  CONSTRAINT wa_messages_case_fkey FOREIGN KEY (
    operator_id,
    user_id,
    case_id
  )
  REFERENCES public.wa_cases (
    operator_id,
    user_id,
    case_id
  )
  ON UPDATE CASCADE
  ON DELETE CASCADE,
  
  CONSTRAINT wa_messages_message_unique UNIQUE (
    operator_id,
    user_id,
    case_id,
    message_id
  ),
  CONSTRAINT wa_messages_idempotency_unique UNIQUE (
    operator_id,
    user_id,
    idempotency_key
  )
);

CREATE INDEX IF NOT EXISTS wa_messages_case_order_idx
  ON public.wa_messages (
    operator_id,
    user_id,
    case_id,
    time_created,
    time_received,
    id
  );

CREATE INDEX IF NOT EXISTS wa_cases_user_status_idx
  ON public.wa_cases (
    operator_id,
    user_id,
    status
  );

CREATE TABLE IF NOT EXISTS public.wa_webhook_payloads (
  
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payload_hash TEXT  NOT NULL,
  object_type  TEXT,
  payload      JSONB NOT NULL,
  received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  CONSTRAINT wa_webhook_payloads_hash_unique
    UNIQUE (
      payload_hash
    )
);

CREATE TABLE IF NOT EXISTS public.wa_webhook_messages (
  
  id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payload_id           BIGINT NOT NULL,
  operator_id          TEXT   NOT NULL,
  display_phone_number TEXT,
  waba_id              TEXT,
  user_id              TEXT NOT NULL,
  message_id           TEXT NOT NULL,
  message_type         TEXT NOT NULL,
  timestamp            TIMESTAMPTZ,
  media_id             TEXT,
  media_mime_type      TEXT,
  context_message_id   TEXT,
  payload              JSONB NOT NULL,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  CONSTRAINT wa_webhook_messages_payload_fkey
    FOREIGN KEY (
      payload_id
    )
    REFERENCES public.wa_webhook_payloads (
      id
    )
    ON UPDATE CASCADE
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS wa_webhook_messages_message_idx
  ON public.wa_webhook_messages (
    operator_id,
    user_id,
    message_id
  );

CREATE INDEX IF NOT EXISTS wa_webhook_messages_time_idx
  ON public.wa_webhook_messages (
    operator_id,
    timestamp
  );

CREATE TABLE IF NOT EXISTS public.wa_webhook_statuses (
  
  id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payload_id           BIGINT NOT NULL,
  operator_id          TEXT   NOT NULL,
  display_phone_number TEXT,
  waba_id              TEXT,
  recipient_id         TEXT NOT NULL,
  message_id           TEXT NOT NULL,
  status               TEXT NOT NULL,
  timestamp            TIMESTAMPTZ,
  conversation_id      TEXT,
  pricing_category     TEXT,
  payload              JSONB NOT NULL,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  CONSTRAINT wa_webhook_statuses_payload_fkey
    FOREIGN KEY (
      payload_id
    )
    REFERENCES public.wa_webhook_payloads (
      id
    )
    ON UPDATE CASCADE
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS wa_webhook_statuses_message_idx
  ON public.wa_webhook_statuses (
    operator_id,
    recipient_id,
    message_id
  );

CREATE INDEX IF NOT EXISTS wa_webhook_statuses_time_idx
  ON public.wa_webhook_statuses (
    operator_id,
    timestamp
  );

CREATE TABLE IF NOT EXISTS public.wa_incoming_queue (
  
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payload_hash TEXT   NOT NULL,
  payload      JSONB  NOT NULL,
  status       TEXT   NOT NULL DEFAULT 'pending',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error   TEXT,
  
  CONSTRAINT wa_incoming_queue_payload_hash_unique
    UNIQUE (
      payload_hash
    ),
  CONSTRAINT wa_incoming_queue_status_check
    CHECK (
      status IN (
        'pending',
        'processing',
        'done',
        'error'
      )
    )
);

CREATE INDEX IF NOT EXISTS wa_incoming_queue_status_idx
  ON public.wa_incoming_queue (
    status,
    created_at,
    id
  );
