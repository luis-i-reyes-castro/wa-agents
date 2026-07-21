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
