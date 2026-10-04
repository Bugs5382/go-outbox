CREATE TABLE IF NOT EXISTS {{.Table}} (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    idempotency_key text        NOT NULL,
    topic           text        NOT NULL,
    aggregate_key   text,
    payload         bytea       NOT NULL,
    content_type    text        NOT NULL DEFAULT '',
    headers         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    status          text        NOT NULL DEFAULT 'pending'
                    CONSTRAINT {{.Base}}_status_check CHECK (status IN ('pending', 'sent', 'dead')),
    attempts        integer     NOT NULL DEFAULT 0,
    available_at    timestamptz NOT NULL DEFAULT now(),
    lease_until     timestamptz,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    sent_at         timestamptz,
    dead_at         timestamptz,
    CONSTRAINT {{.Base}}_idempotency_key_key UNIQUE (idempotency_key)
);

CREATE INDEX IF NOT EXISTS {{.Base}}_pending_idx
    ON {{.Table}} (id) WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS {{.Base}}_aggregate_pending_idx
    ON {{.Table}} (aggregate_key, id) WHERE status = 'pending' AND aggregate_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS {{.Base}}_sent_idx
    ON {{.Table}} (sent_at) WHERE status = 'sent';

CREATE TABLE IF NOT EXISTS {{.Inbox}} (
    consumer        text        NOT NULL,
    idempotency_key text        NOT NULL,
    processed_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, idempotency_key)
);
