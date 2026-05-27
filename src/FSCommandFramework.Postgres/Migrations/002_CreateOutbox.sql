CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    stream_id    TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS ix_outbox_unprocessed ON outbox (id) WHERE processed_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_outbox_stream_id ON outbox (stream_id);
