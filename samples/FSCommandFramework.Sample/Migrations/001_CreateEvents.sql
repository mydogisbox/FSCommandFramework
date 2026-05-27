CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL PRIMARY KEY,
    stream_id   TEXT        NOT NULL,
    sequence    INT         NOT NULL,
    event_type  TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT uq_events_stream_sequence UNIQUE (stream_id, sequence)
);

CREATE INDEX IF NOT EXISTS ix_events_stream_id ON events (stream_id);
