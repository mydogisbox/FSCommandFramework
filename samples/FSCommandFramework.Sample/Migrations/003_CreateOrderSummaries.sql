CREATE TABLE IF NOT EXISTS order_summaries (
    order_id    TEXT        PRIMARY KEY,
    customer_id TEXT        NOT NULL,
    status      TEXT        NOT NULL,
    items       JSONB       NOT NULL,
    placed_at   TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);
