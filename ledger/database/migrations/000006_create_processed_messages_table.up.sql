CREATE TABLE IF NOT EXISTS processed_messages (
    event_id     TEXT        NOT NULL,
    topic        TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_processed_messages PRIMARY KEY (event_id)
);

CREATE INDEX IF NOT EXISTS idx_processed_messages_topic ON processed_messages (topic);
