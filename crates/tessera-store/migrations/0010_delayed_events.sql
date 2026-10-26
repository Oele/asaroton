-- Delayed events (MSC4140): scheduled sends that fire after a timeout unless cancelled.
CREATE TABLE delayed_events (
    delay_id        TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    device_id       TEXT,
    room_id         TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    state_key       TEXT,
    content         JSONB NOT NULL,
    delay_ms        BIGINT NOT NULL,
    running_since   BIGINT NOT NULL,   -- ms since the epoch; due at running_since + delay_ms
    txn_id          TEXT,
    created_ts      BIGINT NOT NULL
);
CREATE INDEX delayed_events_user_idx ON delayed_events(user_id, created_ts);
CREATE INDEX delayed_events_due_idx ON delayed_events((running_since + delay_ms));
CREATE INDEX delayed_events_state_idx ON delayed_events(room_id, event_type, state_key) WHERE state_key IS NOT NULL;
CREATE UNIQUE INDEX delayed_events_txn_idx ON delayed_events(user_id, COALESCE(device_id, ''), txn_id) WHERE txn_id IS NOT NULL;
