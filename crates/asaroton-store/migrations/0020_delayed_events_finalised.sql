-- Delayed events as merged into the spec (MSC4140, v1.20): a sent or cancelled delayed event is
-- kept for a while with how it ended, so GET /v1/delayed_events/{delayId} can tell the client;
-- pending ones are those with finalised_ts NULL.
ALTER TABLE delayed_events
    ADD COLUMN finalised_ts BIGINT,
    ADD COLUMN finalised_event_id TEXT,
    ADD COLUMN finalised_error JSONB;
DROP INDEX delayed_events_due_idx;
CREATE INDEX delayed_events_due_idx ON delayed_events ((running_since + delay_ms)) WHERE finalised_ts IS NULL;
CREATE INDEX delayed_events_finalised_idx ON delayed_events (user_id, finalised_ts) WHERE finalised_ts IS NOT NULL;

-- The delayed event an event was sent from, for `unsigned.delay_id` when the event is served
-- to its sender; kept after the delayed event's own record is pruned.
CREATE TABLE event_delay_ids (
    event_id TEXT PRIMARY KEY,
    user_id  TEXT NOT NULL,
    delay_id TEXT NOT NULL
);
