-- Sticky events (Matrix v1.20, MSC4354): the events of each room that stay sticky until
-- `expires_at` (ms). A row is written when an event that carries a sticky marker is accepted
-- (not rejected, soft-failed or an outlier) and deleted some time after it expires; readers
-- always check `expires_at` themselves.
CREATE TABLE sticky_events (
    event_id        TEXT PRIMARY KEY,
    room_id         TEXT NOT NULL,
    sender          TEXT NOT NULL,
    stream_ordering BIGINT NOT NULL,
    expires_at      BIGINT NOT NULL
);
CREATE INDEX sticky_events_room_idx ON sticky_events (room_id, stream_ordering);
CREATE INDEX sticky_events_sender_idx ON sticky_events (room_id, sender);
CREATE INDEX sticky_events_expiry_idx ON sticky_events (expires_at);

-- Sticky events that were soft-failed on arrival and are still within their sticky time:
-- when the room's state changes they are checked again, and one that passes now is accepted
-- (MSC4354). Small by nature, so checking a room costs one indexed look.
CREATE TABLE soft_failed_sticky (
    event_id   TEXT PRIMARY KEY,
    room_id    TEXT NOT NULL,
    expires_at BIGINT NOT NULL
);
CREATE INDEX soft_failed_sticky_room_idx ON soft_failed_sticky (room_id);

-- A delayed event may be sent as a sticky one: the marker's name and the duration asked for.
ALTER TABLE delayed_events
    ADD COLUMN sticky_marker TEXT,
    ADD COLUMN sticky_duration_ms BIGINT;
