-- Events received over federation that could not be checked because an auth event they cite
-- could not be obtained. Such an event is stored as rejected, so that later events can build
-- on it, and listed here so that the check is tried again: not being able to check an event
-- today does not make it invalid.
CREATE TABLE events_awaiting_auth (
    event_id        TEXT PRIMARY KEY,
    room_id         TEXT NOT NULL,
    origin          TEXT NOT NULL,                    -- the server that delivered the event
    first_ts        BIGINT NOT NULL,
    next_try_ts     BIGINT NOT NULL,
    tries           INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX events_awaiting_auth_due ON events_awaiting_auth (next_try_ts);
