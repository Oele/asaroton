-- Thread subscriptions (MSC4306); stream ids double as the bump stamps of MSC4308.
CREATE SEQUENCE thread_subscriptions_seq;
CREATE TABLE thread_subscriptions (
    user_id         TEXT NOT NULL,
    room_id         TEXT NOT NULL,
    thread_root     TEXT NOT NULL,
    subscribed      BOOLEAN NOT NULL,
    automatic       BOOLEAN NOT NULL DEFAULT FALSE,
    stream_id       BIGINT NOT NULL,
    unsubscribed_at BIGINT,            -- newest event position when the user unsubscribed
    PRIMARY KEY (user_id, room_id, thread_root)
);
CREATE INDEX thread_subscriptions_stream_idx ON thread_subscriptions(user_id, stream_id);
