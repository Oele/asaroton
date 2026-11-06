-- Transaction-ID de-duplication for client sends, forgotten rooms, receipts, typing is in memory.

CREATE TABLE event_txn_ids (
    user_id         TEXT NOT NULL,
    device_id       TEXT NOT NULL DEFAULT '',
    txn_id          TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    created_ts      BIGINT NOT NULL,
    PRIMARY KEY (user_id, device_id, txn_id)
);

ALTER TABLE room_memberships ADD COLUMN forgotten BOOLEAN NOT NULL DEFAULT FALSE;

CREATE SEQUENCE receipts_seq AS BIGINT START 1;

CREATE TABLE receipts (
    room_id         TEXT NOT NULL,
    user_id         TEXT NOT NULL,
    receipt_type    TEXT NOT NULL,                    -- m.read, m.read.private
    thread_id       TEXT NOT NULL DEFAULT '',         -- '' = unthreaded, 'main' or an event id
    event_id        TEXT NOT NULL,
    data            JSONB NOT NULL,                   -- {"ts": ...}
    stream_id       BIGINT NOT NULL,
    PRIMARY KEY (room_id, user_id, receipt_type, thread_id)
);
CREATE INDEX receipts_room_stream_idx ON receipts(room_id, stream_id);
