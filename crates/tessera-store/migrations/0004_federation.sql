-- Federation: outbound queue, per-destination retry state, inbound transaction de-duplication.

CREATE SEQUENCE federation_outbox_seq AS BIGINT START 1;

-- One row per (destination, item). PDUs reference events by id; EDUs carry their JSON.
CREATE TABLE federation_outbox (
    id              BIGINT PRIMARY KEY DEFAULT nextval('federation_outbox_seq'),
    destination     TEXT NOT NULL,
    kind            TEXT NOT NULL,                    -- 'pdu' or 'edu'
    event_id        TEXT,                             -- for pdu
    edu_json        JSONB,                            -- for edu
    created_ts      BIGINT NOT NULL
);
CREATE INDEX federation_outbox_dest_idx ON federation_outbox(destination, id);

CREATE TABLE federation_destinations (
    destination     TEXT PRIMARY KEY,
    failure_ts      BIGINT,                           -- first failure of the current streak
    retry_last_ts   BIGINT,
    retry_interval_ms BIGINT NOT NULL DEFAULT 0,
    last_successful_ts BIGINT
);

-- Transactions we have processed, so retransmissions get the same result.
CREATE TABLE federation_received_transactions (
    origin          TEXT NOT NULL,
    transaction_id  TEXT NOT NULL,
    response        JSONB NOT NULL,
    received_ts     BIGINT NOT NULL,
    PRIMARY KEY (origin, transaction_id)
);

-- Server discovery cache (well-known / SRV results).
CREATE TABLE federation_server_discovery (
    server_name     TEXT PRIMARY KEY,
    host            TEXT NOT NULL,
    port            INTEGER NOT NULL,
    host_header     TEXT NOT NULL,
    tls_name        TEXT NOT NULL,
    expires_ts      BIGINT NOT NULL
);

-- Stripped state delivered with remote invites, shown to the invitee until they join.
CREATE TABLE invite_state (
    room_id         TEXT NOT NULL,
    user_id         TEXT NOT NULL,
    state           JSONB NOT NULL,
    PRIMARY KEY (room_id, user_id)
);
