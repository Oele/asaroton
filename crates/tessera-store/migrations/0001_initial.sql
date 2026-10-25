-- tessera schema v1: identity, rooms, events, state, account data, signing keys.
-- Conventions: *_ts columns are milliseconds since the Unix epoch (Matrix convention);
-- event JSON is stored as text exactly as signed.

-- ---------------------------------------------------------------- users & devices
CREATE TABLE users (
    user_id         TEXT PRIMARY KEY,                 -- full @localpart:server
    password_hash   TEXT,                             -- NULL: no password login
    display_name    TEXT,
    avatar_url      TEXT,
    is_admin        BOOLEAN NOT NULL DEFAULT FALSE,
    is_guest        BOOLEAN NOT NULL DEFAULT FALSE,
    is_deactivated  BOOLEAN NOT NULL DEFAULT FALSE,
    appservice_id   TEXT,
    created_ts      BIGINT NOT NULL
);

CREATE TABLE devices (
    user_id         TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    device_id       TEXT NOT NULL,
    display_name    TEXT,
    created_ts      BIGINT NOT NULL,
    last_seen_ts    BIGINT,
    last_seen_ip    TEXT,
    last_seen_user_agent TEXT,
    hidden          BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (user_id, device_id)
);

CREATE TABLE access_tokens (
    token           TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    device_id       TEXT,
    created_ts      BIGINT NOT NULL,
    expires_ts      BIGINT,                           -- NULL: never
    last_used_ts    BIGINT,
    refresh_token   TEXT,                             -- the refresh token that can rotate this one
    puppets_user_id TEXT                              -- appservice puppeting
);
CREATE INDEX access_tokens_user_idx ON access_tokens(user_id);
CREATE INDEX access_tokens_device_idx ON access_tokens(user_id, device_id);

CREATE TABLE refresh_tokens (
    token           TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    device_id       TEXT NOT NULL,
    created_ts      BIGINT NOT NULL,
    expires_ts      BIGINT,
    next_token      TEXT                              -- set once rotated; old token still valid until used
);

-- ---------------------------------------------------------------- rooms
CREATE TABLE rooms (
    room_id         TEXT PRIMARY KEY,
    room_version    TEXT NOT NULL,
    creator         TEXT,
    is_public       BOOLEAN NOT NULL DEFAULT FALSE,  -- listed in the room directory
    has_auth_chain_index BOOLEAN NOT NULL DEFAULT TRUE,
    created_ts      BIGINT NOT NULL
);

CREATE TABLE room_aliases (
    alias           TEXT PRIMARY KEY,
    room_id         TEXT NOT NULL,
    creator         TEXT,
    created_ts      BIGINT NOT NULL
);
CREATE INDEX room_aliases_room_idx ON room_aliases(room_id);

-- ---------------------------------------------------------------- events
CREATE SEQUENCE events_stream_seq AS BIGINT START 1;
CREATE SEQUENCE events_backfill_seq AS BIGINT START -1 INCREMENT -1 MINVALUE -9223372036854775807 MAXVALUE -1;

CREATE TABLE events (
    stream_ordering BIGINT PRIMARY KEY,               -- positive: live; negative: backfilled
    event_id        TEXT NOT NULL UNIQUE,
    room_id         TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    state_key       TEXT,
    sender          TEXT NOT NULL,
    origin_server_ts BIGINT NOT NULL,
    depth           BIGINT NOT NULL,                  -- topological ordering
    json            TEXT NOT NULL,                    -- federation format, exactly as signed
    original_json   TEXT,                             -- pre-redaction JSON, set once redacted
    state_group     BIGINT,                           -- state *after* this event; NULL for outliers
    rejected        BOOLEAN NOT NULL DEFAULT FALSE,
    soft_failed     BOOLEAN NOT NULL DEFAULT FALSE,
    outlier         BOOLEAN NOT NULL DEFAULT FALSE,
    received_ts     BIGINT NOT NULL
);
CREATE INDEX events_room_stream_idx ON events(room_id, stream_ordering);
CREATE INDEX events_room_topo_idx ON events(room_id, depth, stream_ordering);
CREATE INDEX events_room_type_idx ON events(room_id, event_type) WHERE state_key IS NOT NULL;
CREATE INDEX events_sender_idx ON events(sender);

CREATE TABLE event_edges (
    event_id        TEXT NOT NULL,
    prev_event_id   TEXT NOT NULL,
    PRIMARY KEY (event_id, prev_event_id)
);
CREATE INDEX event_edges_prev_idx ON event_edges(prev_event_id);

CREATE TABLE event_auth (
    event_id        TEXT NOT NULL,
    auth_event_id   TEXT NOT NULL,
    PRIMARY KEY (event_id, auth_event_id)
);
CREATE INDEX event_auth_auth_idx ON event_auth(auth_event_id);

CREATE TABLE forward_extremities (
    room_id         TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    PRIMARY KEY (room_id, event_id)
);

-- Events we have seen referenced (prev/auth) but do not have; drives fetching/backfill.
CREATE TABLE missing_events (
    room_id         TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    referenced_by   TEXT NOT NULL,
    PRIMARY KEY (room_id, event_id, referenced_by)
);

-- ---------------------------------------------------------------- state
CREATE TABLE state_groups (
    id              BIGSERIAL PRIMARY KEY,
    room_id         TEXT NOT NULL,
    prev_group      BIGINT REFERENCES state_groups(id)
);
CREATE INDEX state_groups_room_idx ON state_groups(room_id);

-- Delta of a state group against its prev_group (full state when prev_group IS NULL).
CREATE TABLE state_groups_state (
    state_group     BIGINT NOT NULL REFERENCES state_groups(id) ON DELETE CASCADE,
    event_type      TEXT NOT NULL,
    state_key       TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    PRIMARY KEY (state_group, event_type, state_key)
);

-- Resolved current state of each room.
CREATE TABLE current_state_events (
    room_id         TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    state_key       TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    membership      TEXT,                             -- denormalised for m.room.member rows
    PRIMARY KEY (room_id, event_type, state_key)
);
CREATE INDEX current_state_member_idx ON current_state_events(state_key, membership) WHERE event_type = 'm.room.member';

-- Per-user membership view (local and remote users), maintained from current state.
CREATE TABLE room_memberships (
    room_id         TEXT NOT NULL,
    user_id         TEXT NOT NULL,
    membership      TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    sender          TEXT NOT NULL,
    display_name    TEXT,
    avatar_url      TEXT,
    stream_ordering BIGINT NOT NULL,
    PRIMARY KEY (room_id, user_id)
);
CREATE INDEX room_memberships_user_idx ON room_memberships(user_id, membership);

-- ---------------------------------------------------------------- account data
CREATE SEQUENCE account_data_seq AS BIGINT START 1;

CREATE TABLE account_data (
    user_id         TEXT NOT NULL,
    room_id         TEXT NOT NULL DEFAULT '',         -- '' for global account data
    data_type       TEXT NOT NULL,
    content         JSONB NOT NULL,
    stream_id       BIGINT NOT NULL,
    PRIMARY KEY (user_id, room_id, data_type)
);
CREATE INDEX account_data_stream_idx ON account_data(user_id, stream_id);

CREATE TABLE filters (
    user_id         TEXT NOT NULL,
    filter_id       BIGINT NOT NULL,
    filter_json     JSONB NOT NULL,
    PRIMARY KEY (user_id, filter_id)
);

-- ---------------------------------------------------------------- signing keys
CREATE TABLE server_signing_keys (
    key_id          TEXT PRIMARY KEY,                 -- ed25519:<version>
    seed_b64        TEXT NOT NULL,                    -- 32-byte seed, unpadded base64
    created_ts      BIGINT NOT NULL,
    expired_ts      BIGINT                            -- NULL: active
);

-- Cache of other servers' keys, as fetched from them or a notary.
CREATE TABLE remote_server_keys (
    server_name     TEXT NOT NULL,
    key_id          TEXT NOT NULL,
    verify_key_b64  TEXT NOT NULL,
    valid_until_ts  BIGINT NOT NULL,
    expired_ts      BIGINT,                           -- from old_verify_keys
    fetched_ts      BIGINT NOT NULL,
    fetched_from    TEXT NOT NULL,
    key_json        TEXT NOT NULL,                    -- full signed response for notary re-serving
    PRIMARY KEY (server_name, key_id)
);
