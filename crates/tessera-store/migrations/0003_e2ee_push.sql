-- End-to-end encryption key storage, to-device messages, device list stream, push.

CREATE TABLE device_keys (
    user_id         TEXT NOT NULL,
    device_id       TEXT NOT NULL,
    key_json        JSONB NOT NULL,                   -- the signed DeviceKeys object
    updated_ts      BIGINT NOT NULL,
    PRIMARY KEY (user_id, device_id)
);

CREATE TABLE one_time_keys (
    user_id         TEXT NOT NULL,
    device_id       TEXT NOT NULL,
    algorithm       TEXT NOT NULL,                    -- e.g. signed_curve25519
    key_id          TEXT NOT NULL,                    -- the part after the colon
    key_json        JSONB NOT NULL,
    created_ts      BIGINT NOT NULL,
    PRIMARY KEY (user_id, device_id, algorithm, key_id)
);

CREATE TABLE fallback_keys (
    user_id         TEXT NOT NULL,
    device_id       TEXT NOT NULL,
    algorithm       TEXT NOT NULL,
    key_id          TEXT NOT NULL,
    key_json        JSONB NOT NULL,
    used            BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (user_id, device_id, algorithm)
);

CREATE TABLE cross_signing_keys (
    user_id         TEXT NOT NULL,
    key_type        TEXT NOT NULL,                    -- master, self_signing, user_signing
    key_json        JSONB NOT NULL,
    updated_ts      BIGINT NOT NULL,
    PRIMARY KEY (user_id, key_type)
);

-- Signatures uploaded via /keys/signatures/upload, merged into key_json on read.
CREATE TABLE key_signatures (
    target_user_id  TEXT NOT NULL,
    target_key_id   TEXT NOT NULL,                    -- device id or cross-signing key id
    signing_user_id TEXT NOT NULL,
    signing_key_id  TEXT NOT NULL,
    signature       TEXT NOT NULL,
    PRIMARY KEY (target_user_id, target_key_id, signing_user_id, signing_key_id)
);

-- Device list changes stream: who changed, so syncing clients can refetch.
CREATE SEQUENCE device_lists_seq AS BIGINT START 1;
CREATE TABLE device_list_changes (
    stream_id       BIGINT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    device_id       TEXT,
    changed_ts      BIGINT NOT NULL
);
CREATE INDEX device_list_changes_user_idx ON device_list_changes(user_id, stream_id);

-- To-device messages, per recipient device.
CREATE SEQUENCE to_device_seq AS BIGINT START 1;
CREATE TABLE to_device_messages (
    stream_id       BIGINT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    device_id       TEXT NOT NULL,
    sender          TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    content         JSONB NOT NULL,
    created_ts      BIGINT NOT NULL
);
CREATE INDEX to_device_recipient_idx ON to_device_messages(user_id, device_id, stream_id);

-- Push rules (the whole ruleset per user, as served in m.push_rules).
CREATE TABLE push_rules (
    user_id         TEXT PRIMARY KEY,
    ruleset         JSONB NOT NULL,
    stream_id       BIGINT NOT NULL
);

CREATE TABLE pushers (
    user_id         TEXT NOT NULL,
    app_id          TEXT NOT NULL,
    pushkey         TEXT NOT NULL,
    kind            TEXT NOT NULL,
    app_display_name TEXT NOT NULL,
    device_display_name TEXT NOT NULL,
    profile_tag     TEXT,
    lang            TEXT NOT NULL,
    data            JSONB NOT NULL,
    created_ts      BIGINT NOT NULL,
    last_stream_ordering BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, app_id, pushkey)
);
