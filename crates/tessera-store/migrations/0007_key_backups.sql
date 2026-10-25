-- Server-side key backups (room_keys).

CREATE TABLE key_backup_versions (
    user_id         TEXT NOT NULL,
    version         BIGINT NOT NULL,
    algorithm       TEXT NOT NULL,                    -- {"algorithm": ..., "auth_data": ...} as sent
    etag            BIGINT NOT NULL DEFAULT 0,
    deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (user_id, version)
);

CREATE TABLE key_backup_keys (
    user_id         TEXT NOT NULL,
    version         BIGINT NOT NULL,
    room_id         TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    first_message_index BIGINT NOT NULL,
    forwarded_count BIGINT NOT NULL,
    is_verified     BOOLEAN NOT NULL,
    session_data    TEXT NOT NULL,                    -- the KeyBackupData JSON as sent
    PRIMARY KEY (user_id, version, room_id, session_id)
);
