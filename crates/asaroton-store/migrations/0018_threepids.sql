-- Email addresses on accounts (third-party identifiers, without an identity server). An
-- address belongs to at most one account.
CREATE TABLE user_threepids (
    user_id      TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    medium       TEXT NOT NULL,
    address      TEXT NOT NULL,
    validated_at BIGINT NOT NULL,
    added_at     BIGINT NOT NULL,
    PRIMARY KEY (medium, address)
);
CREATE INDEX user_threepids_user_idx ON user_threepids (user_id);

-- Verifications in progress: a mail went out with a code; once the code comes back the
-- session is validated and can be added to an account. The code is kept as a digest.
CREATE TABLE threepid_sessions (
    sid           TEXT PRIMARY KEY,
    client_secret TEXT NOT NULL,
    medium        TEXT NOT NULL,
    address       TEXT NOT NULL,
    token_hash    TEXT NOT NULL,
    send_attempt  BIGINT NOT NULL,
    next_link     TEXT,
    created_ts    BIGINT NOT NULL,
    last_sent_ts  BIGINT NOT NULL,
    expires_ts    BIGINT NOT NULL,
    validated_at  BIGINT,
    failures      INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX threepid_sessions_secret_idx ON threepid_sessions (client_secret, medium, address);
