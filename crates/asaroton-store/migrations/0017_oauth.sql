-- Sign-in through OAuth 2.0 (the Matrix spec's OAuth 2.0 API), served by the authorization
-- server embedded in asaroton (lithostroton). The devices and tokens stay asaroton's own; a
-- device whose tokens were granted to an OAuth client remembers which client, and the scope.
-- Both stay NULL for devices of a password login.
ALTER TABLE devices
    ADD COLUMN oauth_client_id TEXT,
    ADD COLUMN oauth_scope     TEXT;

-- The authorization server's own state: registered clients, sign-ins in progress, codes,
-- browser sessions. A key-value table with expiry; an entry whose expires_at has passed counts
-- as absent and is purged now and then.
CREATE TABLE lithostroton_store (
    kind       TEXT NOT NULL,
    key        TEXT NOT NULL,
    value      BYTEA NOT NULL,
    expires_at TIMESTAMPTZ,
    PRIMARY KEY (kind, key)
);
CREATE INDEX lithostroton_store_expires_idx ON lithostroton_store (expires_at)
    WHERE expires_at IS NOT NULL;
