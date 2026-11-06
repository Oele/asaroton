-- Media repository metadata; file bytes live on disk under the data directory.

CREATE TABLE media (
    server_name     TEXT NOT NULL,
    media_id        TEXT NOT NULL,
    user_id         TEXT,                             -- uploader (local media)
    content_type    TEXT,
    size            BIGINT,
    upload_name     TEXT,
    sha256          TEXT,
    created_ts      BIGINT NOT NULL,
    uploaded_ts     BIGINT,                           -- NULL while an async upload is pending
    quarantined     BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (server_name, media_id)
);
CREATE INDEX media_user_idx ON media(user_id);

CREATE TABLE media_thumbnails (
    server_name     TEXT NOT NULL,
    media_id        TEXT NOT NULL,
    width           INTEGER NOT NULL,
    height          INTEGER NOT NULL,
    method          TEXT NOT NULL,
    content_type    TEXT NOT NULL,
    size            BIGINT NOT NULL,
    PRIMARY KEY (server_name, media_id, width, height, method)
);

-- Pushers remember the access token that created them so a password change that logs out
-- other devices also removes their pushers.
ALTER TABLE pushers ADD COLUMN access_token TEXT;
