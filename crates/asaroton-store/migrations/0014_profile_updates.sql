-- Profile updates for /sync (MSC4429): when each profile field of a local user last changed.
-- One row per user and field; the value itself stays in the profile, so a sync reports the
-- current value (or null when the field is gone) for every row past its token.

CREATE SEQUENCE profile_updates_seq AS BIGINT START 1;

CREATE TABLE profile_updates (
    user_id     TEXT NOT NULL,
    field_id    TEXT NOT NULL,                  -- 'displayname', 'avatar_url' or a custom field
    stream_id   BIGINT NOT NULL,
    PRIMARY KEY (user_id, field_id)
);
CREATE INDEX profile_updates_stream_idx ON profile_updates(stream_id);
