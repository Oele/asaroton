-- OpenID tokens handed to clients (the client-server openid/request_token endpoint), verified by
-- other servers through the federation openid/userinfo endpoint.
CREATE TABLE openid_tokens (
    token       TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL,
    expires_ts  BIGINT NOT NULL
);
CREATE INDEX openid_tokens_expiry_idx ON openid_tokens(expires_ts);

-- MatrixRTC over LiveKit (MSC4195): participants this server issued SFU tokens to, so that a user
-- who leaves or is banned can be removed from the SFU and a stray SFU participant can be checked.
CREATE TABLE rtc_participants (
    lk_room      TEXT NOT NULL,
    lk_identity  TEXT NOT NULL,
    user_id      TEXT NOT NULL,
    room_id      TEXT NOT NULL,
    issued_ts    BIGINT NOT NULL,
    PRIMARY KEY (lk_room, lk_identity)
);
CREATE INDEX rtc_participants_user_idx ON rtc_participants(room_id, user_id);
CREATE INDEX rtc_participants_issued_idx ON rtc_participants(issued_ts);

-- Delayed leave events the client handed to the server: kept alive while the participant is
-- connected to the SFU, sent as soon as it disconnects.
CREATE TABLE rtc_delegated_leaves (
    delay_id      TEXT PRIMARY KEY REFERENCES delayed_events(delay_id) ON DELETE CASCADE,
    user_id       TEXT NOT NULL,
    room_id       TEXT NOT NULL,
    lk_room       TEXT NOT NULL,
    lk_identity   TEXT NOT NULL,
    created_ts    BIGINT NOT NULL,
    -- When the participant was last seen connected (ms); NULL until the first sighting.
    connected_ts  BIGINT
);
CREATE UNIQUE INDEX rtc_delegated_leaves_participant_idx ON rtc_delegated_leaves(lk_room, lk_identity);
