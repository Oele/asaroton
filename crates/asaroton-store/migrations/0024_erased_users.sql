-- Accounts erased when they were deactivated (`erase: true` on `/account/deactivate`, Matrix
-- v1.10). Whoever joins a room after the erasure is served redacted copies of the account's
-- events; who was there before still gets the originals. `erased_at` is the position of the
-- events stream at the erasure, which tells the two apart; `erased_ts` is when it happened (ms).
CREATE TABLE erased_users (
    user_id   TEXT PRIMARY KEY,
    erased_at BIGINT NOT NULL,
    erased_ts BIGINT NOT NULL
);
