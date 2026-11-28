-- Account locking (Matrix v1.12) and suspension (v1.13), set by a server admin through
-- `PUT /_matrix/client/v1/admin/lock|suspend/{userId}` (v1.18) or the Synapse-style admin API.
-- Both are reversible and leave the account's sessions in place. Only local accounts have rows
-- in `users`, so only they can be locked or suspended.
ALTER TABLE users
    ADD COLUMN is_locked    BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN is_suspended BOOLEAN NOT NULL DEFAULT FALSE;
