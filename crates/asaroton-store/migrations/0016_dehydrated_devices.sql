-- Dehydrated devices (MSC3814): a device that a user's client leaves with the server, keys
-- included, so that room keys sent while every real device is offline have somewhere to go.
-- One per user. The device itself is a row of `devices` like any other; this is what makes
-- it the dehydrated one, and it goes when that row goes.
CREATE TABLE dehydrated_devices (
    user_id         TEXT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
    device_id       TEXT NOT NULL,
    device_data     JSONB NOT NULL,                   -- the pickled device, encrypted by the client
    created_ts      BIGINT NOT NULL,
    FOREIGN KEY (user_id, device_id) REFERENCES devices(user_id, device_id) ON DELETE CASCADE
);
