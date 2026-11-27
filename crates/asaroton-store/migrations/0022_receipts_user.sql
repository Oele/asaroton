-- A user's read receipts in every room at once, for the badge count of a push (tracker #100):
-- the primary key starts with the room.
CREATE INDEX receipts_user_idx ON receipts (user_id);
