-- Custom profile fields (MSC4133 / spec v1.16): arbitrary keys besides displayname and avatar_url.

ALTER TABLE users ADD COLUMN profile_fields JSONB NOT NULL DEFAULT '{}'::jsonb;
