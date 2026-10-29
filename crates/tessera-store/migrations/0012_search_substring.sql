-- Substring search: the plain text next to the lexeme vector, with a trigram index. The text is
-- only stored when substring search is enabled (`tessera search-backfill` fills older rows).
CREATE EXTENSION IF NOT EXISTS pg_trgm;
ALTER TABLE event_search ADD COLUMN value TEXT;
CREATE INDEX event_search_value_trgm_idx ON event_search USING GIN (value gin_trgm_ops);
