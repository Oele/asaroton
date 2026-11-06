-- Event relations (m.relates_to) and full-text search.

CREATE TABLE event_relations (
    event_id        TEXT PRIMARY KEY,
    room_id         TEXT NOT NULL,
    relates_to      TEXT NOT NULL,                    -- the target event
    rel_type        TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    sender          TEXT NOT NULL,
    origin_server_ts BIGINT NOT NULL,
    stream_ordering BIGINT NOT NULL
);
CREATE INDEX event_relations_target_idx ON event_relations(relates_to, rel_type, stream_ordering);
CREATE INDEX event_relations_room_idx ON event_relations(room_id, rel_type, stream_ordering);

CREATE TABLE event_search (
    event_id        TEXT PRIMARY KEY,
    room_id         TEXT NOT NULL,
    stream_ordering BIGINT NOT NULL,
    origin_server_ts BIGINT NOT NULL,
    vector          TSVECTOR NOT NULL
);
CREATE INDEX event_search_vector_idx ON event_search USING GIN(vector);
CREATE INDEX event_search_room_idx ON event_search(room_id, stream_ordering);
