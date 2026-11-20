-- /search with a period (net.oele.search_filters): keeps "this room, between these dates"
-- cheap in a busy room, beside the word and substring indexes.
CREATE INDEX event_search_room_ts_idx ON event_search (room_id, origin_server_ts);
