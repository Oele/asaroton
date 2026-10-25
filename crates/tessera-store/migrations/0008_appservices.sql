-- Application service transaction positions.

CREATE TABLE appservice_positions (
    appservice_id   TEXT PRIMARY KEY,
    stream_ordering BIGINT NOT NULL
);
