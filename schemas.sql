CREATE TABLE station_status (
    station_id VARCHAR(150) PRIMARY KEY,
    is_renting BOOLEAN,
    is_returning BOOLEAN,
    is_installed BOOLEAN,
    num_bikes_disabled INTEGER,
    num_docks_disabled INTEGER,
    num_bikes_available INTEGER,
    num_ebikes_available INTEGER,
    num_docks_available INTEGER,
    last_reported TIMESTAMP,
    data_retrieved_ts TIMESTAMP
);

CREATE TABLE station_info (
    station_id VARCHAR(150) PRIMARY KEY,
    name VARCHAR,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    capacity INTEGER,
    data_retrieved_ts TIMESTAMP
);
