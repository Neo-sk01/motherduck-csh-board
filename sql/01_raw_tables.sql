CREATE TABLE IF NOT EXISTS queue_config (
    queue_id    VARCHAR PRIMARY KEY,
    queue_label VARCHAR,
    description VARCHAR,
    discovered_at TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS raw_cdrs (
    cdr_id            VARCHAR PRIMARY KEY,
    to_id             VARCHAR,
    start_time        TIMESTAMP,
    answer_time       TIMESTAMP,
    end_time          TIMESTAMP,
    duration          INTEGER,
    caller_name       VARCHAR,
    user_extension    VARCHAR,
    ingested_at       TIMESTAMP DEFAULT current_timestamp,
    ingestion_batch   VARCHAR
);

CREATE TABLE IF NOT EXISTS raw_queue_stats (
    queue_id            VARCHAR,
    queue_label         VARCHAR,
    date_start          DATE,
    date_end            DATE,
    calls_offered       INTEGER,
    calls_handled       INTEGER,
    abandoned_calls     INTEGER,
    abandoned_rate      DOUBLE,
    average_talk_time   INTEGER,
    average_handle_time INTEGER,
    ingested_at         TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (queue_id, date_start, date_end)
);

CREATE TABLE IF NOT EXISTS raw_queue_splits (
    queue_id     VARCHAR,
    queue_label  VARCHAR,
    interval_ts  TIMESTAMP,
    volume       INTEGER,
    period_type  VARCHAR,
    ingested_at  TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (queue_id, interval_ts, period_type)
);
