-- The ingest path connects as an unprivileged user, and it can only read the system tables it is
-- measured by. That is not decoration: in the pipeline this repository is based on, the API's own
-- account could not read system.parts, so nobody could see the part count climbing until inserts
-- started failing.
CREATE USER IF NOT EXISTS lab IDENTIFIED BY 'lab';
GRANT SELECT, INSERT, ALTER, CREATE, DROP, TRUNCATE ON lab.* TO lab;
GRANT SELECT ON system.parts TO lab;
GRANT SELECT ON system.merges TO lab;
GRANT SELECT ON system.query_log TO lab;
GRANT SELECT ON system.errors TO lab;
GRANT SELECT ON system.asynchronous_metrics TO lab;

-- The table is ours. That is the whole reason this pipeline exists instead of a generic
-- observability agent: a funnel over "ad_impression then level_complete" needs columns we chose,
-- and revenue needs a Float64, not a string in a map.
--
-- Two times, both kept forever:
--   event_time  - when the client says it happened (a device can be offline for days)
--   ingested_at - when the ingest API took it (monotonic, always sane)
-- Partitioning on the ingest day, not the event day: a client flushing three days of queued events
-- would otherwise write into sealed partitions, which is where "many parts" comes from.
CREATE DATABASE IF NOT EXISTS lab;

CREATE TABLE IF NOT EXISTS lab.events
(
    event_id     String,
    event_time   DateTime64(3),
    ingested_at  DateTime64(3) DEFAULT now64(3),
    game_id      LowCardinality(String),
    event        LowCardinality(String),
    user_id      String,
    device_id    String,
    session_id   String,
    seq          UInt32,
    country_code LowCardinality(String),
    platform     LowCardinality(String),
    app_version  LowCardinality(String),
    -- Money never goes in a string map: it gets a column, a type and a check.
    value        Float64,
    attrs        Map(LowCardinality(String), String),
    -- Deduplication is by the sorting key, so event_id has to be part of it.
    INDEX idx_user user_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toDate(ingested_at)
ORDER BY (game_id, event, event_time, event_id)
TTL toDate(event_time) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Lines Vector could not parse. A pipeline that drops them silently is a pipeline that lies about
-- its own completeness, so the count of this table is part of the measurement.
CREATE TABLE IF NOT EXISTS lab.events_dlq
(
    ingested_at DateTime64(3) DEFAULT now64(3),
    reason      LowCardinality(String),
    raw         String
)
ENGINE = MergeTree
PARTITION BY toDate(ingested_at)
ORDER BY ingested_at
TTL toDate(ingested_at) + INTERVAL 30 DAY;

-- Rollups: the queries a product dashboard actually asks, answered from 1/1000th of the rows.
CREATE TABLE IF NOT EXISTS lab.events_daily
(
    day        Date,
    game_id    LowCardinality(String),
    event      LowCardinality(String),
    platform   LowCardinality(String),
    country_code LowCardinality(String),
    users      AggregateFunction(uniq, String),
    events     AggregateFunction(count),
    revenue    AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (game_id, event, day, platform, country_code)
TTL day + INTERVAL 2 YEAR;

CREATE MATERIALIZED VIEW IF NOT EXISTS lab.mv_events_daily TO lab.events_daily AS
SELECT
    toDate(event_time)                                    AS day,
    game_id,
    event,
    platform,
    country_code,
    uniqState(user_id)                                    AS users,
    countState()                                          AS events,
    sumState(value)                                       AS revenue
FROM lab.events
GROUP BY day, game_id, event, platform, country_code;
