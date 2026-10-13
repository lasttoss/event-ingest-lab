# Keeping raw events, cheaply, for a long time

Design notes for the part that outlives the lab: what stays hot, what goes to object storage, and why
"gzip last month" is not the saving it looks like.

## The measurement that changes the argument

ClickHouse already compresses its own data. Measured on this repository's table:

| | size for 200,000 events |
|---|---|
| ClickHouse on disk | 5.57 MiB (**29 bytes per row**) |
| the same rows as newline-delimited JSON, gzipped with `-9` | about 4.9 MB (**16x**) |

So re-compressing a month of data into an archive saves approximately nothing *unless you then delete
the original*. The saving is the deletion, not the gzip. Per column, the compression ratios explain
where to look:

| column | stored size | ratio |
|---|---|---|
| `event_id` | 1.04 MiB | 4.71x |
| `session_id` | 864 KiB | 1.94x |
| `attrs` (a Map of JSON) | 766 KiB | 5.86x |
| `seq` (a counter) | 746 KiB | **1.00x** |

A column that compresses 1.0x is a column that is eating the disk. Order of attack:

1. Stop storing the same event twice - the pipeline this is based on had 7.6% duplicate rows, which is
   7.6% of the storage bill spent on nothing.
2. `LowCardinality(String)` for `event`, `platform`, `country_code`, `app_version`.
3. `CODEC(ZSTD(3))` on long strings, `Delta`/`DoubleDelta` on timestamps and counters.
4. Extract the fields that get filtered into real columns. A `Map` of strings is a place to put things
   you have not decided about yet, not a schema.
5. Drop the columns nobody queries.

## Day, not month

Both end up in a bucket, so the unit still matters - for four reasons:

| | per day | per month |
|---|---|---|
| one corrupt file costs | a day | a month |
| write, verify, retry | 1-10 GB | hundreds of GB |
| query pruning | 29 of 30 objects skipped | full scan |
| backfill after a bug | one day, in parallel | a month, sequentially |

If you want month-sized objects, compact them *in the bucket* (day -> week -> month) and let the
catalogue hold the grouping. The file naming should follow the partition, not the reporting period.

## Two different things that both say "S3"

**1. Object storage as ClickHouse's cold volume.** `TTL toDate(ingested_at) + INTERVAL 90 DAY TO VOLUME
'cold'` with an S3-backed disk keeps the data queryable *as ClickHouse data*. One copy, no export job,
movement handled by the engine. Cost: latency and S3 request charges - keep a local cache, and keep the
compute in the same region as the bucket, because egress is where this gets expensive.

**2. An independent Parquet/Iceberg archive.** The data stops being a database's private concern and
becomes files anyone can read: DuckDB, Athena, Spark, or ClickHouse through `s3()` / `icebergS3()`. This
is what you replay from when a rollup definition turns out to be wrong, and it is what is still readable
if the database is not.

An events pipeline wants both, and they are not substitutes: the cold volume makes day-to-day queries
cheaper, the archive makes the data yours.

Two consequences worth stating plainly:

- **Do not unzip into a second database to query it.** Parquet can be queried where it lies, with
  partition and column pruning. A gzipped file cannot: finding one user means reading the whole file.
- **Verify before you drop.** Count the rows in the export, compare with the partition, and only then
  `DROP PARTITION`. This is the same class of bug as a client that deletes its whole queue when the queue
  gets long: a delete that was supposed to be safe, made against an unverified assumption.

## When the ingest path changes

| sustained rate | what the ingest path should look like |
|---|---|
| under ~1,000 events/s (roughly 500k DAU) | collector -> ClickHouse with batching and `async_insert`; nothing else |
| ~1k-20k events/s | add a durable log (Kafka, Redpanda, NATS JetStream) for burst absorption and replay; shard ClickHouse; export hourly |
| above ~20k events/s | the durable log plus a stream processor for event-time windows and exactly-once sinks; the warehouse becomes the serving layer over the lake |

Sanity check on the units: 150 events per player per day means 100k DAU is about 175 events/s, and 1M
DAU about 1,750 events/s. "A hundred thousand requests per second" is a top-ten mobile game worldwide,
which is a different problem from a casual game with a few thousand events a day - and the pipeline
above looks the same until it does not.

## What the aggregate layer buys

Raw events are the expensive part; the aggregates are not. Keeping rollups in ClickHouse for years
costs a rounding error compared to the raw stream, so the split is: raw hot for as long as
investigations need it (90 days is a reasonable default), aggregates forever, and object storage as the
archive of record in between. The queries a product asks - daily actives, retention, funnels, revenue -
then never touch the archive at all.
