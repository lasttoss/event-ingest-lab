# GitHub metadata (paste into the repository settings)

**Description**
> One event-log system for three audiences - the game team checking that a client sends valid
> parameters, trust and safety reading one player's timeline, and product reading daily stats - with a
> measured write path: 20,000 single-row inserts lose half the events, the same events batched into
> 5,000-row inserts lose none.

**Topics**
golang, clickhouse, vector, event-pipeline, event-tracking, analytics, anti-cheat, data-quality,
write-ahead-log, batching, observability, game-backend, load-testing, docker-compose

**Pinned blurb for the profile README**
> `event-ingest-lab` - the same 200,000 events, two write paths: one INSERT per event loses half the
> data and leaves seven parts behind; batched inserts lose nothing and leave four.
