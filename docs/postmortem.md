# Postmortem: the pipeline this repository is based on

Anonymised on purpose: no company, no player, no event payload appears here. What is left is the part
that transfers - seven decisions that each looked reasonable and each cost something measurable.

The pipeline: a mobile game client queued events in a local SQLite database, delivered them to a small
Go ingest API, which appended them to a file, which an agent tailed and forwarded into ClickHouse.
Measured on three months of production data before it was retired.

| what the data said | what it meant |
|---|---|
| 534,109 rows over 3 months | about 5,700 events per day - about 0.07 events per second |
| 40,610 duplicate rows (7.6%) | the client had no event identifier, so every retry inserted a second copy |
| 9 rows dated 2031 | one client's clock was wrong and the pipeline believed it |
| `data` and `metadata` as JSON strings | every query paid to parse them, and nothing could be filtered cheaply |

## 1. `parse_json!` — the exclamation mark was the whole bug

The agent's transform parsed each line with `parse_json!`. The `!` means "panic the program if this
fails", and the result was not an error the operator could see: the event was dropped, one line was
written to a container log, and the transform carried on. A game client that emits one non-JSON line
(a crash dump appended to the same file, a partial write during a kill) silently loses events forever.

**The fix is a branch, not a bang**: parse, keep the error, route the unparseable line to a dead-letter
table, and count it. `deploy/vector.toml` here does exactly that, and `lab.events_dlq` is the table that
makes the loss visible. A pipeline that cannot tell you how many lines it rejected is a pipeline that
lies about its own completeness.

## 2. The table was safe for writes and hostile to reads

Partitioned by nothing. Sorted by `(created_at, key)`. The queries that mattered were "one game, one
event, over a time range", which that sort order cannot prune, so every query scanned spans it did not
need. Three months of TTL on a table with no partition key means the TTL has to rewrite parts to delete
data, instead of dropping a partition and being done.

**The fix**: `PARTITION BY toDate(ingested_at)` and `ORDER BY (game_id, event, event_time, event_id)` -
the key reads the queries, and `event_id` in the key is what turns the engine's deduplication on.

## 3. Partitioning on event time punishes the clients that were offline

The instinct is to partition by event time, because that is what the analytics group by. But a phone
that was offline for three days delivers three-day-old events, and each of those writes into a sealed
partition that is already merged and compressed. Partition on **ingest** day, keep event time as a
column, and late data costs nothing.

## 4. A shared key inside the client binary

The client shipped with `ClientId = "..."` and `SecretKey = "..."` as constants. Anyone can decompile a
mobile build, so for an anti-cheat dataset this is worse than an open endpoint: it is an endpoint that
anybody can *write* to, with events that look exactly like real ones.

**Fix**: no secret in the client, per-install tokens, server-side plausibility checks, rate limits, and
the assumption that every client event is untrusted until a server-side fact corroborates it.

## 5. The queue deleted itself at exactly the wrong moment

```csharp
if (logs.Count > 2000)
{
    foreach (var log in logs) _database.DeleteLog(log);
    yield break;
}
```

When the local queue passed 2,000 unsent events - which is what being offline for a while *means* - the
client deleted every one of them without sending anything. The README promised "no data loss".

**The pattern to keep**: cap the queue with eviction of the *oldest* events, never the whole queue, and
count what you dropped so the loss is a number you can attribute.

## 6. The sync coroutine that ran once and then never again

```csharp
if (_isOnline && _syncCoroutine == null) _syncCoroutine = StartCoroutine(SyncLogs());
```

`SyncLogs()` set `_syncCoroutine = null` on two of its three exit paths. On the normal path - events
present, under the cap - the field kept pointing at a finished coroutine, so the guard never passed
again. Events kept being written to SQLite and stayed there until the player restarted the game, at
which point bug 5 was waiting.

**The lesson that generalises**: a helper that cannot prove it started is not started. This lab's
harness has the same rule - the ingest API prints its mode and pid in `/healthz`, and the harness
refuses to measure a service it did not start.

## 7. One HTTP request per event

The sync loop did `foreach (var log in logs) StartCoroutine(SendLog(log))`. Two thousand queued events
became two thousand concurrent uploads. The cost lands on the phone (battery, sockets, cellular), the
ingest edge, and the store - and it is the same mistake as Arm A in the measurements of this
repository, one layer further out. Chunk it: 200 events per POST, sequential, with jittered retries.

## What the rewrite keeps

Two times on every row (`event_time` from the client, `ingested_at` from the server), an `event_id` the
client generates so retries are deduplicable, a queue that drops the oldest instead of everything, a
request that carries many events, a dead-letter table instead of a silent drop, and a harness that has
to prove which service it measured.
