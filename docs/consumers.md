# One stream, three audiences

The reason this is one system instead of three is that all three questions are answered from the same
rows. What differs is the read path, not the data.

```
client SDK  validate shape, queue with a cap, one event_id per event
    |
    v
ingest API  schema, ranges, clock skew, enrichment, deduplication key   <- never trusts the client
    |
    v
WAL -> Vector -> lab.events        the raw stream: every event, including the ones nobody asked for
    |
    +--> lab.events_dlq                 lines that could not be parsed - a number, not a silence
    +--> lab.events (by user)           projection for trust and safety
    +--> lab.events_daily               materialised rollups for product and finance
    +--> quality view                   per app version, for the game team
```

## 1. The game team: is the client sending what it thinks it is sending?

The question is per **app version**, because the answer changes the day a build ships. A quality view
over the raw stream answers it without a second pipeline:

```sql
SELECT
    toDate(event_time)                                          AS day,
    app_version,
    count()                                                     AS events,
    countIf(event = 'level_complete' AND attrs['duration_ms'] = '') AS level_complete_without_duration,
    countIf(event = 'iap_purchase'   AND value = 0)              AS purchases_without_value,
    uniqExact(event)                                            AS distinct_events
FROM lab.events
WHERE day >= today() - 7
GROUP BY day, app_version
ORDER BY day DESC, app_version;
```

Three rules make this useful rather than decorative:

1. **Rejects are data.** The dead-letter table is queried like any other, and its size is a metric. The
   alternative - dropping unparseable lines silently - is how a team spends a month not knowing that a
   released build stopped sending an event.
2. **An unknown event name is not a rejection.** Clients ship before servers do. Rejecting event names
   the ingest edge has never seen loses the first day of every release; accepting and flagging them
   gives the team a signal instead of a hole.
3. **The contract lives in a file, in version control.** Event names and their parameters are validated
   in CI against a schema both the client SDK and the ingest API read. Without that, "validate the
   params before sending" means "validate against whatever this build happens to think".

## 2. Trust and safety: what did this player actually do, in order?

This is the read path with a different shape: one user, many events, ordered by time.

```sql
SELECT event_time, ingested_at, event, attrs
FROM lab.events
WHERE game_id = 'game-0' AND user_id = 'u-000123'
ORDER BY event_time
LIMIT 500;
```

The sort key on the table is `(game_id, event, event_time, event_id)` - right for analytics, wrong for
this. Rather than a second copy of the data, add a projection:

```sql
ALTER TABLE lab.events
    ADD PROJECTION events_by_user (SELECT * ORDER BY (game_id, user_id, event_time, event_id));
ALTER TABLE lab.events MATERIALIZE PROJECTION events_by_user;
```

Two things that matter more than the query:

- **Keep both timestamps.** A client that reports events dated last week, delivered today, is itself a
  signal. Collapsing the two times removes the evidence.
- **Most cheat signals are derived, not queried.** "Completed level 120 faster than the level allows"
  is a join between the event stream and the game's own configuration, so it belongs in a scheduled job
  that writes a `cheat_signals` table, not in an ad-hoc query at 2am. That job is also where the
  definition of each signal gets reviewed and versioned.

And the boundary that keeps this honest: **identity stays out of the analytics path**. The raw stream
carries a pseudonymous user id because trust and safety needs it; the rollups carry counts and
distinct-user counts because product does not. A raw IP address is turned into a country at ingest and
then dropped.

## 3. Product and finance: numbers that do not move when you re-run them

Reports read `lab.events_daily`, never the raw table. Two habits turn that from a preference into a
guarantee:

- **Reconcile against the payment provider.** Purchases in the event stream must add up to the
  provider's settlement report, and the difference - fees, refunds, chargebacks, currency - is a named
  line in the report rather than a rounding surprise. This is the check that catches a broken client
  before finance does.
- **Decide the late-data policy once.** "T+1 is provisional, T+3 is final" is a sentence that has to be
  written down, because a phone that was offline for three days is normal, and a dashboard that changes
  its own history without saying so is worse than one that is a day behind.

Session and timezone definitions belong in the same sentence. Change either and last quarter's numbers
change with it - which is a decision, and should be recorded as one.
