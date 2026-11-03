# Automatic read-only following

Recorded November 3, 2026 against TiDB Filesystem with release-built Turso 0.8.3.
The test starts two reader processes once and keeps both alive through every
step. The fixture begins with 128 rows and 1 KiB payloads per row. Readers use
directory-scoped tokens permitting only `read,list`.

The test uses 250 ms between refresh attempts, a 256-block shared cache per
reader, and writer leases with 12-second TTL and 2-second heartbeat. Normal
defaults are a 1-second reader refresh interval and 30/5-second writer timing.

| Observation | Measured time |
| --- | ---: |
| Follow a new committed row | 206 ms |
| Follow a commit after WAL truncate/restart | 974 ms |
| Follow a schema change and new row | 730 ms |
| Catch up after restoring the reader's transport | 732 ms |
| Follow the replacement writer's committed row | 949 ms |
| Follow a later commit by that writer | 1,149 ms |
| First query after ending the pinned transaction | 581 ms |
| Follow a commit after another WAL reset | 785 ms |
| Follow a commit after a clean writer handoff | 1,209 ms |

These are individual observations, not percentiles or a freshness SLA.
For commits, timing begins after the writer acknowledges COMMIT and ends when a
reader query returns the expected count. It includes polling phase, background
engine preparation and demand reads. A prepared snapshot can exist before that
first query; multiple readers are observed sequentially. Takeover itself took
15.816 seconds through replacement-engine readiness, measured separately from
the visibility of its subsequent commit.

## Verified behavior

- Both reader PIDs remain unchanged. Queries automatically follow updates.
- One reader holds an explicit transaction across commits, checkpoint, WAL reset,
  schema changes and expired-writer takeover. It retains 128 rows and the old
  two-column schema until COMMIT; the next query adopts the prepared generation.
- A reader transport outage leaves cached queries on its last snapshot available and sets
  `last_refresh_error`. Restoring transport lets the same process catch up and
  clears that error.
- A stopped writer cannot commit its abandoned row after resuming. Its successor
  continues to commit; readers also follow a later clean writer handoff.
- Both readers finish with 136 rows, sum of IDs 9,180, no abandoned row, and
  `integrity_check=ok`. No local database files are created.
- Each reader prepares 15 engine generations. Each shared cache contains 20
  verified blocks totaling 369,488 bytes, with 28 and 68 shared-cache hits,
  respectively. Both stay within the 256-block limit.

Unit regressions additionally prove that unchanged database blocks are not
downloaded again when switching generations, concurrent requests for the same
hash share one fetch, errors do not populate the cache, and eviction preserves
the bound. They cover transaction/savepoint pinning, malformed and failed refresh
recovery, schema refresh, repeated WAL reset, and queries running while snapshot
preparation is blocked on I/O.

## Reproduce

```bash
cargo build --release --locked --bins
env -u TIDBFS_ENDPOINT -u TIDBFS_TOKEN \
  python3 follow.py --fs-id "$FS_ID"
```

The script uses the Ti CLI's stored credentials, creates temporary scoped
tokens, and retains test data under `/demos/turso-follow/`. Results go to
`output/follow.json`. Explicit endpoint/token mode is also supported; in that
mode the supplied token must cover the generated test directory and is reused
by both writer and readers.

The [raw report](follow.json) records the executable hash, unchanged reader PIDs,
test settings, exact observations, final reader status and shared cache counters.
See [storage design](../docs/storage.md) for snapshot switching and cache semantics.
