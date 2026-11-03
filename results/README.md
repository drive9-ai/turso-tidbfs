# Performance

Recorded November 3, 2026. TiDB Filesystem and local disk use the same
release-built Turso 0.8.3 engine, WAL mode, `synchronous=FULL`, and a 256 KiB
pager cache. The fixture contains 1,024 rows with 512-byte payloads.

Three trials use fresh directories and separate processes for data creation
and transaction measurements. Execution order alternates between FS and local
disk. Values below are medians of trial-average latencies.

| Operation | Local disk (ms) | TiDB Filesystem (ms) |
| --- | ---: | ---: |
| Insert within a transaction, per row | 0.0035 | 0.0079 |
| Commit 1,024 inserted rows | 0.4874 | 2918.1354 |
| Checkpoint + truncate | 0.5299 | 2408.6717 |
| Single-row commit | 0.0628 | 553.3741 |
| 32-row batch commit | 0.1077 | 811.6024 |

## Storage behavior

The 1,024-row commit uses one block-batch request followed by a conditional
manifest PUT. Small WAL-tail updates can be contained in a single manifest
PUT. Referenced blocks are durable before the manifest is published, and
uncertain or failed acknowledgements stop the session.

## Measurement conditions

Local results retain normal OS caching. Local fsync covers the host filesystem;
FS sync includes remote publication. Writes inside a transaction are buffered;
the commit measurements include the durability barrier. Directory preparation,
connection setup, integrity checks, and process teardown are excluded from
per-operation timing. These are sequential microbenchmarks rather than a
concurrency or capacity measurement.

The raw report records the machine, FS endpoint host, executable hash, measured
source revision, individual samples, and HTTP request counters.

## Reproduce

```bash
python3 benchmark.py --phases seed transactions --trials 3
```

Run `python3 benchmark.py --trials 3` to include point reads and a full scan.

## Correctness

The recovery check commits a transaction, kills the process with another
transaction open, and restores it in a fresh sandbox. It verifies committed
rows, absence of uncommitted rows, database integrity, checkpoint/reopen,
stale-writer rejection, and absence of local database files.

```bash
python3 smoke.py
```

[Benchmark data](benchmark.json) · [Recovery verification](recovery.json)

Independent snapshot readers, remote writer leases and failure handoff are
covered by the [live handoff checks](handoff.md).
Automatic read-only following through checkpoint, WAL reset and writer handoff
is covered by the [following check](follow.md).
