# Recorded comparison — November 3, 2026

Same release Turso 0.8.3 engine; WAL, `synchronous=FULL`, 256 KiB pager cache.
Fixture: 1,024 rows × 512-byte payload. Local APFS versus the real FS endpoint
recorded in the JSON report. Three samples per summary group; table values are
the median of trial-average latencies. No database files were used by the FS backend.

| Operation | Local (ms) | FS (ms) |
| --- | ---: | ---: |
| Insert within transaction (per row) | 0.004442 | 0.009 |
| Bulk commit (1,024 rows) | 0.474584 | 15436.925 |
| Distributed first pass (per query) | 0.016211 | 185.580 |
| Repeated cached point read (per query) | 0.006610 | 0.018 |
| Single-row commit | 0.064177 | 1370.591 |
| 32-row batch commit | 0.085931 | 1739.608 |
| Checkpoint + truncate | 0.372708 | 16120.574 |

The initial seed pair overlapped crash verification and was excluded. An
isolated replacement pair restores three seed samples. Both the excluded raw
runs and the replacement are retained in the JSON for auditability.

## Demand-read evidence

Every remote read-phase open fetched **one 32 KiB data block plus the 1,383-byte
manifest** (34,151 bytes), rather than the entire 610,304-byte database.
The distributed pass fetched 17 further blocks; repeated queries fetched
**zero additional bytes**. The full scan then fetched just the remaining
20,480-byte tail. `point_read_one` targets id=0 in the header-adjacent cached
block and must not be interpreted as a cold remote lookup.

## What the comparison means

The reader now avoids eager downloads and dirty updates retain untouched block
references. Cache-hit queries are close to local engine costs. Cold blocks and
commits remain dominated by sequential remote round trips. For example, the
bulk commit issues 19 block existence GETs, 19 block PUTs, and one manifest PUT.
These numbers identify the next optimization targets; they do not establish
production capacity or concurrency performance.

Local OS caches were not dropped, so the baseline is normal local filesystem
performance, not a cold SSD test. Local fsync protects a local filesystem,
whereas FS sync includes remote publication. HTTP preparation and final
integrity checks are outside individual operation timings. Full-scan figures
in the raw JSON are per row and follow the distributed read pass.

## Correctness

Eight focused Rust tests, Clippy with warnings denied, and formatting checks
passed. The real-FS smoke verified SIGKILL/WAL recovery, loss of uncommitted
rows, integrity checks, checkpoint/reopen, stale-writer rejection, and absence
of local database files.

[Raw benchmark data](2026-11-03-benchmark.json) · [Live smoke](2026-11-03-smoke.json)

Reproduce with `python3 benchmark.py --trials 3` and `python3 smoke.py`.
