# Benchmark results

## Commit-path optimization — November 3, 2026

Contemporaneous A/B comparison: original v1 adapter, optimized v2 adapter, and
local disk. Three trials each, separate directories, alternating execution order.
Same release Turso 0.8.3 engine, `synchronous=FULL`, WAL, 1,024 rows with 512-byte
payloads, and 256 KiB pager cache. Table values are medians of trial means.

| Operation | Local (ms) | FS before (ms) | FS after (ms) | Speedup |
| --- | ---: | ---: | ---: | ---: |
| Commit 1,024 inserted rows | 0.487 | 15591.9 | 2918.1 | 5.34× |
| Checkpoint + truncate | 0.530 | 15810.0 | 2408.7 | 6.56× |
| Single-row commit | 0.063 | 1351.3 | 553.4 | 2.44× |
| 32-row batch commit | 0.108 | 1772.8 | 811.6 | 2.18× |

### Request reduction

The 1,024-row commit previously made 19 existence GETs, 19 individual data PUTs,
and one manifest PUT (**39 requests**). The optimized path makes one block-batch
POST and one manifest PUT (**2 requests**), with zero existence GETs. The raw
report includes these per-phase counts. Small WAL-tail updates can be contained
entirely in one CAS manifest PUT; rollover first externalizes the old inline
block when it remains referenced.

The inline optimization trades extra base64/JSON payload bytes for fewer round
trips. It obeys the existing 48 KiB manifest budget and falls back to external
blocks when the budget is exhausted. Batch uploads respect 128 items / 4 MiB
decoded content per request. Manifest publication waits for every block result;
a mixed/uncertain/error acknowledgement fails the session rather than publishing
incomplete data.

### Limits and remaining costs

The FS path still has substantial network and server work. This does not claim
local-disk latency or production capacity. Local fsync covers a local filesystem;
FS sync includes remote publication. OS caches were not dropped. SQL writes
inside a transaction are buffered and are not a durable-commit measurement.
Per-operation timing excludes connection setup, directory preparation, final
integrity checks, and process teardown. No unrelated live-FS tests ran during
these measurements.

Readers accept v1 and v2. A new sync upgrades to v2; old v1 clients cannot read
an upgraded directory. All measurements used fresh disposable paths.

### Verification and reproduction

Sixteen targeted checks passed: eleven core/store/Turso checks and five HTTP
checks covering batch limits, conflicts, malformed acknowledgements, and partial
failure. Clippy (warnings denied), formatting, and secret scanning passed. The
real-FS smoke passed crash/WAL recovery, rollback visibility, integrity, checkpoint
and reopen, stale-writer rejection, and absence of local database files.

The original executable was built from `83946ff`; the optimized implementation
is `6a1b715`. Both executable SHA-256 hashes and all per-transaction samples
are included in the raw data. To reproduce, build `bench` at each revision and
pass the original binary using `--baseline-binary`:

```bash
python3 benchmark.py --baseline-binary /path/to/v1/bench \
  --phases seed transactions --trials 3
```

[Raw A/B data](2026-11-03-optimized-commits.json) · [Optimized recovery smoke](2026-11-03-optimized-smoke.json)

## Earlier demand-read baseline

[Original local/FS comparison](2026-11-03-benchmark.json) · [Original recovery smoke](2026-11-03-smoke.json)

The earlier read run opened a 610,304-byte database with only one 32 KiB block
and the 1,383-byte manifest. Repeated point queries downloaded zero additional
bytes. Those read measurements are retained rather than repeated for this
commit-path change. The v1 bulk-write results are superseded by the A/B table
above; the earlier raw record preserves its excluded/replacement seed samples.
