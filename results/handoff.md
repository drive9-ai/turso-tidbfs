# Writer handoff and snapshot readers

Recorded November 3, 2026 against TiDB Filesystem with the release-built native
adapter and Turso 0.8.3. Each scenario uses a fresh remote directory, a 12-second
lease TTL and 2-second heartbeat. Normal CLI defaults are 30 and 5 seconds.

| Scenario | Takeover through engine ready | Open after clean release | Old COMMIT after restoration |
| --- | ---: | ---: | --- |
| Cut old writer's HTTP transport, then restore it | 15.928 s | 2.830 s | Rejected; process exited with status 1 |
| Pause old process with SIGSTOP, then resume it | 15.136 s | 2.788 s | Rejected; process exited with status 1 |

Takeover time starts before the new process prepares its directory and observes
the lease, and ends when its engine reports ready. It includes HTTP requests,
the full observation window, lease acquisition and WAL recovery. Clean handoff
also includes process startup and engine open; it does not include an expiry
wait. These are individual scenario measurements, not latency percentiles or
a failover SLA.

Both scenarios verify:

- An idle writer renews for longer than one TTL, and another writer is rejected.
- Two independent read-only processes retain their original snapshots while
  the writer commits, fails and is replaced.
- The successor recovers all three acknowledged rows, excluding an abandoned
  transaction, then commits two more rows.
- Restoring the old process cannot commit its abandoned transaction or release
  the successor's ownership. The successor can still write and exclude competitors.
- Reopening a reader sees all five final rows; final checkpoint/reopen preserves
  them, `integrity_check` returns `ok`, and no local database files are created.

The restored process reports:

```text
FS session failed or expired; discard it and reopen from remote state
```

The network case uses a disposable loopback proxy only for the old process.
It drops new HTTP connections during the fault; other processes reach the
same remote filesystem directly. The pause case stops the complete old process,
including its heartbeat thread. No lease is edited or force-released by the test.

Unit regressions separately gate an old manifest PUT already in flight, allow
another owner to acquire, then release that PUT and verify the store CAS rejects
it. They also cover late and lost claim/renewal acknowledgements, writer races,
read-only enforcement and expired-owner cleanup. This is correctness coverage
for the adapter protocol; arbitrary clients bypassing CAS or changing internal
objects are outside that protocol. See [storage contract](../docs/storage.md).

## Reproduce

```bash
cargo build --release --locked --bins
env -u TIDBFS_ENDPOINT -u TIDBFS_TOKEN \
  python3 handoff.py --fs-id "$FS_ID"
```

The script uses the Ti CLI's stored credentials to issue a temporary scoped
token, and writes its report to `output/handoff.json`. Use `--failure network`
or `--failure pause` for one scenario. The raw report records the executable
hash, test parameters, remote test directories, exact timings, rejected-process
error and exit status, final rows and their SHA-256 digest. It contains no tokens.

An additional check with the final release build opens the saved database using
a token limited to `read,list`, verifies all five rows and integrity, then sends
an obsolete expected revision directly to the FS API. The server returns HTTP
409 and the manifest bytes and revision remain unchanged. Its executable hash
and outcomes are recorded under `additional_verification` in the raw report.

[Raw report](handoff.json) · [Getting started](../README.md)
