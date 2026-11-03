# Storage design

## Demand reads and writes

Opening a logical file creates only an in-memory descriptor: size and immutable
block references come from `manifest.json`. No data blocks are fetched until
`pread` needs them. Turso's own open/recovery can immediately read its header or
WAL pages; those reads are demand I/O too, not a pre-download of every file.

- Blocks are **32 KiB** and verified with SHA-256 and expected length.
- Repeated reads hit cache. Each file retains up to **64 clean blocks** using LRU
  eviction. Dirty blocks stay pinned until a successful remote sync, so pending
  writes can temporarily exceed that clean-cache bound.
- Partial overwrites fetch only the source blocks they must preserve. Full
  block overwrites never fetch the previous contents.
- Truncate/grow tracks a retained snapshot boundary and zero-fills discarded
  bytes: old immutable data cannot reappear after a truncate and regrow.
- Sync hashes changed/new blocks, preserves untouched block references, uploads
  immutable data, and publishes the manifest last. Unrelated blocks are not read
  during sync. Clean cached blocks can then be evicted safely.

The storage directory contains:

```text
manifest.json          # atomic map of logical database/WAL files
blocks/<sha256>        # immutable blocks; last block can be shorter
```

These objects are adapter storage, not ordinary SQLite files that another
filesystem client can open directly.

### Commit protocol

New immutable blocks are sent through `/v1/fs:batch-write`, up to 128 blocks per
request (4 MiB decoded content). There are no existence GETs before a create.
Only a definite per-item 409 with revision zero can reuse an existing object,
and its contents must then match exactly. Every item acknowledgement is checked;
partial failure, missing/duplicate paths, or a post-commit error stops publication.
The manifest always follows the data batch in a separate conditional PUT.

The last WAL block can be embedded as base64 inside the manifest when the whole
manifest stays within 48 KiB. A small steady-state commit then has **one remote CAS update**,
with WAL bytes and references published together. When that tail rolls into a
normal block, its previous inline contents are uploaded before the next manifest
replaces them. If the inline budget is unavailable, normal block upload remains
the fallback. This does not weaken `FULL` durability or defer an acknowledged
commit into a background upload.

Benchmark and smoke runs use fresh directories.
The HTTP adapter requires an FS deployment with batch-write support.

## Durability and conflict behavior

A conditional PUT with `X-Dat9-Expected-Revision` publishes all logical file
versions through one manifest. `sync` reports completion only after the server
acknowledges a strictly newer revision. Stale sessions cannot separately
replace another session's database or WAL. Sessions never rebase local mutations
onto another writer's manifest.

Upload failure, a manifest conflict, or an uncertain acknowledgement poisons
the session. Discard its connections and reopen remote state. A failed commit
may already have reached the server: reconcile via SQL before retrying an
application operation. Unpublished blocks are harmless orphans; there is no
block garbage collection yet.

## Writer lease and takeover

The manifest contains an optional lease: a unique owner, heartbeat sequence and
TTL in milliseconds. Lease acquisition, renewal, release and file publication
all conditionally replace **the same manifest object**. There is no separate
lock file whose expiry can race an unguarded database PUT. Immutable block
uploads confer no write ownership; only the manifest CAS makes them visible.

- **Acquire:** read and validate the manifest, then CAS a new owner into that
  revision before opening Turso. Only a definite revision conflict can be
  re-observed. An uncertain acquisition result is not retried or released.
- **Renew:** an idle background thread advances the heartbeat sequence. File
  publication also advances it. The local deadline starts before sending the
  request. Both the previous deadline and the new lifetime must still be valid
  when the acknowledgement arrives. A late or unknown acknowledgement cannot
  revive ownership.
- **Expire:** the writer checks its monotonic deadline before and after I/O.
  Renewal or mutation failures poison the session permanently. The CLI also
  checks health around SQL execution, including queries satisfied from cache.
- **Take over:** a contender observes the *same owner, sequence and TTL* for a
  full TTL using its own monotonic clock, re-reading while it waits. A changed
  heartbeat restarts that observation window. It then CASes a new owner into
  the observed revision. No wall-clock synchronization is required. A paused
  contender still has to win that CAS against the latest state.
- **Fence:** after takeover, any old publication, heartbeat or release carries
  an obsolete expected revision and is rejected by the server. This includes
  a request that was already in flight when ownership changed. If the old
  request wins first, the contender loses CAS and must re-observe; it cannot
  overwrite the winning update or open Turso as a writer first.
- **Release:** after closing engine connections, a healthy owner clears its
  lease using CAS. This permits immediate handoff. A failed or expired owner
  does not attempt release and cannot clear a successor's ownership. Explicit
  `FsIo::close()` reports release failures; Drop performs best-effort cleanup.

The command-line program defaults to a 30-second TTL, 5-second heartbeat and
90-second lock wait. The library's default lock wait is zero. Contenders retain
no observation history across restarts, so recovery can require a full TTL
from the first observation, plus network and engine recovery time. No server
sweeper is needed. The existing conditional FS API and directory-scoped tokens
are sufficient; this does not use the inode-based extent lock API.

Ownership is an adapter protocol for cooperating clients. It requires atomic
revision CAS on an unchanged manifest inode and immutable blocks. Other clients
with write permission must not delete, rename, replace or edit internal storage
objects, bypass conditional writes, or restore old manifests. It is not mandatory
access control against arbitrary authorized filesystem clients.

## Independent readers

`FsIo::snapshot` reads one manifest and retains its database/WAL images for the
reader's entire lifetime. Open the engine read-only. It never acquires or renews
the writer lease, creates remote directories, publishes files, or checkpoints
remote data. The adapter rejects writes even if a caller supplies writable open
flags. Snapshots can coexist with writer commits and takeover because referenced
blocks remain immutable and inline bytes belong to the captured manifest.
There is no block garbage collection; any future GC must account for readers.

A fixed snapshot sees commits in its captured WAL, not later publications.
Separate databases must use separate directories because one manifest and lease
govern the directory as a unit. Connections from one shared Turso `Database`
still support reader snapshots and one write transaction at a time; a competing
transaction receives `Busy`.

### Automatic following

`FollowingReader` prepares new immutable engines in a background polling thread.
It reads exactly one manifest per attempt, validates it, ignores lease-only
updates, and opens a read-only `Database` against the captured database/WAL
images. Schema preparation must succeed before the candidate replaces the
current generation. Failed or malformed candidates leave the last usable
engine in place; status exposes the error and subsequent attempts retry.
Manifest revisions cannot move backwards.

A `FollowingConnection` adopts the latest prepared generation before an
execute/query call only if Turso reports autocommit mode. It never parses SQL
to guess transaction state. Explicit `BEGIN` and `SAVEPOINT` transactions retain
their original connection, engine, schema and file images until they finish.
The next call after COMMIT/ROLLBACK can upgrade. A multi-statement batch stays
on one generation for the full call. Results are fully materialized; raw Turso
statement handles are not exposed by this wrapper. Session-specific PRAGMAs
and temporary state are not migrated when the connection changes generations.

This is snapshot replacement, not appending bytes into an already-open engine's
WAL. Reopening the matched pair handles checkpoint, WAL truncation/restart,
writer recovery and schema changes without modifying Turso's private WAL index
or page cache. Existing transactions retain their old immutable data; no remote
reader lock can delay the writer's checkpoint. Generations without current
ownership or connections are dropped. Shared multi-process WAL memory is not
required. A held read transaction intentionally continues to see its old version.

### Shared immutable block cache

Following generations share a bounded cache keyed by SHA-256. Only bytes whose
hash and expected length validate enter the cache, including valid inline WAL
tails. Each file descriptor copies from those immutable bytes into its private
blocks; mutations cannot alter shared entries. Cache hits revalidate expected
length. Concurrent misses for one hash share a result; unrelated hashes fetch
without holding the cache mutex. Errors are not retained as block entries.
LRU eviction limits the cache to the configured block count (default 256, at
most 8 MiB of payload). Turso page caches and private file caches are additional
memory, and active old transactions keep their engine generation alive.

The cache is shared within one follower, not between OS processes. The existing
per-file 64-clean-block limit still applies. Unchanged database blocks can be
reused across engine preparation, while changed or evicted blocks remain demand
reads. There is no WAL-only incremental recovery optimization in this version.

Polling defaults to one second between attempts. Visibility delay also includes
network, recovery and schema preparation; refresh does not block SQL on the
reader-state mutex during I/O. Brief mutex holds select or publish a generation.
A failed refresh may leave queries serving older data, so monitor
`last_refresh_error`, `applied_revision`, `observed_revision`, and each connection's
`snapshot_revision`. Lease-only revisions do not imply new SQL data. The CLI's
`--read-only` mode follows automatically; `--snapshot` and `FsIo::snapshot` stay
fixed. Reader refresh never acquires writer ownership or writes remote state.

The shared state mutex serializes file access, including blocking HTTP calls.
Heartbeats cannot renew while that mutex is held by a long operation; a stalled
operation can therefore expire the session. Manifest requests are bounded by the
remaining confirmed lifetime and success is checked again after the response.
Block transfers can finish as harmless orphans after expiry, but publication
cannot proceed. Increase TTL for workloads with longer I/O, rather than retrying
an expired session. Shared multi-process WAL / `.tshm`, experimental MVCC,
partial sync and Turso Cloud sync are outside this implementation.

[Back to the getting-started guide](../README.md)
