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

Use **one active session per directory**. This is optimistic conflict detection,
not a distributed lock service. Separate sessions read their own snapshots and
can make a previous writer's next sync fail. File locks coordinate handles only
within one adapter session. Connections created from one shared Turso `Database`
support reader snapshots and one write transaction at a time; a competing writer
gets `Busy`. Separate `FsIo` instances do not share that coordination, even in
the same process. Their manifests are loaded once and are not automatically
refreshed. Use different remote directories for independent databases.

The adapter's shared state mutex serializes file access, including blocking
HTTP calls. Supporting overlapping SQL transactions is not a claim of parallel
block transfers. The command-line program uses a single sequential connection.
Shared multi-process WAL / `.tshm`, remote database locks and session takeover,
MVCC, partial sync, and Turso Cloud sync are outside this implementation.


[Back to the getting-started guide](../README.md)
