# turso-tidbfs

**Run SQL in an agent's sandbox. Keep the saved data in TiDB Filesystem.**

[Turso](https://github.com/tursodatabase/turso) is a SQLite-compatible database
engine. This project gives it native I/O backed by
[TiDB Filesystem](https://www.pingcap.com/tidb/tidb-cloud-filesystems/), so a new
process can reopen the same committed data after the original process exits.

```text
Your application → SQL → Turso in your process → HTTPS → TiDB Filesystem
```

No FUSE mount or local database file is needed. This is an early prototype;
each remote database directory supports one writer and multiple snapshot readers.

## Why use it?

An agent can build up useful state across many steps: which documents it has
processed, which tasks are finished, and what its tools returned. When that
state lives only on a sandbox's disk, replacing the sandbox can lose the work.

`turso-tidbfs` lets your application keep that structured state in SQL tables
with committed data stored in TiDB Filesystem. A new process can open the same
filesystem and database directory, query the saved records, and use them to
continue the task.

- **Keep progress across restarts.** Store task status, processing checkpoints,
  and results independently of the process that produced them.
- **Query the state your agent needs.** Use SQL to find unfinished work, inspect
  results, and update related records in a transaction.
- **Use your workspace's storage and permissions.** Database state can live
  alongside input files and generated artifacts in the same FS namespace,
  with access delegated through directory-scoped tokens.
- **Read data as it is needed.** Native I/O fetches blocks on demand, so opening
  a database does not require downloading the complete database file or
  mounting a filesystem.

For example, a research agent can commit the IDs of documents it has processed
and keep large source files in FS. After that process exits, another process
opens the same database and queries which documents remain. Your application
uses those recovered records to decide what the agent should do next and
provides the logic for resuming its workflow.

This prototype fits small databases and commits at task boundaries. See
[performance](results/README.md) for remote commit costs and [limits](#limits)
for the current size and concurrency bounds.

## Getting started

The walkthrough below is for **macOS or Linux** and uses a TiDB Cloud account.
It takes you from credentials to a committed row, then reopens that row in a
new process.

### 1. Install the tools

You need Git, a current stable Rust toolchain, a C compiler/linker, and the
TiDB Cloud CLI (`ti`). Python 3 is needed only for the smoke test and benchmark.

Install Rust using the [official Rust installer](https://rust-lang.org/tools/install/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
. "$HOME/.cargo/env"
cargo --version
```

For native build tools, macOS users can run `xcode-select --install` if the
Command Line Tools are missing. On Ubuntu/Debian, run
`sudo apt-get update && sudo apt-get install -y build-essential`.

Install the [TiDB Cloud CLI](https://github.com/tidbcloud/ti-cli):

```bash
curl -fsSL https://github.com/tidbcloud/ti-cli/releases/latest/download/install.sh | sh -s -- --yes
export PATH="$HOME/.ti/bin:$PATH"
ti --version
```

Skip an installation step if the tool is already available.

### 2. Configure TiDB Cloud access

Create a public/private API key pair in the
[TiDB Cloud API Keys console](https://tidbcloud.com/org-settings/api-keys), then run:

```bash
ti configure
```

Enter the public key, private key, and a Filesystem region when prompted. For
example, `aws-us-east-1` is supported. Choose a region close to your application;
remote commit latency includes network travel time. See the
[CLI setup guide](https://github.com/tidbcloud/ti-cli#quick-start-guide) for other
supported regions.

**These Cloud API keys manage the filesystem. Your application will use a
separate FS token, obtained in the next step.**

### 3. Create a filesystem and get an FS token

Create a filesystem and capture its ID:

```bash
export FS_ID="$(ti fs create-file-system \
  --display-name turso-workspace \
  --wait \
  --query file_system_id --output text)"
```

`--wait` returns when the filesystem is ready. The CLI stores its owner token
locally, so you can use it to issue a limited application token.

<details>
<summary>Already have a filesystem? Use it instead of creating another one.</summary>

List your filesystems and copy the desired `file_system_id`:

```bash
ti fs list-file-systems
export FS_ID="YOUR_FILE_SYSTEM_ID"
```

Use the same CLI region/profile that owns the filesystem. If no owner token is
stored locally, generate and store one using your configured Cloud API keys:

```bash
ti fs generate-file-system-token \
  --file-system-id "$FS_ID" \
  --token-name turso-tidbfs-owner \
  --ttl 24h \
  --store-locally \
  --query file_system_id --output text
```

If the CLI says another token is already selected, add `--replace` only when
you want to replace that local selection. This does not revoke the old remote
token. See [FS token management](https://docs.pingcap.com/ai/manage-filesystem-tokens/)
for importing or managing existing tokens.

</details>

Choose a directory for this database, discover the filesystem's endpoint, and
issue a token that can read and write only that directory:

```bash
export FS_ROOT="/apps/turso-demo"

export TIDBFS_ENDPOINT="$(ti fs check-file-system \
  --file-system-id "$FS_ID" \
  --query endpoint.base_url --output text)"

export TIDBFS_TOKEN="$(ti fs generate-file-system-scoped-token \
  --file-system-id "$FS_ID" \
  --subject turso-tidbfs \
  --ttl 24h \
  --allow "$FS_ROOT:read,list,write" \
  --query fs_token --output text)"
```

Keep these variables in the terminal where you will run the program:

| Variable | Meaning |
| --- | --- |
| `FS_ID` | The filesystem selected in `ti`; used to manage access. |
| `FS_ROOT` | A remote directory containing this database's state. |
| `TIDBFS_ENDPOINT` | The HTTPS endpoint for that filesystem's region. |
| `TIDBFS_TOKEN` | The FS token used by this program, not a Cloud API key. |

The token expires after 24 hours. Repeat the token-generation command to renew
access to the same data. Keep tokens out of source files and Git commits.

### 4. Clone and compile

```bash
git clone https://github.com/drive9-ai/turso-tidbfs.git
cd turso-tidbfs
cargo build --release --locked --bins
```

The first build downloads dependencies and can take a few minutes. The SQL
executable is **`target/release/turso-fs`**; the local-disk benchmark executable
is `target/release/bench`.

### 5. Run SQL

Start the program in the same terminal where you set the variables:

```bash
./target/release/turso-fs "$FS_ROOT"
```

The program creates the remote storage directory if needed. When it prints
`"ready":true`, enter the following **one complete SQL batch per line**:

```sql
CREATE TABLE IF NOT EXISTS tasks(id INTEGER PRIMARY KEY, title TEXT, status TEXT);
BEGIN; INSERT OR REPLACE INTO tasks VALUES(1,'Summarize the paper','done'); COMMIT;
?SELECT id,title,status FROM tasks;
```

Prefix a query with `?` to print its rows. The last line returns:

```json
{"ok":true,"rows":[["1","Summarize the paper","done"]]}
```

Press **Ctrl+D** on an empty line to close the program.

### 6. Reopen the saved data

Start a new process with the same filesystem and directory:

```bash
./target/release/turso-fs "$FS_ROOT"
```

Then query the table:

```sql
?SELECT id,title,status FROM tasks;
?PRAGMA integrity_check;
```

The saved row is still there, and the integrity check returns `ok`. A different
remote directory starts a different database.

In a new terminal on the same machine, set the same `FS_ID` and `FS_ROOT`, then
repeat the endpoint/token commands from step 3. To use a separate sandbox,
build the executable there and pass it `TIDBFS_ENDPOINT`, `TIDBFS_TOKEN`, and
`FS_ROOT`. Generate that scoped token on your trusted machine; the sandbox
needs neither Cloud API keys nor the filesystem's owner token. Reuse the
existing filesystem rather than creating another one.

The logical database name defaults to `agent.db`. To choose a name, pass it as
the second argument:

```bash
./target/release/turso-fs "$FS_ROOT" research.db
```

## Run the recovery check

The smoke script creates its own directory and temporary token using the CLI's
stored owner token. Clear the interactive example's environment variables for
this command only, since its token is limited to `/apps/turso-demo`:

```bash
env -u TIDBFS_ENDPOINT -u TIDBFS_TOKEN \
  python3 smoke.py --fs-id "$FS_ID"
```

It commits rows, kills the process with an uncommitted transaction open, and
reopens the database in a fresh process. It also checks integrity, checkpoint
and reopen, writer exclusion, and absence of local database files.
Results are written to `output/results.json`.

## Compare with local disk

```bash
env -u TIDBFS_ENDPOINT -u TIDBFS_TOKEN \
  python3 benchmark.py --fs-id "$FS_ID" --trials 3
```

The benchmark uses the same Turso engine, SQL workload, and `synchronous=FULL`
for both FS and local disk. It measures point reads, a full scan, writes inside
a transaction, durable commits, and checkpointing. Results go to
`output/benchmark.json`. See [recorded performance](results/README.md) for the
fixture and measurement conditions.

Both scripts can use `--profile NAME`. If exactly one active filesystem has a
local owner token, `--fs-id` can be omitted. They generate 30-minute tokens and
unique directories under `/demos/turso/` and `/demos/turso-bench/`. Test data is
retained remotely for inspection.

If you set `TIDBFS_ENDPOINT` and `TIDBFS_TOKEN` explicitly for these scripts, that
token must allow their `/demos/` paths. A token limited to the interactive
example's directory cannot access those test directories.

## Troubleshooting

| What you see | What to check |
| --- | --- |
| `ti: command not found` | Add `$HOME/.ti/bin` to `PATH`. |
| Linker or C compiler missing | Install Xcode Command Line Tools on macOS, or `build-essential` on Ubuntu/Debian. |
| Missing environment variable | Set `TIDBFS_ENDPOINT` and `TIDBFS_TOKEN` in the terminal running `turso-fs`. |
| No local owner token | Use the existing-filesystem instructions in step 3 to generate or import one. |
| HTTP 401 | Issue a new FS token; the current one may have expired or been revoked. |
| HTTP 403 | Check that the token permits `read,list,write` for `FS_ROOT`. For scripts, use their CLI-token mode shown above. |
| Remote database is busy | The writer lease is held. Close the owner, wait for expiry, or increase `TIDBFS_LOCK_TIMEOUT_MS`. |
| Failed or expired session / HTTP 409 | Discard the old process and reopen. Check committed state before retrying a failed application operation. |

## How storage works

Reads fetch **32 KiB blocks on demand**, with a bounded cache. Writes upload
immutable blocks and publish a conditional manifest after the data is durable.
A small WAL tail can be stored directly in that manifest. The program uses
`synchronous=FULL`; a commit waits for remote confirmation.

See [storage design](docs/storage.md) for the I/O interfaces, cache, commit
protocol, and failure behavior.

## One writer, multiple readers

Independent processes can read the database while one process owns its writer
lease. Read-only processes automatically follow committed updates without
restarting. An open read transaction keeps its original snapshot until it ends.

In one terminal, start the writer normally:

```bash
./target/release/turso-fs "$FS_ROOT"
```

In other terminals, set the same endpoint, token and directory, then open readers:

```bash
./target/release/turso-fs --read-only "$FS_ROOT"
?SELECT id,title,status FROM tasks;
```

A read-only process does not acquire the writer lease or modify remote state.
It can use a token scoped to `"$FS_ROOT:read,list"`. A background worker polls the
manifest and prepares a new read-only engine when the file images change.
The next query adopts the prepared version; explicit transactions stay on their
original version:

```sql
BEGIN;
?SELECT count(*) FROM tasks;
-- A writer can commit, checkpoint, or be replaced while this transaction is open.
?SELECT count(*) FROM tasks;
COMMIT;
-- The next query can adopt the latest prepared snapshot.
?SELECT count(*) FROM tasks;
```

`SAVEPOINT` transactions are pinned too. Switching happens between input lines
(or library execute/query calls), never partway through a SQL batch. For a fixed
snapshot that stays unchanged until the process exits, use `--snapshot` instead
of `--read-only`.

| Reader setting | Default |
| --- | --- |
| `TIDBFS_REFRESH_MS` | `1000` — pause between background refresh attempts; range 10–60,000 ms. |
| `TIDBFS_CACHE_BLOCKS` | `256` — shared immutable blocks per reader process, up to 8 MiB of payload; range 1–4,096 blocks. |

The block cache is shared across engine generations and connections within the
reader, reusing verified blocks and coalescing simultaneous requests for the
same hash. It is separate from Turso's page cache and each generation's private
file cache. Polling plus engine preparation causes asynchronous visibility;
there is no fixed freshness bound. Lease-only heartbeat updates do not replace
the engine. Checkpoint, WAL restart and writer takeover are handled by opening
a matched database/WAL pair from one manifest.

If a refresh fails, queries can continue on the last usable snapshot while the
background worker retries. Uncached blocks still require remote access.
Query responses include `snapshot_revision` and
`reader` status: `applied_revision` is the latest prepared snapshot, which can
be newer than a pinned transaction; `last_refresh_error` reports refresh failures.
Monitor that status when freshness matters. Connection-specific settings and
temporary state are not carried between engine generations.

### Let agents take turns writing

A second writer waits for ownership before opening the engine. A clean exit
releases the lease immediately. After a crash or loss of connectivity, a new
process can take over once it has observed an unchanged lease for a full TTL.
The new owner advances the same manifest revision used for commits, so an old
process cannot publish against that revision after takeover.
For short agent tasks, open a writer, commit the batch, then close the process
to hand ownership to the next agent. A live writer keeps its lease even while idle.

| Setting | Default in the command-line program |
| --- | --- |
| `TIDBFS_LEASE_TTL_MS` | `30000` — confirmed writer lifetime; also the observation window for takeover. |
| `TIDBFS_HEARTBEAT_MS` | `5000` — renew an idle writer's lease. Commits also renew it. |
| `TIDBFS_LOCK_TIMEOUT_MS` | `90000` — maximum time to wait for ownership; use `0` to fail immediately when busy. |

Heartbeat must be positive and at most one third of TTL. TTL must be between
100 and 300,000 ms. Shorter values reduce failover time but tolerate less network
latency or process stalls. Blocking file I/O shares the heartbeat's state mutex;
choose a TTL that comfortably exceeds the longest I/O operation. Takeover timing
includes observation, HTTP requests and engine recovery; it is not a fixed SLA.

An expired session, failed renewal or uncertain publication permanently stops
that writer. Restoring its network connection does not renew its ownership.
Discard the old connections and reopen a new instance. A failed commit can have
an uncertain outcome: query the recovered database before retrying application work.

For embedded use, `FsIo::writer(store, options)` or `FsIo::new(store)` acquires
ownership; `SessionOptions::default()` fails immediately on contention.
`FollowingReader::open(store, name, ReaderOptions::default())` starts automatic
following; use its `connect()`, then `FollowingConnection::execute()` / `query()`.
Keep the reader alive while its connections need updates. `refresh()` explicitly
checks once, and `status()` exposes freshness and shared cache statistics.
`FsIo::snapshot(store)` remains a fixed snapshot; open Turso with
`OpenOptions::new(dialect).flags(OpenFlags::ReadOnly)`. Connections from one
shared Turso `Database` retain the engine's reader snapshots and single write
transaction coordination. Close the connections before calling `io.close()`
to release ownership. The CLI uses one connection and executes lines sequentially.
The adapter uses blocking HTTP and serializes block I/O within each instance.

### Verify automatic following

```bash
env -u TIDBFS_ENDPOINT -u TIDBFS_TOKEN \
  python3 follow.py --fs-id "$FS_ID"
```

The test keeps the same reader processes alive across commits, checkpoint/WAL
reset, schema changes, a reader network outage, expired-writer takeover, and a
clean handoff. It checks pinned transactions, shared cache reuse, and final
integrity. CLI authentication creates temporary read-only scoped tokens for the
readers. Results go to `output/follow.json`; test data stays under
`/demos/turso-follow/`. See the [recorded following check](results/follow.md).

### Verify failure and recovery

```bash
env -u TIDBFS_ENDPOINT -u TIDBFS_TOKEN \
  python3 handoff.py --fs-id "$FS_ID"
```

This runs two live scenarios against TiDB Filesystem: cutting the old writer's
transport through a disposable local proxy, and pausing/resuming the old process
with `SIGSTOP`/`SIGCONT`. In each case it checks live-owner exclusion, idle
heartbeats, two independent fixed snapshot readers (`--snapshot`), takeover, WAL recovery, rejection
of the restored writer's COMMIT, the successor's continued ownership, and final
rows and integrity. It uses 12-second TTLs and 2-second heartbeats for the test;
normal program defaults remain as shown above. Results go to `output/handoff.json`.
It creates disposable data under `/demos/turso-handoff/` and uses the same CLI
authentication as the smoke test. See [recorded handoff checks](results/handoff.md).

## Limits

- One writer per remote database directory, with independent read-only snapshots.
- 8 MiB per logical file, 8 logical files, and a 48 KiB manifest.
- HTTP I/O blocks the calling thread. Hosted endpoints require HTTPS.
- The server must support batch writes, and objects must fit its inline-upload limit.
- No block garbage collection, synchronous cross-client coherence, or workspace-wide transactions.
- WAL mode is covered; experimental Turso modes are outside this prototype.

## Development

```bash
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
```

The engine is pinned to Turso **0.8.3**. Python scripts use only the standard
library. They build release binaries when needed; use `TURSO_FS_BINARY` or
`TURSO_FS_BENCH_BINARY` to reuse a specific build, and `CARGO_TARGET_DIR` to
choose a build directory. With a custom target directory, adjust the executable
paths in the walkthrough accordingly.

An optional GitHub Actions template is in `ci/github-actions.yml`.
