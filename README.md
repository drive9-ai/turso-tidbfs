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
use one active process per remote database directory.

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

export DRIVE9_BASE="$(ti fs check-file-system \
  --file-system-id "$FS_ID" \
  --query endpoint.base_url --output text)"

export DRIVE9_API_KEY="$(ti fs generate-file-system-scoped-token \
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
| `DRIVE9_BASE` | The HTTPS endpoint for that filesystem's region. |
| `DRIVE9_API_KEY` | The FS token used by this program, not a Cloud API key. |

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
build the executable there and pass it `DRIVE9_BASE`, `DRIVE9_API_KEY`, and
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
env -u DRIVE9_BASE -u DRIVE9_API_KEY \
  python3 smoke.py --fs-id "$FS_ID"
```

It commits rows, kills the process with an uncommitted transaction open, and
reopens the database in a fresh process. It also checks integrity, checkpoint
and reopen, stale-writer rejection, and absence of local database files.
Results are written to `output/results.json`.

## Compare with local disk

```bash
env -u DRIVE9_BASE -u DRIVE9_API_KEY \
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

If you set `DRIVE9_BASE` and `DRIVE9_API_KEY` explicitly for these scripts, that
token must allow their `/demos/` paths. A token limited to the interactive
example's directory cannot access those test directories.

## Troubleshooting

| What you see | What to check |
| --- | --- |
| `ti: command not found` | Add `$HOME/.ti/bin` to `PATH`. |
| Linker or C compiler missing | Install Xcode Command Line Tools on macOS, or `build-essential` on Ubuntu/Debian. |
| Missing environment variable | Set `DRIVE9_BASE` and `DRIVE9_API_KEY` in the terminal running `turso-fs`. |
| No local owner token | Use the existing-filesystem instructions in step 3 to generate or import one. |
| HTTP 401 | Issue a new FS token; the current one may have expired or been revoked. |
| HTTP 403 | Check that the token permits `read,list,write` for `FS_ROOT`. For scripts, use their CLI-token mode shown above. |
| HTTP 409 or failed session | Stop the stale process and reopen the database. Check committed state before retrying a failed application operation. |

## How storage works

Reads fetch **32 KiB blocks on demand**, with a bounded cache. Writes upload
immutable blocks and publish a conditional manifest after the data is durable.
A small WAL tail can be stored directly in that manifest. The program uses
`synchronous=FULL`; a commit waits for remote confirmation.

See [storage design](docs/storage.md) for the I/O interfaces, cache, commit
protocol, and failure behavior.

## Limits

- One active process per remote database directory; concurrent stale writers fail.
- 8 MiB per logical file, 8 logical files, and a 48 KiB manifest.
- HTTP I/O blocks the calling thread. Hosted endpoints require HTTPS.
- The server must support batch writes, and objects must fit its inline-upload limit.
- No block garbage collection, live cross-client coherence, or workspace-wide transactions.
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
