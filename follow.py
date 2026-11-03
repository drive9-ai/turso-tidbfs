#!/usr/bin/env python3
"""Live automatic snapshot following through commits, WAL resets and writer takeover."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid

from handoff import PartitionProxy
from smoke import HERE, Session, access, ti


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fs-id")
    parser.add_argument("--profile", default="default")
    parser.add_argument("--output", type=Path, default=HERE / "output/follow.json")
    args = parser.parse_args()
    binary = Path(os.environ.get("TURSO_FS_BINARY", Path(os.environ.get("CARGO_TARGET_DIR", HERE / "target")) / "release/turso-fs")).resolve()
    if not os.environ.get("TURSO_FS_BINARY"):
        subprocess.run(["cargo", "build", "--release", "--locked", "--manifest-path", str(HERE / "Cargo.toml"), "--bin", "turso-fs"], check=True)
    root = "/demos/turso-follow/" + uuid.uuid4().hex
    env = {**access(args, root), "TIDBFS_LEASE_TTL_MS": "12000", "TIDBFS_HEARTBEAT_MS": "2000",
           "TIDBFS_LOCK_TIMEOUT_MS": "0", "TIDBFS_REFRESH_MS": "250", "TIDBFS_CACHE_BLOCKS": "256"}
    read_env = dict(env)
    read_only_token = False
    if not (os.environ.get("TIDBFS_ENDPOINT") and os.environ.get("TIDBFS_TOKEN")):
        fs_id = args.fs_id
        if not fs_id:
            fs_id = next(f["file_system_id"] for f in ti(["fs", "list-file-systems"], args.profile)["file_systems"] if f.get("has_local_token") and f.get("status") == "active")
        read_env["TIDBFS_TOKEN"] = ti(["fs", "generate-file-system-scoped-token", "--file-system-id", fs_id,
                                       "--subject", "turso-follow-readers", "--ttl", "30m", "--allow", root + ":read,list"], args.profile)["fs_token"]
        read_only_token = True
    proxy = PartitionProxy(env["TIDBFS_ENDPOINT"])
    sessions = []
    paused = None
    report = {"recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "root": root,
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "refresh_ms": 250,
              "ttl_ms": 12000, "read_only_scoped_tokens": read_only_token, "checks": [], "visibility_ms": {}}

    def progress(message):
        print(json.dumps({"progress": message}), flush=True)

    def start(readonly=False, child_env=None):
        s = Session(binary, root, child_env or env, readonly=readonly)
        sessions.append(s)
        return s

    def wait_count(session, expected, label):
        began = time.monotonic()
        deadline = began + 90
        while True:
            rows = session.sql("?SELECT count(*) FROM items")
            if rows == [[str(expected)]]:
                report["visibility_ms"][label] = round((time.monotonic() - began) * 1000, 3)
                return session.last_response
            assert time.monotonic() < deadline, (label, rows, session.last_response)
            time.sleep(0.1)

    try:
        writer = start()
        writer.sql("CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT);")
        values = ",".join("(%d,'%s')" % (i, "x" * 1024) for i in range(128))
        writer.sql("BEGIN; INSERT INTO items VALUES " + values + "; COMMIT;")
        writer.sql("PRAGMA wal_checkpoint(TRUNCATE);")
        pinned = start(readonly=True, child_env=read_env)
        live = start(readonly=True, child_env={**read_env, "TIDBFS_ENDPOINT": proxy.endpoint})
        report["reader_pids"] = [pinned.process.pid, live.process.pid]
        # Warm database blocks, not only the schema/header, before switching engines.
        for reader in (pinned, live):
            assert reader.sql("?SELECT count(*),sum(length(payload)) FROM items") == [["128", "131072"]]
        pinned.sql("BEGIN;")
        assert pinned.sql("?SELECT count(*) FROM items") == [["128"]]
        pinned_revision = pinned.last_response["snapshot_revision"]
        writer.sql("INSERT INTO items VALUES(128,'commit');")
        wait_count(live, 129, "commit")
        assert pinned.sql("?SELECT count(*) FROM items") == [["128"]]
        assert pinned.last_response["snapshot_revision"] == pinned_revision
        report["checks"].append("same live reader follows commit; explicit transaction retains old snapshot")
        progress("commit following and transaction pinning verified")

        writer.sql("PRAGMA wal_checkpoint(TRUNCATE);")
        writer.sql("INSERT INTO items VALUES(129,'reset');")
        wait_count(live, 130, "wal_reset")
        writer.sql("BEGIN; ALTER TABLE items ADD COLUMN tag TEXT; INSERT INTO items VALUES(130,'schema','ddl'); COMMIT;")
        wait_count(live, 131, "schema_change")
        assert live.sql("?SELECT tag FROM items WHERE id=130") == [["ddl"]]
        assert len(pinned.sql("?PRAGMA table_info(items)")) == 2
        assert len(live.sql("?PRAGMA table_info(items)")) == 3
        report["checks"].append("checkpoint/truncate, new WAL generation and schema changes followed; old transaction retains old schema")
        progress("checkpoint, WAL reset and schema following verified")

        proxy.cut.set()
        assert proxy.rejected.wait(30), "reader did not poll through cut transport"
        writer.sql("INSERT INTO items VALUES(131,'offline','retry');")
        deadline = time.monotonic() + 30
        while True:
            assert live.sql("?SELECT count(*) FROM items") == [["131"]]
            if live.last_response["reader"]["last_refresh_error"]:
                break
            assert time.monotonic() < deadline
            time.sleep(0.1)
        proxy.cut.clear()
        wait_count(live, 132, "reader_reconnect")
        assert live.last_response["reader"]["last_refresh_error"] is None
        report["checks"].append("reader transport outage retains last usable snapshot and reports lag/error; same process catches up after reconnect")
        progress("reader disconnection and automatic recovery verified")

        writer.sql("BEGIN; INSERT INTO items VALUES(999,'abandoned','must disappear');")
        writer.process.send_signal(signal.SIGSTOP)
        paused = writer
        progress("writer paused; waiting for lease takeover")
        began = time.monotonic()
        successor = start(child_env={**env, "TIDBFS_LOCK_TIMEOUT_MS": "60000"})
        report["takeover_open_ms"] = round((time.monotonic() - began) * 1000, 3)
        assert successor.sql("?SELECT count(*) FROM items") == [["132"]]
        successor.sql("INSERT INTO items VALUES(132,'successor','takeover');")
        wait_count(live, 133, "writer_takeover")
        assert pinned.sql("?SELECT count(*) FROM items") == [["128"]]
        assert pinned.last_response["reader"]["applied_revision"] > pinned_revision
        writer.process.send_signal(signal.SIGCONT)
        paused = None
        try:
            writer.sql("COMMIT;")
        except RuntimeError:
            assert writer.process.wait(timeout=15) != 0
            assert any("failed or expired" in line for line in writer.errors), writer.errors
        else:
            raise AssertionError("old writer committed after takeover")
        writer.stop(kill=True)
        successor.sql("INSERT INTO items VALUES(133,'successor','still owner');")
        wait_count(live, 134, "successor_commit")
        pinned.sql("COMMIT;")
        wait_count(pinned, 134, "transaction_end")
        assert pinned.last_response["snapshot_revision"] > pinned_revision
        report["checks"].append("same reader processes follow lease takeover; old transaction survives takeover and upgrades after COMMIT; restored writer rejected")
        progress("writer takeover and transaction-boundary switching verified")

        successor.sql("PRAGMA wal_checkpoint(TRUNCATE);")
        successor.sql("INSERT INTO items VALUES(134,'reset again','new generation');")
        for reader, label in ((live, "second_wal_reset"), (pinned, "second_reader_reset")):
            wait_count(reader, 135, label)
        successor.stop()
        clean = start()
        clean.sql("INSERT INTO items VALUES(135,'clean handoff','final');")
        for reader, label in ((live, "clean_handoff"), (pinned, "second_reader_handoff")):
            wait_count(reader, 136, label)
            assert reader.sql("?SELECT count(*),sum(id) FROM items") == [["136", "9180"]]
            assert reader.sql("?SELECT count(*) FROM items WHERE id=999") == [["0"]]
            assert reader.sql("?PRAGMA integrity_check") == [["ok"]]
            stats = reader.last_response["reader"]["cache"]
            assert stats["hits"] > 0 and stats["bytes"] <= 256 * 32768, stats
        report["reader_status"] = [pinned.last_response, live.last_response]
        report["final_count"] = 136
        report["reader_processes_unchanged"] = [pinned.process.pid, live.process.pid] == report["reader_pids"]
        assert report["reader_processes_unchanged"]
        report["integrity_check"] = "ok"
        report["checks"].append("same PIDs follow repeated WAL reset and clean writer handoff; shared cache hits and bounded size verified; final integrity ok")
        for session in sessions:
            if session.process.poll() is None:
                session.stop()
        report["local_database_files"] = 0
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
    finally:
        if paused and paused.process.poll() is None:
            paused.process.send_signal(signal.SIGCONT)
        for session in sessions:
            session.stop(kill=True)
        proxy.close()


if __name__ == "__main__":
    main()
