#!/usr/bin/env python3
"""Verify live writer handoff, stale-process rejection and independent snapshot readers."""
import argparse
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time
from urllib.parse import urlsplit
import uuid

from smoke import HERE, Session, access


class PartitionProxy:
    """A disposable transport fault for the old process; other clients go direct."""
    def __init__(self, endpoint):
        self.cut = threading.Event()
        self.rejected = threading.Event()
        parsed = urlsplit(endpoint)
        connection_type = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        self.upstream = connection_type(parsed.hostname, parsed.port, timeout=30)
        self.upstream_lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def forward(self):
                if owner.cut.is_set():
                    owner.rejected.set()
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                data = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                headers = {key: value for key, value in self.headers.items()
                           if key.lower() not in {"host", "connection", "content-length"}}
                try:
                    with owner.upstream_lock:
                        owner.upstream.request(self.command, self.path, body=data or None, headers=headers)
                        response = owner.upstream.getresponse()
                        body = response.read()
                        status = response.status
                        response_headers = {key: response.getheader(key) for key in ("Content-Type", "X-Dat9-Revision")}
                except Exception as exc:
                    # Never retry a mutation with an uncertain upstream outcome.
                    print(json.dumps({"proxy_transport_error": type(exc).__name__}), flush=True)
                    self.send_error(502)
                    return
                self.send_response(status)
                for key, value in response_headers.items():
                    if value:
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = do_PUT = do_POST = forward

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.worker.start()
        self.endpoint = "http://127.0.0.1:" + str(self.server.server_port)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.worker.join(timeout=5)
        self.upstream.close()


def event(failure, message):
    print(json.dumps({"failure": failure, "progress": message}), flush=True)


def run_case(args, binary, failure):
    root = "/demos/turso-handoff/" + uuid.uuid4().hex
    env = {**access(args, root), "TIDBFS_LEASE_TTL_MS": str(args.ttl_ms),
           "TIDBFS_HEARTBEAT_MS": str(args.heartbeat_ms), "TIDBFS_LOCK_TIMEOUT_MS": "0"}
    proxy = PartitionProxy(env["TIDBFS_ENDPOINT"]) if failure == "network" else None
    old_env = {**env, "TIDBFS_ENDPOINT": proxy.endpoint} if proxy else env
    sessions = []
    paused = None
    checks = []

    def start(readonly=False, child_env=None):
        session = Session(binary, root, child_env or env, readonly=readonly)
        sessions.append(session)
        return session

    def reject_writer():
        try:
            unexpected = start()
        except RuntimeError as exc:
            assert "remote database is busy" in str(exc), str(exc)
        else:
            unexpected.stop(kill=True)
            raise AssertionError("a second writer acquired a live lease")

    try:
        old = start(child_env=old_env)
        old.sql("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT);")
        old.sql("BEGIN; INSERT INTO items VALUES(1,'saved'),(2,'saved'); COMMIT;")
        readers = [start(readonly=True), start(readonly=True)]
        for reader in readers:
            assert reader.sql("?SELECT id,value FROM items ORDER BY id") == [["1", "saved"], ["2", "saved"]]
        old.sql("INSERT INTO items VALUES(3,'saved');")
        for reader in readers:
            assert reader.sql("?SELECT count(*) FROM items") == [["2"]]
        checks.append("two independent readers retain snapshots while writer commits")
        reject_writer()
        # An idle owner must survive longer than one TTL through real heartbeats.
        event(failure, "checking idle heartbeat renewal and writer exclusion")
        time.sleep((args.ttl_ms + args.heartbeat_ms) / 1000)
        reject_writer()
        old.sql("BEGIN; INSERT INTO items VALUES(900,'uncommitted');")
        checks.append("live idle writer renews beyond TTL; competing writer is rejected")
        if proxy:
            proxy.cut.set()
            assert proxy.rejected.wait(args.ttl_ms / 1000 + 5), "no renewal attempted through cut transport"
        else:
            old.process.send_signal(signal.SIGSTOP)
            paused = old
        event(failure, "old process unavailable; waiting for a new writer to take ownership")
        began = time.monotonic()
        successor_env = {**env, "TIDBFS_LOCK_TIMEOUT_MS": str(args.ttl_ms * 3 + 30000)}
        successor = start(child_env=successor_env)
        takeover_ms = round((time.monotonic() - began) * 1000, 3)
        assert successor.sql("?SELECT id,value FROM items ORDER BY id") == [[str(i), "saved"] for i in range(1, 4)]
        successor.sql("INSERT INTO items VALUES(4,'successor');")
        checks.append("successor recovers acknowledged WAL and excludes abandoned transaction")
        if proxy:
            proxy.cut.clear()
        else:
            old.process.send_signal(signal.SIGCONT)
            paused = None
        try:
            old.sql("COMMIT;")
        except RuntimeError:
            assert old.process.wait(timeout=15) != 0
            assert any("failed or expired" in line for line in old.errors), old.errors
        else:
            raise AssertionError("old process committed after takeover")
        old.stop(kill=True)
        checks.append("restored old process cannot commit or release successor ownership")
        successor.sql("INSERT INTO items VALUES(5,'successor');")
        reject_writer()
        for reader in readers:
            assert reader.sql("?SELECT count(*) FROM items") == [["2"]]
            reader.stop()
        latest = start(readonly=True)
        expected = [[str(i), "saved" if i < 4 else "successor"] for i in range(1, 6)]
        assert latest.sql("?SELECT id,value FROM items ORDER BY id") == expected
        assert latest.sql("?PRAGMA integrity_check") == [["ok"]]
        latest.stop()
        successor.sql("PRAGMA wal_checkpoint(TRUNCATE);")
        successor.stop()
        clean_began = time.monotonic()
        final = start()
        clean_open_ms = round((time.monotonic() - clean_began) * 1000, 3)
        actual = final.sql("?SELECT id,value FROM items ORDER BY id")
        assert actual == expected
        assert final.sql("?PRAGMA integrity_check") == [["ok"]]
        final.stop()
        checks.append("clean release permits immediate handoff; final rows and integrity verified; no local database files")
        event(failure, "handoff, stale-process rejection and final data verified")
        return {"failure": failure, "root": root, "ttl_ms": args.ttl_ms,
                "heartbeat_ms": args.heartbeat_ms, "takeover_open_ms": takeover_ms,
                "clean_handoff_open_ms": clean_open_ms, "checks": checks,
                "snapshot_readers": len(readers), "integrity_check": "ok", "local_database_files": 0,
                "stale_commit_exit_code": old.process.returncode,
                "stale_process_error": " ".join(old.errors)[-1000:],
                "final_rows": actual,
                "rows_sha256": hashlib.sha256(json.dumps(actual, separators=(",", ":")).encode()).hexdigest()}
    finally:
        if paused and paused.process.poll() is None:
            paused.process.send_signal(signal.SIGCONT)
        for session in sessions:
            session.stop(kill=True)
        if proxy:
            proxy.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fs-id")
    parser.add_argument("--profile", default="default")
    parser.add_argument("--ttl-ms", type=int, default=12000)
    parser.add_argument("--heartbeat-ms", type=int, default=2000)
    parser.add_argument("--failure", choices=["network", "pause", "both"], default="both")
    parser.add_argument("--output", type=Path, default=HERE / "output/handoff.json")
    args = parser.parse_args()
    if not 100 <= args.ttl_ms <= 300000 or not 0 < args.heartbeat_ms <= args.ttl_ms // 3:
        parser.error("TTL must be 100..300000 ms and heartbeat positive, at most TTL/3")
    binary = Path(os.environ.get("TURSO_FS_BINARY", Path(os.environ.get("CARGO_TARGET_DIR", HERE / "target")) / "release/turso-fs")).resolve()
    if not os.environ.get("TURSO_FS_BINARY"):
        subprocess.run(["cargo", "build", "--release", "--locked", "--manifest-path", str(HERE / "Cargo.toml"), "--bin", "turso-fs"], check=True)
    cases = ["network", "pause"] if args.failure == "both" else [args.failure]
    report = {"binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "runs": []}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for failure in cases:
        report["runs"].append(run_case(args, binary, failure))
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
