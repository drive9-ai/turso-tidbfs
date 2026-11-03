#!/usr/bin/env python3
"""Manual native I/O smoke against real FS; credentials stay in child environment."""
import argparse
import json
import os
from pathlib import Path
import queue
import subprocess
import tempfile
import threading
import time
import uuid

HERE = Path(__file__).resolve().parent


def ti(args, profile):
    result = subprocess.run(["ti", "--profile", profile, "--output", "json", *args],
                            capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise RuntimeError("Ti CLI failed; check login and filesystem access")
    return json.loads(result.stdout)


def access(args, root):
    if os.environ.get("TIDBFS_ENDPOINT") and os.environ.get("TIDBFS_TOKEN"):
        return dict(os.environ)
    fs_id = args.fs_id
    if not fs_id:
        candidates = [x for x in ti(["fs", "list-file-systems"], args.profile)["file_systems"]
                      if x.get("has_local_token") and x.get("status") == "active"]
        if len(candidates) != 1:
            raise RuntimeError("select a filesystem using --fs-id")
        fs_id = candidates[0]["file_system_id"]
    command = ["fs", "generate-file-system-scoped-token", "--file-system-id", fs_id,
               "--subject", "turso-native-fs-demo", "--ttl", "30m",
               "--allow", root + ":read,list,write"]
    plan = ti(command + ["--dry-run"], args.profile)
    endpoint = next(x["message"] for x in plan["checks"]
                    if x["name"] == "endpoint_selection" and x["status"] == "passed")
    token = ti(command, args.profile)["fs_token"]
    return {**os.environ, "TIDBFS_ENDPOINT": endpoint, "TIDBFS_TOKEN": token}


class Session:
    def __init__(self, binary, root, env):
        self.folder = tempfile.TemporaryDirectory(prefix="turso-fs-sandbox-")
        self.process = subprocess.Popen([str(binary), root], env=env, cwd=self.folder.name,
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, text=True, bufsize=1)
        self.lines = queue.Queue()
        self.errors = []
        self.output_reader = threading.Thread(target=self._read, args=(self.process.stdout, self.lines), daemon=True)
        self.output_reader.start()
        self.error_reader = threading.Thread(target=self._read, args=(self.process.stderr, self.errors), daemon=True)
        self.error_reader.start()
        try:
            ready = self.receive()
            assert ready.get("ready"), ready
        except BaseException:
            self.stop(kill=True)
            raise

    @staticmethod
    def _read(pipe, target):
        for line in pipe:
            if isinstance(target, list):
                target.append(line.rstrip())
            else:
                target.put(line)
        if not isinstance(target, list):
            target.put(None)

    def receive(self):
        try:
            line = self.lines.get(timeout=120)
        except queue.Empty:
            raise RuntimeError("native session timed out") from None
        if line is None:
            self.process.wait(timeout=10)
            self.error_reader.join(timeout=5)
            raise RuntimeError("native session exited: " + " ".join(self.errors)[-1500:])
        return json.loads(line)

    def sql(self, sql):
        self.process.stdin.write(sql + "\n")
        self.process.stdin.flush()
        response = self.receive()
        assert response.get("ok"), response
        return response["rows"]

    def stop(self, kill=False):
        if self.process.poll() is None:
            if kill:
                self.process.kill()
            else:
                self.process.stdin.close()
            self.process.wait(timeout=120)
        self.error_reader.join(timeout=5)
        assert not list(Path(self.folder.name).iterdir()), "unexpected local database files"
        self.folder.cleanup()
        if not kill:
            assert self.process.returncode == 0, self.errors
        return self.errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fs-id")
    parser.add_argument("--profile", default="default")
    args = parser.parse_args()
    target = Path(os.environ.get("CARGO_TARGET_DIR", HERE / "target")).resolve()
    binary = Path(os.environ.get("TURSO_FS_BINARY", target / "release/turso-fs"))
    if not os.environ.get("TURSO_FS_BINARY"):
        subprocess.run(["cargo", "build", "--release", "--locked", "--manifest-path", str(HERE / "Cargo.toml")], check=True)
    root = "/demos/turso/" + uuid.uuid4().hex
    env = access(args, root)
    report = {"root": root, "checks": [], "pids": [], "metrics": []}
    sessions = []
    started = time.monotonic()

    def start():
        session = Session(binary, root, env)
        sessions.append(session)
        report["pids"].append(session.process.pid)
        return session

    try:
        writer = start()
        writer.sql("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT);")
        values = ",".join("(%d,'%s')" % (i, "x" * 1024) for i in range(64))
        writer.sql("BEGIN; INSERT INTO items VALUES " + values + "; COMMIT;")
        assert writer.sql("?SELECT count(*),sum(id) FROM items") == [["64", "2016"]]
        writer.sql("BEGIN; INSERT INTO items VALUES(999,'uncommitted');")
        report["metrics"] += writer.stop(kill=True)
        report["checks"].append("committed transaction acknowledged, then SIGKILL with open transaction")

        recovered = start()
        assert recovered.sql("?SELECT count(*),sum(id) FROM items") == [["64", "2016"]]
        assert recovered.sql("?PRAGMA integrity_check") == [["ok"]]
        report["checks"].append("fresh process recovers WAL; uncommitted row absent; integrity_check=ok")
        recovered.sql("PRAGMA wal_checkpoint(TRUNCATE);")
        report["metrics"] += recovered.stop()

        first, stale = start(), start()
        first.sql("INSERT INTO items VALUES(64,'new');")
        try:
            stale.sql("INSERT INTO items VALUES(65,'must not overwrite');")
        except RuntimeError:
            assert stale.process.wait(timeout=15) != 0
            assert any("409" in line for line in stale.errors), stale.errors
        else:
            raise AssertionError("stale writer unexpectedly succeeded")
        stale.stop(kill=True)
        report["metrics"] += first.stop()
        report["checks"].append("stale process rejected by remote manifest CAS (HTTP 409)")

        final = start()
        assert final.sql("?SELECT count(*),sum(id) FROM items") == [["65", "2080"]]
        assert final.sql("?PRAGMA integrity_check") == [["ok"]]
        report["metrics"] += final.stop()
        report["checks"].append("checkpoint/reopen preserves winning commit; no local database files")
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        output = HERE / "output"
        output.mkdir(exist_ok=True)
        (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
    finally:
        for session in sessions:
            if session.process.poll() is None:
                session.stop(kill=True)


if __name__ == "__main__":
    main()
