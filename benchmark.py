#!/usr/bin/env python3
"""Compare the same release Turso engine over native FS and local filesystem I/O."""
import argparse
from datetime import datetime, timezone
import json
import hashlib
import os
from pathlib import Path
import platform
import statistics
import subprocess
import tempfile
import uuid
from smoke import access

HERE = Path(__file__).resolve().parent

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fs-id")
    parser.add_argument("--profile", default="default")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--phases", nargs="+", choices=["seed", "read", "transactions"], default=["seed", "read", "transactions"])
    parser.add_argument("--output", type=Path, default=HERE / "output/benchmark.json")
    args = parser.parse_args()
    if not 1 <= args.trials <= 10:
        parser.error("--trials must be between 1 and 10")
    target = Path(os.environ.get("CARGO_TARGET_DIR", HERE / "target")).resolve()
    binary = Path(os.environ.get("TURSO_FS_BENCH_BINARY", target / "release/bench"))
    if not os.environ.get("TURSO_FS_BENCH_BINARY"):
        subprocess.run(["cargo", "build", "--release", "--locked", "--bin", "bench"], cwd=HERE, check=True)
    root = "/demos/turso-bench/" + uuid.uuid4().hex
    env = access(args, root)
    # Reports record only host/machine information, never credentials or signed URLs.
    report = {"recorded_at": datetime.now(timezone.utc).isoformat(), "machine": platform.platform(),
              "turso_core": "0.8.3", "build": "release", "trials": args.trials,
              "fs_host": __import__("urllib.parse", fromlist=["urlparse"]).urlparse(env["DRIVE9_BASE"]).hostname,
              "remote_root": root, "runs": [],
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "notes": ["Same Turso engine and SQL; synchronous=FULL, WAL, auto-checkpoint disabled.",
                        "Each phase runs in a separate process; adapter/pager caches start empty in read phase.",
                        "OS disk caches are not dropped; local results include normal OS caching.",
                        "FS commit includes remote durability; local commit covers this machine's filesystem only.",
                        "HTTP preparation and full integrity checks are outside per-operation timing.",
                        "Single-row and batch commit samples are sequential, not concurrent."]}
    def run(backend, path, phase, trial):
        with tempfile.TemporaryDirectory(prefix="turso-fs-bench-process-") as sandbox:
            result = subprocess.run([str(binary), backend, path, phase], env=env, cwd=sandbox,
                                    capture_output=True, text=True, timeout=240)
            if result.returncode:
                raise RuntimeError("benchmark failed: " + result.stderr[-2000:])
            if backend == "fs" and list(Path(sandbox).iterdir()):
                raise RuntimeError("FS benchmark unexpectedly created local database files")
            data = json.loads(result.stdout)
            data["backend"] = backend
            data["trial"] = trial
            report["runs"].append(data)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.with_suffix(".partial.json").write_text(json.dumps(report, indent=2) + "\n")
            print(f"trial {trial}: {backend} {phase} complete", flush=True)
    with tempfile.TemporaryDirectory(prefix="turso-fs-local-bench-") as local:
        report["local_storage"] = "temporary directory on host filesystem (not tmpfs on macOS)"
        for trial in range(1, args.trials + 1):
            paths = {"local": str(Path(local) / f"trial{trial}.db"), "fs": root + f"/trial{trial}"}
            # Alternate execution order to reduce systematic order effects.
            backends = ["local", "fs"]
            if trial % 2 == 0:
                backends.reverse()
            for backend in backends:
                for phase in args.phases:
                    run(backend, paths[backend], phase, trial)
    groups = {}
    for run_result in report["runs"]:
        for phase in run_result["phases"]:
            if "ms_per_operation" in phase:
                groups.setdefault((run_result["backend"], phase["name"]), []).append(phase["ms_per_operation"])
    report["summary"] = []
    for name in dict.fromkeys(key[1] for key in groups):
        local_ms = statistics.median(groups[("local", name)])
        remote_ms = statistics.median(groups[("fs", name)])
        item = {"name": name, "local_ms": local_ms, "fs_ms": remote_ms,
                "fs_over_local": remote_ms / local_ms}
        report["summary"].append(item)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["summary"], indent=2))

if __name__ == "__main__":
    main()
