#!/usr/bin/env python3
"""
monitor.py — Encode-progress watcher.

Two modes (auto-detected via environment):
──────────────────────────────────────────
GitHub Actions mode  (env: GH_TOKEN, RUN_ID, REPO, CHUNKS):
    Polls the GitHub REST API every 30 s and prints a per-chunk status table.
    Mirrors the inline Python script from the original monitor job.

Local mode  (no GH_TOKEN set):
    Watches /tmp/.prog_<stem>.txt files written by encode.py and prints a
    live progress table until all encoded-parts/*.mkv files exist.

CLI usage:
    python3 monitor.py                              # auto-detect mode
    python3 monitor.py --mode github               # force GitHub API mode
    python3 monitor.py --mode local --chunks 10    # force local mode
    python3 monitor.py --mode local --watch-dir encoded-parts
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import R, B, GR, RD, CY, YL, DIM, fmt_duration, progress_bar

PROG_DIR     = Path("/tmp")
POLL_GITHUB  = 30   # seconds between GitHub API polls
POLL_LOCAL   = 5    # seconds between local progress file polls


# ─── GitHub API mode ─────────────────────────────────────────────────────────
def monitor_github(
    gh_token: str,
    run_id: str,
    repo: str,
    chunks: list[str],
) -> None:
    """
    Poll GitHub REST API every POLL_GITHUB seconds.
    Prints a per-chunk status table to stdout until all encode-parts jobs finish.
    """
    API  = "https://api.github.com"
    HDRS = {
        "Authorization":        f"Bearer {gh_token}",
        "Accept":               "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    W     = 60
    total = len(chunks)
    parts = [c.replace(".mkv", "") for c in chunks]
    start = time.time()

    CONCLUSION_ICON = {
        "success":   "✅",
        "failure":   "❌",
        "cancelled": "🚫",
        "skipped":   "⏭️",
    }

    def gh_get(url: str) -> dict | None:
        req = urllib.request.Request(url, headers=HDRS)
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.load(r)
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            print(f"[monitor] HTTP error — {e}", flush=True)
            return None

    def get_encode_jobs() -> dict[str, dict]:
        """Return encode-parts matrix jobs keyed by part_id."""
        jobs: dict[str, dict] = {}
        url  = f"{API}/repos/{repo}/actions/runs/{run_id}/jobs?per_page=100"
        data = gh_get(url)
        if not data:
            return jobs
        for j in data.get("jobs", []):
            name = j.get("name", "")
            if name.startswith("encode-parts (") and name.endswith(")"):
                part_file = name[len("encode-parts ("):-1]
                part_id   = part_file.replace(".mkv", "")
                jobs[part_id] = j
        return jobs

    def job_duration(j: dict) -> int:
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        started = j.get("started_at")
        if not started:
            return 0
        start_dt = datetime.datetime.strptime(started, fmt)
        completed = j.get("completed_at")
        end_dt = (datetime.datetime.strptime(completed, fmt)
                  if completed else datetime.datetime.utcnow())
        return max(0, int((end_dt - start_dt).total_seconds()))

    while True:
        jobs    = get_encode_jobs()
        elapsed = time.time() - start

        done   = sum(1 for j in jobs.values() if j.get("status") == "completed")
        active = [j for j in jobs.values() if j.get("status") == "in_progress"]

        pct_total = ((done * 100 + len(active) * 50) / total) if total else 0

        print("━" * W)
        print(f"  📊  MATRIX ENCODE PROGRESS   [{fmt_duration(elapsed)} elapsed]")
        print("━" * W)
        print(f"  Chunks  : {done}/{total} done"
              + (f"  ({len(active)} encoding)" if active else ""))
        print(f"  Overall : {progress_bar(pct_total)}  {pct_total:.0f}%")
        print("━" * W)

        for i, part in enumerate(parts):
            j   = jobs.get(part)
            idx = f"[{i:02d}]"
            if j is None:
                print(f"  {idx} ⏳  waiting for runner…")
            else:
                status     = j.get("status", "")
                conclusion = j.get("conclusion", "")
                dur        = job_duration(j)
                if status == "completed":
                    icon = CONCLUSION_ICON.get(conclusion, "❓")
                    print(f"  {idx} {icon}  {fmt_duration(dur)}")
                elif status == "in_progress":
                    print(f"  {idx} 🔄  {fmt_duration(dur)} elapsed")
                else:
                    print(f"  {idx} ⏳  queued")

        print("━" * W, flush=True)

        if done == total and total > 0:
            print(f"\n{GR}✅  All {total} chunks finished.{R}", flush=True)
            break

        time.sleep(POLL_GITHUB)


# ─── Local progress file mode ─────────────────────────────────────────────────
def _read_ffmpeg_progress(prog_file: Path) -> dict[str, str]:
    """Parse an ffmpeg -progress text file into a key/value dict."""
    if not prog_file.exists():
        return {}
    try:
        lines = prog_file.read_text(errors="ignore").strip().splitlines()
        data: dict[str, str] = {}
        for line in lines:
            if "=" in line:
                k, _, v = line.partition("=")
                data[k.strip()] = v.strip()
        return data
    except OSError:
        return {}


def monitor_local(
    chunks: list[Path],
    watch_dir: Path = Path("encoded-parts"),
    poll_interval: float = POLL_LOCAL,
) -> None:
    """
    Watch /tmp/.prog_<stem>.txt progress files and encoded-parts/ output files.
    Blocks until all chunks have a matching *-encoded.mkv in watch_dir.
    """
    total = len(chunks)
    start = time.time()
    W     = 56

    # Print blank lines so the cursor-up trick works on first iteration
    print("\n" * (total + 6), end="", flush=True)

    while True:
        done   = 0
        lines  = []

        for i, chunk in enumerate(chunks):
            encoded   = watch_dir / chunk.name.replace(".mkv", "-encoded.mkv")
            prog_file = PROG_DIR / f".prog_{chunk.stem}.txt"

            if encoded.exists():
                done += 1
                size_mb = encoded.stat().st_size / 1_048_576
                lines.append(f"  [{i:02d}] {GR}✅  done — {size_mb:.1f} MB{R}")
            else:
                d     = _read_ffmpeg_progress(prog_file)
                frame = d.get("frame", "?")
                speed = d.get("speed", "?")
                if d:
                    lines.append(f"  [{i:02d}] 🔄  frame={frame}  speed={speed}  {chunk.name}")
                else:
                    lines.append(f"  [{i:02d}] ⏳  waiting…   {chunk.name}")

        elapsed  = time.time() - start
        pct_done = (done / total * 100) if total else 0

        # Overwrite previous table
        print(f"\033[{total + 6}A", end="")
        print("━" * W)
        print(f"  📊  LOCAL ENCODE PROGRESS   [{fmt_duration(elapsed)} elapsed]")
        print("━" * W)
        print(f"  Chunks  : {done}/{total} done")
        print(f"  Overall : {progress_bar(pct_done)}  {pct_done:.0f}%")
        print("━" * W)
        for l in lines:
            print(l)
        sys.stdout.flush()

        if done == total and total > 0:
            print(f"\n{GR}✅  All {total} chunks encoded.{R}", flush=True)
            break

        time.sleep(poll_interval)


# ─── Auto-detection + CLI ─────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Monitor encode progress")
    p.add_argument("--mode", choices=["github", "local", "auto"], default="auto")
    p.add_argument("--chunks",    type=int, default=None,
                   help="Number of chunks (local mode, if no part_*.mkv found yet)")
    p.add_argument("--watch-dir", default="encoded-parts",
                   help="Directory where encoded-*.mkv files appear (local mode)")
    p.add_argument("--interval",  type=float, default=POLL_LOCAL,
                   help=f"Poll interval in seconds (local mode, default {POLL_LOCAL})")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    mode = args.mode

    # Auto-detect based on environment
    if mode == "auto":
        mode = "github" if os.environ.get("GH_TOKEN") else "local"

    if mode == "github":
        gh_token = os.environ.get("GH_TOKEN", "")
        run_id   = os.environ.get("RUN_ID",   "")
        repo     = os.environ.get("REPO",     "")
        chunks_raw = os.environ.get("CHUNKS", "[]")

        if not all([gh_token, run_id, repo]):
            print(f"{RD}GitHub mode requires env: GH_TOKEN, RUN_ID, REPO, CHUNKS{R}")
            sys.exit(1)

        try:
            chunks_list = json.loads(chunks_raw)
        except json.JSONDecodeError:
            print(f"{RD}CHUNKS must be a JSON array of filenames.{R}")
            sys.exit(1)

        monitor_github(gh_token, run_id, repo, chunks_list)

    else:  # local
        watch_dir = Path(args.watch_dir)
        chunks    = sorted(Path(".").glob("part_*.mkv"))
        if not chunks and args.chunks:
            # Chunks not split yet — build synthetic list for display
            chunks = [Path(f"part_{i:02d}.mkv") for i in range(args.chunks)]

        if not chunks:
            print(f"{RD}No part_*.mkv files found.  "
                  f"Run split.py first or pass --chunks N.{R}")
            sys.exit(1)

        monitor_local(chunks, watch_dir, poll_interval=args.interval)
