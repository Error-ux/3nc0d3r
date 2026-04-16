#!/usr/bin/env python3
"""
split.py — Split source.mkv into N keyframe-aligned chunks.

Outputs : part_00.mkv, part_01.mkv, …  (in --out-dir)
          Prints a JSON array of filenames to stdout for GitHub Actions matrix.

CLI usage:
    python3 split.py source.mkv [--chunks 10] [--out-dir .]
    python3 split.py source.mkv --print-matrix   # also emit matrix_json to $GITHUB_OUTPUT
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import (R, B, GR, RD, CY, YL, DIM,
                   check_ffmpeg, get_duration, get_subtitle_maps,
                   fmt_size, fmt_duration, FFMPEG)

DEFAULT_CHUNKS = 10


# ─── Public API ──────────────────────────────────────────────────────────────
def split(
    source: Path,
    chunk_count: int = DEFAULT_CHUNKS,
    out_dir: Path = Path("."),
) -> list[Path]:
    """
    Split *source* into *chunk_count* keyframe-aligned MKV segments.
    Returns a sorted list of output chunk paths.
    """
    check_ffmpeg()
    source  = Path(source)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{B}━━━ Split ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{R}", flush=True)
    print(f"  Source  : {source}  ({source.stat().st_size / 1_048_576:.1f} MB)", flush=True)
    print(f"  Chunks  : {chunk_count}", flush=True)

    duration   = get_duration(source)
    chunk_secs = int(duration / chunk_count)
    print(f"  Duration: {fmt_duration(duration)}  →  ~{chunk_secs}s per chunk", flush=True)

    sub_maps = get_subtitle_maps(source)
    print(f"  Subtitle maps : {sub_maps if sub_maps else 'none'}", flush=True)

    out_pattern = str(out_dir / "part_%02d.mkv")

    cmd = [
        FFMPEG,
        "-analyzeduration", "100M", "-probesize", "100M",
        "-i", str(source),
        "-map", "0:v", "-map", "0:a", *sub_maps,
        "-c:v", "copy",
        "-c:a", "copy",
        "-c:s", "copy",
        "-segment_time", str(chunk_secs),
        "-segment_time_delta", "0.05",
        "-f", "segment",
        "-segment_format", "matroska",
        out_pattern,
    ]

    print(f"  Running ffmpeg segment muxer…", flush=True)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"{RD}❌  ffmpeg split failed:\n{result.stderr}{R}")
        sys.exit(1)

    chunks = sorted(out_dir.glob("part_*.mkv"))
    if not chunks:
        print(f"{RD}❌  No part_*.mkv files found after split.{R}")
        sys.exit(1)

    print(f"{GR}✅  Split complete → {len(chunks)} chunks{R}", flush=True)
    for c in chunks:
        print(f"   {c.name}  ({c.stat().st_size / 1_048_576:.1f} MB)")

    return chunks


def write_github_output(chunks: list[Path], chunk_count: int) -> None:
    """
    Write matrix_json and chunk_count to $GITHUB_OUTPUT (GitHub Actions).
    No-op if GITHUB_OUTPUT is not set.
    """
    gh_output = os.environ.get("GITHUB_OUTPUT")
    if not gh_output:
        return
    matrix_json = json.dumps([c.name for c in chunks])
    with open(gh_output, "a") as f:
        f.write(f"matrix_json={matrix_json}\n")
        f.write(f"chunk_count={chunk_count}\n")
    print(f"  {DIM}→ matrix_json and chunk_count written to $GITHUB_OUTPUT{R}", flush=True)


# ─── CLI entry point ──────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Split source.mkv into keyframe-aligned chunks")
    p.add_argument("source",                    help="Input MKV file")
    p.add_argument("--chunks",   type=int, default=DEFAULT_CHUNKS, help="Number of chunks")
    p.add_argument("--out-dir",  default=".",   help="Directory for chunk files (default: .)")
    p.add_argument("--print-matrix", action="store_true",
                   help="Also write matrix_json to $GITHUB_OUTPUT")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    chunks = split(Path(args.source), args.chunks, Path(args.out_dir))
    if args.print_matrix:
        write_github_output(chunks, len(chunks))
    # Always echo the JSON array so shell scripts can capture it
    print(json.dumps([c.name for c in chunks]), flush=True)
