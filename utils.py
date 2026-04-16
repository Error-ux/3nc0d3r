"""
utils.py — Shared helpers: ffmpeg wrappers, ANSI colours, formatters.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

# ─── ffmpeg binary names ──────────────────────────────────────────────────────
FFMPEG  = "ffmpeg"
FFPROBE = "ffprobe"

# ─── ANSI Colours ─────────────────────────────────────────────────────────────
R   = "\033[0m"
B   = "\033[1m"
DIM = "\033[2m"
CY  = "\033[96m"
GR  = "\033[92m"
YL  = "\033[93m"
RD  = "\033[91m"

# ─── Dependency checks ────────────────────────────────────────────────────────

def check_ffmpeg() -> None:
    for tool in (FFMPEG, FFPROBE):
        if shutil.which(tool) is None:
            print(f"{RD}Error: '{tool}' is not installed or not in PATH.{R}")
            sys.exit(1)

# ─── ffprobe helpers ──────────────────────────────────────────────────────────

def get_duration(path: Path) -> float:
    result = subprocess.run(
        [FFPROBE, "-v", "error",
         "-analyzeduration", "100M", "-probesize", "100M",
         "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1",
         str(path)],
        capture_output=True, text=True, check=True,
    )
    return float(result.stdout.strip())


def get_frame_count(path: Path) -> int:
    result = subprocess.run(
        [FFPROBE, "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=nb_frames",
         "-of", "csv=p=0", str(path)],
        capture_output=True, text=True,
    )
    raw = result.stdout.strip().splitlines()
    try:
        return int(raw[0]) if raw else 0
    except (ValueError, IndexError):
        return 0


def get_subtitle_maps(path: Path) -> list[str]:
    """
    Return ffmpeg -map args for text-based subtitle streams only.
    PGS and DVD bitmap formats are excluded (they'll be handled separately).
    """
    skip   = {"hdmv_pgs_subtitle", "dvd_subtitle", "pgssub", "hdmv_pgs_bitmap"}
    result = subprocess.run(
        [FFPROBE, "-v", "error", "-select_streams", "s",
         "-show_entries", "stream=codec_name",
         "-of", "csv=p=0", str(path)],
        capture_output=True, text=True,
    )
    maps: list[str] = []
    for i, codec in enumerate(result.stdout.strip().splitlines()):
        if codec.strip().lower() not in skip:
            maps.extend(["-map", f"0:s:{i}"])
    return maps


def get_all_subtitle_info(path: Path) -> list[dict]:
    """
    Return a list of subtitle stream dicts with index, codec, lang, title.
    Used for PGS detection and sub-title renaming.
    """
    import json
    result = subprocess.run(
        [FFPROBE, "-v", "quiet",
         "-print_format", "json",
         "-show_streams", "-select_streams", "s",
         str(path)],
        capture_output=True, text=True,
    )
    try:
        data = json.loads(result.stdout)
    except Exception:
        return []
    subs = []
    for st in data.get("streams", []):
        tags     = st.get("tags", {})
        tag_low  = {k.lower(): v for k, v in tags.items()}
        subs.append({
            "index":   st.get("index", 0),
            "codec":   st.get("codec_name", ""),
            "lang":    tag_low.get("language", "und"),
            "title":   tag_low.get("title",    ""),
            "forced":  bool(st.get("disposition", {}).get("forced", 0)),
            "default": bool(st.get("disposition", {}).get("default", 0)),
        })
    return subs


def verify_mkv_magic(path: Path) -> bool:
    try:
        return path.read_bytes()[:4].hex() == "1a45dfa3"
    except OSError:
        return False

# ─── Formatting helpers ───────────────────────────────────────────────────────

def fmt_size(mb: float) -> str:
    return f"{mb / 1024:.1f} GB" if mb >= 1024 else f"{mb:.0f} MB"


def fmt_duration(secs: float) -> str:
    m, s = divmod(int(secs), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def progress_bar(pct: float, width: int = 20) -> str:
    filled = int(pct / 100 * width)
    return "▰" * filled + "▱" * (width - filled)
