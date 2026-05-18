"""
encode.py — Encode a single video chunk to AV1 (SVT-AV1) + Opus audio.

GitHub Actions matrix job usage (one runner per chunk):
    python3 encode.py part_00.mkv [options]
    → writes part_00-encoded.mkv to --out-dir

This script is intentionally silent on Telegram.
All progress updates are owned by batch_monitor.py.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from utils.media import (
    check_ffmpeg, get_subtitle_maps, get_all_subtitle_info,
    get_frame_count, fmt_size, fmt_duration, FFMPEG,
)

# ─── Defaults ─────────────────────────────────────────────────────────────────
DEFAULT_CRF    = 38
DEFAULT_PRESET = 4


# ─── SVT-AV1 param builder ────────────────────────────────────────────────────

def _build_svtav1_params(grain: int = 0, duration: float = 1500.0) -> str:
    """
    Build the SVT-AV1 tune string with dynamic la-depth.

    la-depth scales to content length:
      < 5 min  → 90  (shorts, OPs, demos)
      < 25 min → 60  (standard episodes)
      25 min + → 40  (movies, long OVAs)
    """
    grain_val = max(0, min(50, int(grain)))

    if duration < 300:
        la_depth = 90
    elif duration < 1500:
        la_depth = 60
    else:
        la_depth = 40

    return (
        f"tune=2:film-grain={grain_val}:enable-overlays=1:"
        f"aq-mode=2:variance-boost-strength=3:variance-octile=6:"
        f"enable-qm=1:qm-min=0:qm-max=8:sharpness=1:"
        f"scd=1:scd-sensitivity=10:enable-tf=1:"
        f"pin=0:lp=2:tile-columns=2:tile-rows=1:la-depth={la_depth}:"
        f"fast-decode=1"
    )


# ─── Video filter builder ─────────────────────────────────────────────────────

def _build_vf(
    crop_val: str | None = None,
    res:      str | None = None,
    is_hdr:   bool       = False,
) -> list[str]:
    """Return the ffmpeg -vf argument list (empty if no filters needed)."""
    filters: list[str] = []

    if crop_val:
        filters.append(f"crop={crop_val}")

    if res and str(res).strip().isdigit():
        filters.append(f"scale=-2:{res}:flags=lanczos")

    if is_hdr:
        filters += [
            "zscale=t=linear:npl=100",
            "format=gbrpf32le",
            "zscale=p=bt709",
            "tonemap=hable:desat=0",
            "zscale=t=bt709:m=bt709:r=tv",
            "format=yuv420p10le",
        ]

    return ["-vf", ",".join(filters)] if filters else []


# ─── PGS + subtitle metadata ──────────────────────────────────────────────────

def _build_sub_args(chunk: Path) -> tuple[list[str], list[str]]:
    """
    Returns (pgs_exclusion_args, sub_title_meta_args).

    pgs_exclusions — -map -0:s:N for every PGS/bitmap sub stream.
    sub_title_meta — -metadata:s:s:N title=<lang_name> for kept text subs.
    """
    _PGS = {"hdmv_pgs_subtitle", "dvd_subtitle", "pgssub", "hdmv_pgs_bitmap"}
    sub_info = get_all_subtitle_info(chunk)

    pgs_exclusions: list[str] = []
    sub_title_meta: list[str] = []
    out_sub_idx = 0

    for i, st in enumerate(sub_info):
        codec = st.get("codec", "").lower()
        if codec in _PGS:
            pgs_exclusions += ["-map", f"-0:s:{i}"]
            print(f"  [{chunk.name}] Stripping PGS s:{i} ({st['lang']})")
            continue
        try:
            from utils.rename import lang_code_to_name
            lang_name = lang_code_to_name(st["lang"])
        except Exception:
            lang_name = st["lang"].upper() or "Unknown"
        sub_title_meta += [f"-metadata:s:s:{out_sub_idx}", f"title={lang_name}"]
        out_sub_idx += 1

    return pgs_exclusions, sub_title_meta


# ─── Single-chunk encoder ─────────────────────────────────────────────────────

def encode_chunk(
    chunk:         Path,
    out_dir:       Path,
    crf:           int   = DEFAULT_CRF,
    preset:        int   = DEFAULT_PRESET,
    grain:         int   = 0,
    crop_val:      str | None = None,
    res:           str | None = None,
    is_hdr:        bool  = False,
    audio_bitrate: str   = "64k",
    duration:      float = 1500.0,
) -> tuple[Path, bool]:
    """
    Encode a single chunk with SVT-AV1 + Opus.
    Returns (output_path, success).
    No Telegram calls — monitor job owns all channel updates.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / chunk.name.replace(".mkv", "-encoded.mkv")

    total_frames               = get_frame_count(chunk)
    pgs_exclusions, sub_title_meta = _build_sub_args(chunk)
    vf_args                    = _build_vf(crop_val, res, is_hdr)
    svtav1_params              = _build_svtav1_params(grain, duration)

    sdr_tags = (
        ["-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709"]
        if not is_hdr else []
    )

    print(
        f"  [{chunk.name}] frames={total_frames}  "
        f"crf={crf}  preset={preset}  grain={grain}  "
        f"crop={crop_val or 'none'}  hdr={is_hdr}",
        flush=True,
    )

    cmd = [
        FFMPEG,
        "-analyzeduration", "100M", "-probesize", "100M",
        "-i", str(chunk),
        "-map", "0:v", "-map", "0:a", "-map", "0:s?",
        *pgs_exclusions,
        *vf_args,
        "-c:v", "libsvtav1",
        "-pix_fmt", "yuv420p10le",
        "-crf", str(crf),
        *sdr_tags,
        "-preset", str(preset),
        "-svtav1-params", svtav1_params,
        "-threads", "0",
        "-c:a", "libopus", "-b:a", audio_bitrate, "-vbr", "on",
        *sub_title_meta,
        "-c:s", "copy",
        "-map_chapters", "0",
        str(out), "-y",
    ]

    start = time.time()
    proc  = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    _, stderr = proc.communicate()
    elapsed = time.time() - start

    if proc.returncode != 0:
        err = stderr.decode(errors="replace")
        print(f"\n[ERROR] Encode failed: {chunk.name}\n{err}", flush=True)
        return out, False

    size_mb = out.stat().st_size / 1_048_576 if out.exists() else 0
    print(
        f"  ✅  {chunk.name} → {out.name}  "
        f"({fmt_size(size_mb)})  [{fmt_duration(elapsed)}]",
        flush=True,
    )
    return out, True


# ─── CLI entry point ──────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Encode a video chunk with SVT-AV1 + Opus")
    p.add_argument("chunk",               help="Single chunk MKV file (matrix mode)")
    p.add_argument("--crf",           type=int,   default=DEFAULT_CRF)
    p.add_argument("--preset",        type=int,   default=DEFAULT_PRESET)
    p.add_argument("--grain",         type=int,   default=0,      help="Film grain 0–50")
    p.add_argument("--crop",          default=None,               help="Crop string e.g. 1920:800:0:140")
    p.add_argument("--res",           default=None,               help="Scale height e.g. 1080")
    p.add_argument("--hdr",           action="store_true",        help="Apply HDR→SDR tonemap")
    p.add_argument("--audio-bitrate", default="64k",              help="Opus bitrate (default 64k)")
    p.add_argument("--duration",      type=float, default=1500.0, help="Source duration for la-depth calc")
    p.add_argument("--out-dir",       default=None,               help="Output directory")
    p.add_argument("--params-file",   default=None,               help="Path to encode_params.json")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    check_ffmpeg()

    chunk = Path(args.chunk)
    if not chunk.exists():
        print(f"[ERROR] Chunk not found: {chunk}")
        sys.exit(1)

    # Load params from JSON if provided (preferred over CLI flags for batch jobs)
    params: dict = {}
    if args.params_file and Path(args.params_file).exists():
        with open(args.params_file) as f:
            params = json.load(f)

    out_dir = Path(args.out_dir) if args.out_dir else chunk.parent

    print(f"\n━━━ Encode (single chunk) ━━━━━━━━━━━━━━━━━━━━━━━━", flush=True)

    _, ok = encode_chunk(
        chunk         = chunk,
        out_dir       = out_dir,
        crf           = params.get("crf",           args.crf),
        preset        = params.get("preset",         args.preset),
        grain         = params.get("grain",          args.grain),
        crop_val      = params.get("crop_val",       args.crop),
        res           = params.get("res",            args.res),
        is_hdr        = params.get("is_hdr",         args.hdr),
        audio_bitrate = params.get("audio_bitrate",  args.audio_bitrate),
        duration      = params.get("duration",       args.duration),
    )
    sys.exit(0 if ok else 1)
