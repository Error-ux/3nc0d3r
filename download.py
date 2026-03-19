"""
download.py

Handles all download routing:
  - Telegram file/message links  → tg_handler.py
  - Magnet links                 → disabled (exit 1)
  - M3U8 / HLS streams           → fetch ALL segments simultaneously via aria2c
                                   (token-expiry safe) + openssl decrypt + ffmpeg mux
  - Streaming platforms          → yt-dlp + aria2c
  - Direct CDN / file URLs       → aria2c

Reads from env: VIDEO_URL, CUSTOM
Writes: source.mkv, tg_fname.txt, download.log
"""

import os
import sys
import re
import subprocess
import urllib.parse
import tempfile
from pathlib import Path

URL    = os.environ.get("VIDEO_URL", "").strip()
CUSTOM = os.environ.get("CUSTOM", "").strip()

CDN_REFERER_MAP = {
    "uwucdn.top":         "https://kwik.cx/",
    "kwik.cx":            "https://kwik.cx/",
    "animefever":         "https://animefever.tv/",
    "cache.libria.fun":   "https://www.anilibria.tv/",
    "delivery.animepahe": "https://animepahe.ru/",
    "moon-cdn":           "https://gogoanime.cl/",
    "gogo-cdn":           "https://gogoanime.cl/",
}

STREAMING_PLATFORMS = [
    "bilibili.com", "nicovideo.jp",
    "vimeo.com", "dailymotion.com", "twitch.tv",
]

USER_AGENT = "Mozilla/5.0"


# ── Helpers ────────────────────────────────────────────────────────────────

def run(cmd, check=True, **kwargs):
    print(f"$ {' '.join(str(c) for c in cmd)}", flush=True)
    return subprocess.run(cmd, check=check, **kwargs)


def resolve_filename(url):
    """Return a .mkv filename for the given URL."""
    if CUSTOM:
        return f"{CUSTOM}.mkv"
    try:
        name = subprocess.check_output(
            ["python3", "resolve_filename.py", url],
            stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        name = urllib.parse.unquote(
            re.sub(r'\?.*', '', Path(url).name)
        )
    if not name.endswith((".mkv", ".mp4", ".webm")):
        name += ".mkv"
    return name


def detect_referer(url):
    for domain, referer in CDN_REFERER_MAP.items():
        if domain in url:
            print(f"🔗 Auto-detected referer: {referer} (matched: {domain})", flush=True)
            return referer
    return ""


def curl_fetch(url, referer="", extra_headers=None):
    """Fetch URL bytes via curl. Raises RuntimeError on failure."""
    ref_arg    = f'-H "Referer: {referer}"' if referer else ""
    extra_args = ""
    if extra_headers:
        extra_args = " ".join(f'-H "{k}: {v}"' for k, v in extra_headers.items())
    cmd = f'curl -sL --fail -A "{USER_AGENT}" {ref_arg} {extra_args} "{url}"'
    result = subprocess.run(cmd, shell=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"curl failed for {url}")
    return result.stdout


def fetch_aes_key(uri, referer):
    """
    Fetch an HLS AES-128 key, trying several header strategies.
    Key servers often have stricter CORS/referer rules than segment CDNs.
    """
    origin = referer.rstrip("/") if referer else ""
    strategies = [
        # 1. Referer + Origin (most permissive)
        {"Referer": referer, "Origin": origin} if referer else {},
        # 2. No headers at all (key server may whitelist bare requests)
        {},
        # 3. Referer only
        {"Referer": referer} if referer else {},
        # 4. Origin only
        {"Origin": origin} if origin else {},
    ]

    last_err = None
    for i, headers in enumerate(strategies):
        ref = headers.pop("Referer", "")
        try:
            data = curl_fetch(uri, referer=ref, extra_headers=headers or None)
            if data and len(data) >= 16:
                print(f"🔑 Key fetched (strategy {i+1}): {uri}", flush=True)
                return data
            print(f"⚠️  Strategy {i+1} returned short body ({len(data)}B), trying next…",
                  flush=True)
        except RuntimeError as e:
            print(f"⚠️  Strategy {i+1} failed: {e}", flush=True)
            last_err = e

    raise RuntimeError(f"All key-fetch strategies exhausted for {uri}") from last_err


# ── M3U8 parallel downloader ───────────────────────────────────────────────

def resolve_base_url(m3u8_url):
    return m3u8_url.rsplit("/", 1)[0] + "/"


def parse_master_m3u8(content, base_url):
    """
    If this is a master playlist, return the URL of the highest-bandwidth variant.
    Otherwise return None.
    """
    lines    = content.splitlines()
    best_bw  = -1
    best_url = None
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF"):
            bw_match = re.search(r"BANDWIDTH=(\d+)", line)
            bw = int(bw_match.group(1)) if bw_match else 0
            if bw > best_bw and i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if next_line and not next_line.startswith("#"):
                    best_bw  = bw
                    best_url = next_line if next_line.startswith("http") \
                               else base_url + next_line
    return best_url


def parse_segments(content, base_url):
    """
    Return list of (segment_url, key_info_or_None).
    key_info = {"method": str, "uri": str, "iv": str|None}
    """
    segments = []
    lines    = content.splitlines()
    key_info = None

    for line in lines:
        line = line.strip()
        if line.startswith("#EXT-X-KEY"):
            method = re.search(r'METHOD=([^,\s]+)', line)
            uri    = re.search(r'URI="([^"]+)"',    line)
            iv     = re.search(r'IV=([^,\s]+)',      line)
            key_info = {
                "method": method.group(1) if method else "NONE",
                "uri":    uri.group(1)    if uri    else "",
                "iv":     iv.group(1)     if iv     else None,
            }
        elif line and not line.startswith("#"):
            seg_url = line if line.startswith("http") else base_url + line
            segments.append((seg_url, key_info))

    return segments


def build_aria2c_input(segments, referer, out_dir):
    """
    Write an aria2c input file with per-URI headers baked in.
    This is the key to avoiding 403s — no shell quoting issues.
    """
    lines = []
    for idx, (url, _key) in enumerate(segments):
        seg_file = out_dir / f"seg_{idx:05d}.ts"
        lines.append(url)
        lines.append(f"  out={seg_file.name}")
        lines.append(f"  dir={out_dir}")
        lines.append(f"  header=User-Agent: {USER_AGENT}")
        if referer:
            lines.append(f"  header=Referer: {referer}")
        lines.append("")  # blank line = separator

    input_file = out_dir / "aria2c_input.txt"
    input_file.write_text("\n".join(lines))
    return input_file


def decrypt_segment(seg_path, key_bytes, iv_bytes):
    """AES-128-CBC decrypt a .ts segment in-place via openssl."""
    iv_hex = iv_bytes.hex() if isinstance(iv_bytes, (bytes, bytearray)) else iv_bytes
    tmp    = seg_path.with_suffix(".dec")
    cmd = (
        f'openssl enc -d -aes-128-cbc -nosalt '
        f'-K {key_bytes.hex()} -iv {iv_hex} '
        f'-in "{seg_path}" -out "{tmp}"'
    )
    result = subprocess.run(cmd, shell=True)
    if result.returncode == 0:
        tmp.replace(seg_path)
    else:
        print(f"⚠️  Decryption failed for {seg_path.name}, using raw segment", flush=True)
        tmp.unlink(missing_ok=True)


def download_m3u8(url):
    """
    True parallel HLS downloader.

    Why not yt-dlp --concurrent-fragments?
      Segment URLs contain time-limited tokens. yt-dlp processes fragments
      in rolling batches, so later segments arrive after the tokens expire → 403.
      aria2c with --max-concurrent-downloads=N fires ALL segments at once,
      so every request is made while the tokens are still valid.
    """
    print("📡 M3U8 detected — true parallel segment download (token-expiry safe)", flush=True)
    referer  = detect_referer(url)
    base_url = resolve_base_url(url)

    # Step 1: fetch playlist
    print("⬇️  Fetching playlist…", flush=True)
    raw = curl_fetch(url, referer).decode(errors="replace")

    # Step 2: handle master playlist → pick highest-bandwidth variant
    best_variant = parse_master_m3u8(raw, base_url)
    if best_variant:
        print(f"🎯 Master playlist — selected best variant:\n   {best_variant}", flush=True)
        base_url = resolve_base_url(best_variant)
        raw = curl_fetch(best_variant, referer).decode(errors="replace")

    # Step 3: parse all segment URLs + encryption metadata
    segments = parse_segments(raw, base_url)
    if not segments:
        raise RuntimeError("No segments found in playlist")
    print(f"📋 Found {len(segments)} segments", flush=True)

    with tempfile.TemporaryDirectory(prefix="hls_segs_") as tmp_dir:
        out_dir = Path(tmp_dir)

        # Step 4: download ALL segments simultaneously before tokens expire
        input_file = build_aria2c_input(segments, referer, out_dir)
        print(f"⚡ Firing all {len(segments)} segments simultaneously…", flush=True)
        run([
            "aria2c",
            "--input-file",                str(input_file),
            "--max-connection-per-server=16",
            "--split=16",
            "--min-split-size=1M",
            f"--max-concurrent-downloads={len(segments)}",  # ALL at once
            "--console-log-level=warn",
            "--summary-interval=10",
            "--retry-wait=5",
            "--max-tries=10",
            "--allow-overwrite=true",
            "--auto-file-renaming=false",
        ])

        # Step 5: decrypt AES-128 segments if needed
        key_cache = {}
        for idx, (seg_url, key_info) in enumerate(segments):
            if not key_info or key_info["method"] == "NONE":
                continue

            seg_path = out_dir / f"seg_{idx:05d}.ts"
            if not seg_path.exists():
                print(f"⚠️  Missing segment {idx}, skipping decrypt", flush=True)
                continue

            key_uri = key_info["uri"]
            if key_uri not in key_cache:
                key_cache[key_uri] = fetch_aes_key(key_uri, referer)

            key_bytes = key_cache[key_uri]

            # IV defaults to segment index (big-endian 128-bit)
            if key_info["iv"]:
                iv_hex   = key_info["iv"].replace("0x", "").replace("0X", "")
                iv_bytes = bytes.fromhex(iv_hex.zfill(32))
            else:
                iv_bytes = idx.to_bytes(16, "big")

            decrypt_segment(seg_path, key_bytes, iv_bytes)

        # Step 6: build ffmpeg concat list
        seg_paths = sorted(out_dir.glob("seg_*.ts"))
        if not seg_paths:
            raise RuntimeError("No .ts segments found after download")

        concat_file = out_dir / "concat.txt"
        concat_file.write_text("\n".join(f"file '{p}'" for p in seg_paths))

        # Step 7: mux into final MKV
        print(f"🎬 Muxing {len(seg_paths)} segments → source.mkv…", flush=True)
        run([
            "ffmpeg", "-y",
            "-f",        "concat",
            "-safe",     "0",
            "-i",        str(concat_file),
            "-c",        "copy",
            "-movflags", "+faststart",
            "source.mkv",
        ])

    print("✅ source.mkv ready", flush=True)


# ── Other download strategies (unchanged) ─────────────────────────────────

def download_telegram():
    print("📨 Telegram link detected — delegating to tg_handler.py", flush=True)
    run(["python3", "tg_handler.py"])


def download_streaming(url):
    print("📡 Streaming platform detected — using yt-dlp", flush=True)
    run([
        "yt-dlp",
        "--add-header", f"User-Agent: {USER_AGENT}",
        "--downloader", "aria2c",
        "--downloader-args",
        "aria2c:-x 16 -s 16 -k 1M --console-log-level=warn "
        "--summary-interval=10 --retry-wait=5 --max-tries=10",
        "--merge-output-format", "mkv",
        "-o", "source.mkv",
        url,
    ])


def download_direct(url):
    print("📥 Direct CDN download — resolving URL...", flush=True)
    result = subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-w", "%{url_effective}", "-L",
         "-A", USER_AGENT, url],
        capture_output=True
    )
    final_url = result.stdout.decode().strip() or url
    print(f"✅ Resolved: {final_url}", flush=True)
    run([
        "aria2c", "-x", "16", "-s", "16", "-k", "1M",
        f"--user-agent={USER_AGENT}",
        "--console-log-level=warn",
        "--summary-interval=10",
        "--retry-wait=5",
        "--max-tries=10",
        "-o", "source.mkv",
        final_url,
    ])


# ── Main router ───────────────────────────────────────────────────────────

def main():
    if not URL:
        print("❌ VIDEO_URL is not set", file=sys.stderr)
        sys.exit(1)

    fn = resolve_filename(URL)

    if URL.startswith("tg_file:") or URL.startswith("https://t.me/"):
        download_telegram()

    elif URL.startswith("magnet:"):
        print("❌ ERROR: Magnet links are disabled.")
        sys.exit(1)

    elif "m3u8" in URL:
        download_m3u8(URL)

    elif any(p in URL for p in STREAMING_PLATFORMS):
        download_streaming(URL)

    else:
        download_direct(URL)

    Path("tg_fname.txt").write_text(fn)
    print(f"✅ Done → {fn}", flush=True)


if __name__ == "__main__":
    main()
