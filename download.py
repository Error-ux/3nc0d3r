"""
download.py

Handles all download routing:
  - Telegram file/message links  → tg_handler.py
  - Magnet links                 → disabled (exit 1)
  - M3U8 / HLS streams           → 30 parallel curl workers (bash script approach)
                                   + AES key pre-fetch + openssl decrypt + ffmpeg mux
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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
PARTS      = 30          # parallel curl workers (mirrors bash script)


# ── Helpers ────────────────────────────────────────────────────────────────

def run(cmd, check=True, **kwargs):
    print(f"$ {' '.join(str(c) for c in cmd)}", flush=True)
    return subprocess.run(cmd, check=check, **kwargs)


def resolve_filename(url):
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
    extra_args = " ".join(f'-H "{k}: {v}"' for k, v in extra_headers.items()) \
                 if extra_headers else ""
    cmd = f'curl -sL --fail -A "{USER_AGENT}" {ref_arg} {extra_args} "{url}"'
    result = subprocess.run(cmd, shell=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"curl failed for {url}")
    return result.stdout


def fetch_aes_key(uri, referer):
    """Try multiple header strategies to fetch an AES-128 key."""
    origin = referer.rstrip("/") if referer else ""
    strategies = [
        {"Referer": referer, "Origin": origin} if referer else {},
        {},
        {"Referer": referer} if referer else {},
        {"Origin": origin}   if origin else {},
    ]
    last_err = None
    for i, headers in enumerate(strategies):
        ref = headers.pop("Referer", "")
        try:
            data = curl_fetch(uri, referer=ref, extra_headers=headers or None)
            if data and len(data) >= 16:
                print(f"🔑 Key fetched (strategy {i+1}): {uri}", flush=True)
                return data
            print(f"⚠️  Strategy {i+1} short body ({len(data)}B), trying next…", flush=True)
        except RuntimeError as e:
            print(f"⚠️  Strategy {i+1} failed: {e}", flush=True)
            last_err = e
    raise RuntimeError(f"All key-fetch strategies exhausted for {uri}") from last_err


# ── M3U8 helpers ───────────────────────────────────────────────────────────

def resolve_base_url(m3u8_url):
    return m3u8_url.rsplit("/", 1)[0] + "/"


def parse_master_m3u8(content, base_url):
    lines    = content.splitlines()
    best_bw  = -1
    best_url = None
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF"):
            bw_match = re.search(r"BANDWIDTH=(\d+)", line)
            bw = int(bw_match.group(1)) if bw_match else 0
            if bw > best_bw and i + 1 < len(lines):
                nxt = lines[i + 1].strip()
                if nxt and not nxt.startswith("#"):
                    best_bw  = bw
                    best_url = nxt if nxt.startswith("http") else base_url + nxt
    return best_url


def parse_segments(content, base_url):
    """Return list of (segment_url, key_info_or_None)."""
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


# ── Download part (mirrors bash download_part function) ────────────────────

def download_part(part_id, seg_slice, out_dir, referer):
    """
    Download a sequential slice of segments using curl.
    Mirrors the bash script's download_part() function.
    Each of the PARTS workers calls this in its own thread.
    """
    for idx, (seg_url, _key) in seg_slice:
        fname = out_dir / f"{idx:05d}.ts"
        ref_arg = f'-H "Referer: {referer}"' if referer else ""
        cmd = f'curl -sL --fail -A "{USER_AGENT}" {ref_arg} -o "{fname}" "{seg_url}"'
        result = subprocess.run(cmd, shell=True)
        if result.returncode != 0:
            print(f"⚠️  Part {part_id}: failed segment {idx} ({seg_url})", flush=True)


# ── Main M3U8 handler ──────────────────────────────────────────────────────

def download_m3u8(url):
    """
    Parallel HLS downloader — Python port of script.sh.

    Splits segments into PARTS slices and fires each slice in its own
    thread (curl downloads sequentially within each slice).
    AES keys are pre-fetched immediately after playlist parsing so their
    tokens don't expire before the downloads complete.
    """
    print(f"📡 M3U8 detected — {PARTS} parallel curl workers", flush=True)
    referer  = detect_referer(url)
    base_url = resolve_base_url(url)

    # [+] Fetching playlist...
    print("[+] Fetching playlist...", flush=True)
    raw = curl_fetch(url, referer).decode(errors="replace")

    # If master playlist → pick best
    best_variant = parse_master_m3u8(raw, base_url)
    if best_variant:
        print(f"[+] Master playlist detected → {best_variant}", flush=True)
        base_url = resolve_base_url(best_variant)
        raw = curl_fetch(best_variant, referer).decode(errors="replace")

    # [+] Extracting segments...
    print("[+] Extracting segments...", flush=True)
    segments = parse_segments(raw, base_url)
    total    = len(segments)
    if not total:
        raise RuntimeError("No segments found in playlist")
    print(f"[+] Total segments: {total}", flush=True)

    # Pre-fetch AES keys while tokens are still valid
    key_cache = {}
    for _seg_url, key_info in segments:
        if key_info and key_info["method"] != "NONE" and key_info["uri"]:
            uri = key_info["uri"]
            if uri not in key_cache:
                key_cache[uri] = fetch_aes_key(uri, referer)

    # [+] Splitting into PARTS parts...
    seg_per_part = (total + PARTS - 1) // PARTS
    print(f"[+] Splitting into {PARTS} parts ({seg_per_part} segments each)…", flush=True)

    # Build slices: list of (part_id, [(global_idx, seg_url, key_info), ...])
    slices = []
    for p in range(PARTS):
        start = p * seg_per_part
        if start >= total:
            break
        end   = min(start + seg_per_part, total)
        chunk = [(i, segments[i]) for i in range(start, end)]
        slices.append((p, chunk))
        print(f"[+] Part {p} → segments {start} to {end - 1}", flush=True)

    with tempfile.TemporaryDirectory(prefix="hls_segs_") as tmp_dir:
        out_dir = Path(tmp_dir)

        # Start parallel downloads (mirrors: download_part $p $start $end &)
        with ThreadPoolExecutor(max_workers=PARTS) as pool:
            futures = {
                pool.submit(download_part, p, chunk, out_dir, referer): p
                for p, chunk in slices
            }
            for fut in as_completed(futures):
                p = futures[fut]
                try:
                    fut.result()
                except Exception as e:
                    print(f"⚠️  Part {p} raised: {e}", flush=True)

        print("[+] All parts downloaded", flush=True)

        # Decrypt AES-128 segments using pre-fetched keys
        for idx, (seg_url, key_info) in enumerate(segments):
            if not key_info or key_info["method"] == "NONE":
                continue
            seg_path = out_dir / f"{idx:05d}.ts"
            if not seg_path.exists():
                continue
            key_uri   = key_info["uri"]
            key_bytes = key_cache.get(key_uri) or fetch_aes_key(key_uri, referer)
            if key_info["iv"]:
                iv_hex   = key_info["iv"].replace("0x", "").replace("0X", "")
                iv_bytes = bytes.fromhex(iv_hex.zfill(32))
            else:
                iv_bytes = idx.to_bytes(16, "big")
            decrypt_segment(seg_path, key_bytes, iv_bytes)

        # [+] Creating file list... (mirrors: ls *.ts | sort > list.txt)
        print("[+] Creating file list...", flush=True)
        seg_paths = sorted(out_dir.glob("*.ts"))
        if not seg_paths:
            raise RuntimeError("No .ts files found after download")

        concat_file = out_dir / "list.txt"
        concat_file.write_text("\n".join(f"file '{p}'" for p in seg_paths))

        # [+] Merging with ffmpeg...
        print("[+] Merging with ffmpeg...", flush=True)
        run([
            "ffmpeg", "-loglevel", "error",
            "-f", "concat", "-safe", "0",
            "-i", str(concat_file),
            "-c", "copy",
            "source.mkv",
        ])

    print("[+] Done → source.mkv", flush=True)


# ── Other download strategies ──────────────────────────────────────────────

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
