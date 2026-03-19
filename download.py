"""
download.py

Handles all download routing:
  - Telegram file/message links  → tg_handler.py
  - Magnet links                 → disabled (exit 1)
  - M3U8 / HLS streams           → ffmpeg
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


# ── Download strategies ────────────────────────────────────────────────────

def download_telegram():
    print("📨 Telegram link detected — delegating to tg_handler.py", flush=True)
    run(["python3", "tg_handler.py"])


def download_m3u8(url):
    print("📡 M3U8 detected — ffmpeg download", flush=True)
    referer = detect_referer(url)

    headers = f"User-Agent: {USER_AGENT}\r\n"
    if referer:
        headers += f"Referer: {referer}\r\n"

    run([
        "ffmpeg",
        "-headers", headers,
        "-i", url,
        "-c", "copy",
        "-y",
        "source.mkv",
    ])


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
