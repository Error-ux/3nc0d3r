"""
download.py

Handles all download routing:
  - Telegram file/message links  → tg_handler.py
  - Magnet links                 → disabled (exit 1)
  - M3U8 / HLS streams           → aria2c parallel + openssl decrypt + ffmpeg mux
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
    "uwucdn.top":        "https://kwik.cx/",
    "kwik.cx":           "https://kwik.cx/",
    "animefever":        "https://animefever.tv/",
    "cache.libria.fun":  "https://www.anilibria.tv/",
    "delivery.animepahe":"https://animepahe.ru/",
    "moon-cdn":          "https://gogoanime.cl/",
    "gogo-cdn":          "https://gogoanime.cl/",
}

STREAMING_PLATFORMS = [
    "bilibili.com", "nicovideo.jp",
    "vimeo.com", "dailymotion.com", "twitch.tv",
]


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


def curl_fetch(url, referer="", output=None):
    # Use shell=True so curl behaves identically to bash — avoids header
    # differences that cause 403 on protected key servers
    out_arg  = f"-o {output}" if output else "-o -"
    ref_arg  = f'-H "Referer: {referer}"' if referer else ""
    cmd = f'curl -sL --fail -A "Mozilla/5.0" {ref_arg} {out_arg} "{url}"'
    result = subprocess.run(cmd, shell=True, capture_output=not output)
    if result.returncode != 0:
        raise RuntimeError(f"curl failed (HTTP {result.returncode}) for {url}")
    return result.stdout if not output else None


# ── Download strategies ────────────────────────────────────────────────────

def download_telegram():
    print("📨 Telegram link detected — delegating to tg_handler.py", flush=True)
    run(["python3", "tg_handler.py"])


def download_m3u8(url):
    print("📡 M3U8 detected — ffmpeg sequential download", flush=True)
    referer = detect_referer(url)

    # ffmpeg sequential is reliable on CDN-protected streams:
    # - Opens a fresh TCP/TLS connection per segment (harder to IP-ban mid-stream)
    # - Passes Referer on every request including AES key fetch via -headers
    # - Proven working on uwucdn.top and similar protected HLS sources
    # NOTE: headers string must use actual \r\n (CRLF) — same as bash $'...' syntax
    if referer:
        headers = f"Referer: {referer}\r\nUser-Agent: Mozilla/5.0\r\n"
    else:
        headers = "User-Agent: Mozilla/5.0\r\n"

    cmd = [
        "ffmpeg",
        "-headers", headers,
        "-allowed_extensions", "ALL",
        "-extension_picky", "0",
        "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-i", url,
        "-c", "copy",
        "source.mkv",
        "-y",
    ]
    run(cmd)


def download_streaming(url):
    print("📡 Streaming platform detected — using yt-dlp", flush=True)
    run([
        "yt-dlp",
        "--add-header", "User-Agent:Mozilla/5.0",
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
         "-A", "Mozilla/5.0", url],
        capture_output=True
    )
    final_url = result.stdout.decode().strip() or url
    print(f"✅ Resolved: {final_url}", flush=True)
    run([
        "aria2c", "-x", "16", "-s", "16", "-k", "1M",
        "--user-agent=Mozilla/5.0",
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
