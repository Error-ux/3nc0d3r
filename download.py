"""
download.py
Handles all source acquisition for the AV1 pipeline.

URL routing:
  tg_file: / t.me/   →  tg_handler.py  (Pyrogram bot download)
  magnet:             →  blocked (exits 1)
  *.m3u8 / platforms  →  yt-dlp + aria2c, with CDN referer auto-detection
  everything else     →  aria2c direct download (curl pre-resolves redirects)

Outputs:
  source.mkv          — downloaded file (always this name)
  tg_fname.txt        — human-readable final filename for the encode step
"""

import json
import os
import re
import sys
import subprocess
import urllib.parse

# ─────────────────────────────────────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────────────────────────────────────
URL        = os.environ.get("VIDEO_URL", "").strip()
CUSTOM     = os.environ.get("CUSTOM", "").strip()
BOT_TOKEN  = os.environ.get("TG_BOT_TOKEN", "").strip()
CHAT_ID    = os.environ.get("TG_CHAT_ID", "").strip()
RUN_NUMBER = os.environ.get("GITHUB_RUN_NUMBER", "?")

# ─────────────────────────────────────────────────────────────────────────────
# CDN → Referer map  (mirrors the bash associative array)
# ─────────────────────────────────────────────────────────────────────────────
CDN_REFERER_MAP = {
    "uwucdn.top":          "https://kwik.cx/",
    "owocdn.top":          "https://kwik.cx/",
    "kwik.cx":             "https://kwik.cx/",
}

# Platforms routed through yt-dlp regardless of extension
YTDLP_DOMAINS = (
    "bilibili.com",
    "nicovideo.jp",
    "vimeo.com",
    "dailymotion.com",
    "twitch.tv",
    "kwik.cx",      # Cloudflare-protected CDN, requires impersonation
)

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def run(cmd, label=""):
    """Run a subprocess, stream output live, raise on non-zero exit."""
    tag = f"[{label}] " if label else ""
    print(f"{tag}▶ {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"❌ {tag}command failed (exit {result.returncode})", flush=True)
        sys.exit(result.returncode)


def resolve_filename(url):
    """
    Best-effort human-readable filename from URL.
    Delegates to resolve_filename.py then falls back to URL basename.
    Always returns a string ending in .mkv / .mp4 / .webm.
    """
    try:
        out = subprocess.check_output(
            ["python3", "resolve_filename.py", url],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        if out:
            return out
    except Exception:
        pass

    # Fallback: URL path basename, URL-decoded
    raw = urllib.parse.urlparse(url).path.split("/")[-1]
    raw = re.sub(r"\?.*", "", raw)
    return urllib.parse.unquote(raw)


def ensure_video_ext(name):
    """Append .mkv if name has no recognised video extension."""
    if not re.search(r"\.(mkv|mp4|webm)$", name, re.IGNORECASE):
        return name + ".mkv"
    return name


def write_fname(name):
    with open("tg_fname.txt", "w", encoding="utf-8") as f:
        f.write(name)
    print(f"📝 tg_fname.txt → {name}", flush=True)


def resolve_output_name():
    """Return the final output filename (with extension)."""
    if CUSTOM:
        return ensure_video_ext(CUSTOM)
    return ensure_video_ext(resolve_filename(URL))


def detect_referer(url):
    """
    Return (referer_url, ffmpeg_headers_string) if the URL matches a known CDN,
    otherwise (None, None).
    """
    for cdn_domain, referer in CDN_REFERER_MAP.items():
        if cdn_domain in url:
            print(f"🔗 Auto-detected referer: {referer}  (matched: {cdn_domain})", flush=True)
            ffmpeg_headers = (
                "-allowed_extensions ALL "
                "-extension_picky 0 "
                "-protocol_whitelist file,http,https,tcp,tls,crypto "
                f"-headers 'Referer: {referer}\\r\\nUser-Agent: Mozilla/5.0\\r\\n'"
            )
            return referer, ffmpeg_headers
    return None, None


def notify_download_start(method, output_name):
    """Send a Telegram message announcing the download has started."""
    if not BOT_TOKEN or not CHAT_ID:
        return
    message = (
        "<code>"
        "┌─── 📥 [ DOWNLOAD.INIT ] ───────────────┐\n"
        "│\n"
        f"│ 📂 FILE : {output_name}\n"
        f"│ ⚙️  VIA  : {method}\n"
        f"│ 🔢 RUN  : #{RUN_NUMBER}\n"
        "│\n"
        "│ 🚀 STATUS: Acquiring source...\n"
        "└────────────────────────────────────┘"
        "</code>"
    )
    payload = json.dumps({"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"})
    subprocess.run(
        [
            "curl", "-s", "-X", "POST",
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            "-H", "Content-Type: application/json",
            "-d", payload,
        ],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


# ─────────────────────────────────────────────────────────────────────────────
# DOWNLOAD ROUTES
# ─────────────────────────────────────────────────────────────────────────────

def download_telegram():
    """Delegate to tg_handler.py for all Telegram URLs."""
    print("📡 Telegram URL detected → tg_handler.py", flush=True)
    run(["python3", "tg_handler.py"], label="TG")


def download_hls_or_platform():
    """Use yt-dlp (+ aria2c backend) for HLS streams and known platforms."""
    output_name = resolve_output_name()
    write_fname(output_name)

    notify_download_start("yt-dlp (HLS/platform)", output_name)
    referer, ffmpeg_headers = detect_referer(URL)

    cmd = [
        "yt-dlp",
        "--add-header", "User-Agent:Mozilla/5.0",
        "--extractor-args", "generic:impersonate",
        "--downloader", "aria2c",
        "--downloader-args",
        "aria2c:-x 16 -s 16 -k 1M --console-log-level=warn "
        "--summary-interval=10 --retry-wait=5 --max-tries=10",
        "--merge-output-format", "mkv",
        "-o", "source.mkv",
    ]

    if referer:
        cmd += ["--referer", referer]

    if ffmpeg_headers:
        cmd += ["--downloader-args", f"ffmpeg_i:{ffmpeg_headers}"]

    cmd.append(URL)
    print(f"📡 Streaming URL detected → yt-dlp  [{output_name}]", flush=True)
    run(cmd, label="yt-dlp")


def download_direct():
    """Use aria2c for plain CDN/direct file URLs (curl pre-resolves redirects)."""
    output_name = resolve_output_name()
    write_fname(output_name)

    notify_download_start("aria2c (direct)", output_name)
    # Pre-resolve redirects so aria2c gets the clean final URL
    print("🔗 Resolving final URL...", flush=True)
    resolved = subprocess.check_output(
        [
            "curl", "-s", "-o", "/dev/null", "-w", "%{url_effective}", "-L",
            "--user-agent", "Mozilla/5.0", URL,
        ],
        text=True,
    ).strip()
    print(f"✅ Resolved: {resolved}", flush=True)

    referer, _ = detect_referer(URL)

    cmd = [
        "aria2c",
        "-x", "16", "-s", "16", "-k", "1M",
        "--user-agent=Mozilla/5.0",
        "--console-log-level=warn",
        "--summary-interval=10",
        "--retry-wait=5",
        "--max-tries=10",
        "-o", "source.mkv",
    ]

    if referer:
        cmd += [f"--header=Referer: {referer}"]
        print(f"🔗 Sending referer for direct download: {referer}", flush=True)

    cmd.append(resolved)
    print(f"📥 Direct download → aria2c  [{output_name}]", flush=True)
    run(cmd, label="aria2c")


# ─────────────────────────────────────────────────────────────────────────────
# ROUTER
# ─────────────────────────────────────────────────────────────────────────────

def route():
    if not URL:
        print("❌ VIDEO_URL is empty.", flush=True)
        sys.exit(1)

    # ── Telegram ─────────────────────────────────────────────────────────────
    if URL.startswith("tg_file:") or "t.me/" in URL:
        download_telegram()
        return

    # ── Magnet (blocked) ─────────────────────────────────────────────────────
    if URL.startswith("magnet:"):
        print("❌ ERROR: Magnet links are disabled.", flush=True)
        sys.exit(1)

    # ── anibd.app → Anidb.py ─────────────────────────────────────────────────
    if "anibd.app" in URL:
        import Anidb
        Anidb.download(URL)
        return

    # ── HLS / known streaming platforms → yt-dlp ─────────────────────────────
    is_hls      = "m3u8" in URL
    is_platform = any(d in URL for d in YTDLP_DOMAINS)
    if is_hls or is_platform:
        download_hls_or_platform()
        return

    # ── Direct CDN / plain file URL → aria2c ─────────────────────────────────
    download_direct()


if __name__ == "__main__":
    route()
