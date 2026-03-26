"""
download.py
Handles all source acquisition for the AV1 pipeline.

URL routing:
  tg_file: / t.me/   →  tg_handler.py  (Pyrogram bot download)
  magnet:             →  blocked (exits 1)
  CDN_REFERER_MAP     →  proxy + aria2c, fallback to yt-dlp on failure
  *.m3u8 / platforms  →  yt-dlp + aria2c, with CDN referer auto-detection
  everything else     →  aria2c direct first, proxy fallback on failure

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
# CDN → Referer map
# Any domain listed here is tried via CF-bypass proxy first, then yt-dlp.
# ─────────────────────────────────────────────────────────────────────────────
CDN_REFERER_MAP = {
    "uwucdn.top":           "https://kwik.cx/",
    "owocdn.top":           "https://kwik.cx/",
    "kwik.cx":              "https://kwik.cx/",
    "vdownload.hembed.com": "https://hanime1.me/",
    "hembed.com":           "https://hanime1.me/",
}

# Platforms routed through yt-dlp regardless of extension
YTDLP_DOMAINS = (
    "bilibili.com",
    "nicovideo.jp",
    "vimeo.com",
    "dailymotion.com",
    "twitch.tv",
)

# CF-bypass proxy base URL
PROXY_BASE = "https://universal-proxy.cloud-dl.workers.dev/?url="

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def run(cmd, label=""):
    """Run a subprocess, stream output live, exit on failure."""
    tag = f"[{label}] " if label else ""
    print(f"{tag}▶ {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"❌ {tag}command failed (exit {result.returncode})", flush=True)
        sys.exit(result.returncode)


def run_no_exit(cmd, label=""):
    """
    Same as run() but returns the exit code instead of calling sys.exit().
    Used for fallback logic — caller decides whether to retry or abort.
    """
    tag = f"[{label}] " if label else ""
    print(f"{tag}▶ {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"⚠️  {tag}command failed (exit {result.returncode}) — will try fallback", flush=True)
    return result.returncode


def resolve_filename(url):
    """
    Best-effort human-readable filename from URL.
    Delegates to resolve_filename.py then falls back to URL basename.
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


def build_proxy_url(url):
    """Wrap a URL through the CF-bypass proxy."""
    return f"{PROXY_BASE}{urllib.parse.quote(url, safe='')}"


def build_aria2c_cmd(download_url, referer=None):
    """Return a base aria2c command list for the given URL and optional referer."""
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
    cmd.append(download_url)
    return cmd


def cleanup_partial():
    """Remove any partial aria2c output so a retry starts clean."""
    for leftover in ("source.mkv", "source.mkv.aria2"):
        if os.path.exists(leftover):
            os.remove(leftover)
            print(f"🗑️  Removed partial file: {leftover}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# DOWNLOAD ROUTES
# ─────────────────────────────────────────────────────────────────────────────

def download_telegram():
    """Delegate to tg_handler.py for all Telegram URLs."""
    print("📡 Telegram URL detected → tg_handler.py", flush=True)
    run(["python3", "tg_handler.py"], label="TG")


def download_cdn_proxy():
    """
    For domains in CDN_REFERER_MAP (e.g. uwucdn.top, kwik.cx):

    Attempt 1 — CF-bypass proxy + aria2c (fast, no yt-dlp overhead).
    Attempt 2 — yt-dlp with impersonation + referer (handles stricter CF checks).

    The original URL is used directly (not curl-resolved) since CDN auth
    tokens are embedded in the URL and curl may not follow them correctly.
    """
    output_name = resolve_output_name()
    write_fname(output_name)

    notify_download_start("aria2c (CF-proxy → yt-dlp fallback)", output_name)

    referer, ffmpeg_headers = detect_referer(URL)
    ref = referer or "https://kwik.cx/"

    # ── Attempt 1: CF-bypass proxy + aria2c ──────────────────────────────────
    proxied = build_proxy_url(URL)
    print(f"📥 Attempt 1/2: CF-proxy + aria2c  [{output_name}]", flush=True)
    print(f"🌐 Proxy URL: {proxied}", flush=True)

    cmd = build_aria2c_cmd(proxied, referer=ref)
    exit_code = run_no_exit(cmd, label="aria2c-proxy")

    if exit_code == 0:
        print("✅ Proxy download succeeded.", flush=True)
        return

    # ── Attempt 2: yt-dlp with impersonation ─────────────────────────────────
    print(f"🔄 Attempt 2/2: proxy failed — retrying via yt-dlp  [{output_name}]", flush=True)
    cleanup_partial()

    ytdlp_cmd = [
        "yt-dlp",
        "--add-header", f"Referer:{ref}",
        "--add-header", "User-Agent:Mozilla/5.0",
        "--extractor-args", "generic:impersonate",
        "--downloader", "aria2c",
        "--downloader-args",
        "aria2c:-x 16 -s 16 -k 1M --console-log-level=warn "
        "--summary-interval=10 --retry-wait=5 --max-tries=10",
        "--merge-output-format", "mkv",
        "-o", "source.mkv",
    ]

    if ffmpeg_headers:
        ytdlp_cmd += ["--downloader-args", f"ffmpeg_i:{ffmpeg_headers}"]

    ytdlp_cmd.append(URL)
    exit_code = run_no_exit(ytdlp_cmd, label="yt-dlp-fallback")

    if exit_code != 0:
        print("❌ Both proxy and yt-dlp attempts failed. Aborting.", flush=True)
        sys.exit(exit_code)

    print("✅ yt-dlp fallback succeeded.", flush=True)


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
    """
    Try aria2c direct first. If it fails, clean up and retry via CF-bypass proxy.
    curl pre-resolves redirects before either attempt.
    """
    output_name = resolve_output_name()
    write_fname(output_name)

    notify_download_start("aria2c (direct → proxy fallback)", output_name)

    # Pre-resolve redirects so aria2c / proxy both get the clean final URL
    print("🔗 Resolving final URL...", flush=True)
    resolved = subprocess.check_output(
        [
            "curl", "-s", "-o", "/dev/null", "-w", "%{url_effective}", "-L",
            "--globoff", "--user-agent", "Mozilla/5.0", URL,
        ],
        text=True,
    ).strip()
    print(f"✅ Resolved: {resolved}", flush=True)

    referer, _ = detect_referer(URL)
    if referer:
        print(f"🔗 Referer detected: {referer}", flush=True)

    # ── Attempt 1: direct ────────────────────────────────────────────────────
    print(f"📥 Attempt 1/2: direct → aria2c  [{output_name}]", flush=True)
    direct_cmd = build_aria2c_cmd(resolved, referer=referer)
    exit_code = run_no_exit(direct_cmd, label="aria2c-direct")

    if exit_code == 0:
        print("✅ Direct download succeeded.", flush=True)
        return

    # ── Attempt 2: proxy fallback ────────────────────────────────────────────
    print(f"🔄 Attempt 2/2: direct failed — retrying via proxy  [{output_name}]", flush=True)
    cleanup_partial()

    proxied_url = build_proxy_url(resolved)
    print(f"🌐 Proxy URL: {proxied_url}", flush=True)

    proxy_cmd = build_aria2c_cmd(proxied_url, referer=referer)
    exit_code = run_no_exit(proxy_cmd, label="aria2c-proxy")

    if exit_code != 0:
        print("❌ Both direct and proxy attempts failed. Aborting.", flush=True)
        sys.exit(exit_code)

    print("✅ Proxy fallback download succeeded.", flush=True)


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

    # ── anibd.app → anibd.py ─────────────────────────────────────────────────
    if "anibd.app" in URL:
        import anibd
        anibd.download(URL)
        return

    # ── Known CDN domains → proxy + aria2c, then yt-dlp fallback ─────────────
    # Checked BEFORE HLS/platform so CDN direct links never hit yt-dlp first.
    if any(cdn in URL for cdn in CDN_REFERER_MAP):
        download_cdn_proxy()
        return

    # ── HLS / known streaming platforms → yt-dlp ─────────────────────────────
    is_hls      = "m3u8" in URL
    is_platform = any(d in URL for d in YTDLP_DOMAINS)
    if is_hls or is_platform:
        download_hls_or_platform()
        return

    # ── Direct CDN / plain file URL → direct first, proxy on failure ─────────
    download_direct()


if __name__ == "__main__":
    route()
