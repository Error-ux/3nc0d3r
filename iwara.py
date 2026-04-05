#!/usr/bin/env python3
"""
iwara.py
Iwara.tv / Iwara.ai video downloader — pipeline module + standalone CLI.

Pipeline usage (called by download.py):
    from iwara import download
    download(url)

CLI usage:
    python3 iwara.py <url> [quality]
    python3 iwara.py https://www.iwara.tv/video/laOLZIqV5BJA5W
    python3 iwara.py https://www.iwara.tv/video/laOLZIqV5BJA5W 1080

Outputs (pipeline-standard):
    source.mkv        — downloaded video
    tg_fname.txt      — human-readable filename for the encode step
    iwara_source.txt  — marker so main.py skips anitopy auto-rename

Environment variables (pipeline):
    IWARA_TOKEN        — Bearer token for R-18 / authenticated content
    IWARA_QUALITY      — Preferred quality (e.g. "Source", "1080", "720"); default: Source
    TG_BOT_TOKEN       — Telegram bot token for progress notifications
    TG_CHAT_ID         — Telegram chat ID
    GITHUB_RUN_NUMBER  — Run number for display
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urlparse, parse_qs, quote
import urllib.request

# ─── Configuration ────────────────────────────────────────────────────────────
CHUNK_SIZE     = 65536   # 64 KB chunks for pipeline speed
TIMEOUT        = 20
_IWARA_SECRET  = os.environ.get("IWARA_SECRET", "")
# ─── Env ──────────────────────────────────────────────────────────────────────
BOT_TOKEN     = os.environ.get("TG_BOT_TOKEN",        "").strip()
CHAT_ID       = os.environ.get("TG_CHAT_ID",          "").strip()
RUN_NUMBER    = os.environ.get("GITHUB_RUN_NUMBER",   "?")
IWARA_TOKEN   = os.environ.get("IWARA_TOKEN",         "").strip()
IWARA_QUALITY = os.environ.get("IWARA_QUALITY",       "").strip()
# ─── ANSI (CLI only) ──────────────────────────────────────────────────────────
R   = "\033[0m";  B  = "\033[1m";  DIM = "\033[2m"
CY  = "\033[96m"; GR = "\033[92m"; YL  = "\033[93m"; RD = "\033[91m"


# ─── Telegram Helpers ─────────────────────────────────────────────────────────

def _tg_api(endpoint: str, payload: dict) -> dict | None:
    if not BOT_TOKEN or not CHAT_ID:
        return None
    try:
        data   = json.dumps(payload).encode()
        result = subprocess.run(
            ["curl", "-s", "-X", "POST",
             f"https://api.telegram.org/bot{BOT_TOKEN}/{endpoint}",
             "-H", "Content-Type: application/json",
             "-d", data.decode()],
            check=False, timeout=10, capture_output=True,
        )
        return json.loads(result.stdout.decode()) if result.stdout else None
    except Exception:
        return None


def _tg_send(text: str) -> int | None:
    resp = _tg_api("sendMessage", {
        "chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"
    })
    try:
        return resp["result"]["message_id"]
    except Exception:
        return None


def _tg_edit(msg_id: int | None, text: str) -> None:
    if not msg_id:
        return
    _tg_api("editMessageText", {
        "chat_id": CHAT_ID, "message_id": msg_id,
        "text": text, "parse_mode": "HTML",
    })


def _notify_start(filename: str) -> int | None:
    return _tg_send(
        "<code>"
        "┌─── 📥 [ IWARA.DOWNLOADER ] ────────┐\n"
        "│\n"
        f"│ 📂 FILE : {filename}\n"
        f"│ 🔢 RUN  : #{RUN_NUMBER}\n"
        "│\n"
        "│ 🚀 STATUS: Fetching from Iwara...\n"
        "└────────────────────────────────────┘"
        "</code>"
    )


def _notify_progress(msg_id: int | None, filename: str, done_mb: float,
                     total_mb: float, pct: float, speed_mbs: float) -> None:
    filled = int(pct / 100 * 15)
    bar    = "▰" * filled + "▱" * (15 - filled)
    _tg_edit(msg_id,
        "<code>"
        "┌─── 🛰️ [ IWARA.DOWNLOAD.ACTIVE ] ───┐\n"
        "│\n"
        f"│ 📂 FILE   : {filename}\n"
        f"│ 📊 PROG   : [{bar}] {pct:.0f}%\n"
        f"│            {done_mb:.1f} / {total_mb:.1f} MB\n"
        f"│ ⚡ SPEED  : {speed_mbs:.2f} MB/s\n"
        "│\n"
        "└────────────────────────────────────┘"
        "</code>"
    )


def _notify_done(msg_id: int | None, filename: str, size_mb: float) -> None:
    _tg_edit(msg_id,
        "<code>"
        "┌─── ✅ [ IWARA.DOWNLOAD.COMPLETE ] ─┐\n"
        "│\n"
        f"│ 📂 FILE : {filename}\n"
        f"│ 📦 SIZE : {size_mb:.1f} MB\n"
        "│\n"
        "│ 🔄 STATUS: Transferring to Encoder...\n"
        "└────────────────────────────────────┘"
        "</code>"
    )


def _notify_error(msg_id: int | None, reason: str) -> None:
    text = (
        "<code>"
        "┌─── ❌ [ IWARA.DOWNLOAD.FAILED ] ───┐\n"
        "│\n"
        f"│ ❌ ERROR: {reason[:120]}\n"
        "│\n"
        "│ 🛠️ STATUS: Downlink terminated.\n"
        "└────────────────────────────────────┘"
        "</code>"
    )
    if msg_id:
        _tg_edit(msg_id, text)
    else:
        _tg_send(text)


# ─── Utility Helpers ──────────────────────────────────────────────────────────

def extract_video_id(url: str) -> str | None:
    """
    Extract video ID from any iwara URL format.
      https://www.iwara.tv/video/laOLZIqV5BJA5W
      https://www.iwara.tv/video/laOLZIqV5BJA5W/some-title
      https://iwara.ai/video/laOLZIqV5BJA5W
    """
    m = re.search(r"/video/([A-Za-z0-9]+)", url)
    return m.group(1) if m else None


def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", name).strip()


def format_time(seconds: float) -> str:
    if seconds < 0 or seconds == float("inf"):
        return "??:??:??"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


# ─── API Helpers ──────────────────────────────────────────────────────────────

def _build_headers(token: str | None = None, site: str = "www.iwara.tv") -> dict:
    h = {
        "Accept":       "application/json",
        "Content-Type": "application/json",
        "User-Agent":   (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
        ),
        "Origin":  f"https://{site}",
        "Referer": f"https://{site}/",
        "X-Site":  site,
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _fetch_json(url: str, headers: dict) -> dict | list:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            return json.loads(r.read().decode())
    except HTTPError as e:
        body = e.read().decode()
        # 301 → differentSite redirect (iwara.tv vs iwara.ai)
        if e.code == 301:
            try:
                err      = json.loads(body)
                site_map = {"iwara_ai": "www.iwara.ai", "iwara": "www.iwara.tv"}
                new_site = site_map.get(err.get("siteId")) if err.get("message") == "errors.differentSite" else None
                if new_site:
                    headers["X-Site"] = new_site
                    headers["Origin"]  = f"https://{new_site}"
                    headers["Referer"] = f"https://{new_site}/"
                    retry = urllib.request.Request(url, headers=headers)
                    with urllib.request.urlopen(retry, timeout=TIMEOUT) as r2:
                        return json.loads(r2.read().decode())
            except Exception:
                pass
        raise RuntimeError(f"HTTP {e.code}: {body[:200]}")


def _get_remote_size(url: str, headers: dict) -> int:
    """Try HEAD then Range GET to find the total file size."""
    cdn_h = {k: headers[k] for k in ("User-Agent", "Referer", "Origin") if k in headers}
    try:
        req = urllib.request.Request(url, headers=cdn_h, method="HEAD")
        with urllib.request.urlopen(req, timeout=5) as r:
            ln = r.getheader("Content-Length")
            if ln:
                return int(ln)
    except Exception:
        pass
    try:
        req = urllib.request.Request(url, headers=cdn_h, method="GET")
        req.add_header("Range", "bytes=0-0")
        with urllib.request.urlopen(req, timeout=5) as r:
            cr = r.getheader("Content-Range")
            if cr and "/" in cr:
                return int(cr.split("/")[-1])
    except Exception:
        pass
    return 0


# ─── Core Resolver ────────────────────────────────────────────────────────────

def resolve_download(
    video_id: str,
    token:    str | None = None,
    quality:  str | None = None,
) -> tuple[str, str, str]:
    """
    Resolve the best download URL for a given Iwara video ID.
    Returns (download_url, quality_label, video_title).
    Raises RuntimeError on any failure.
    """
    headers = _build_headers(token)

    # 1. Fetch video metadata
    data      = _fetch_json(f"https://api.iwara.tv/video/{video_id}", headers)
    file_url  = data.get("fileUrl")
    raw_title = data.get("title", "Unknown_Video")

    if not file_url:
        raise RuntimeError(
            "'fileUrl' missing — R-18 content requires IWARA_TOKEN to be set."
        )
    if file_url.startswith("//"):
        file_url = "https:" + file_url

    # 2. Compute X-Version hash
    parsed       = urlparse(file_url)
    last_segment = parsed.path.strip("/").split("/")[-1]
    expires      = parse_qs(parsed.query).get("expires", [""])[0]
    x_version    = hashlib.sha1(
        f"{last_segment}_{expires}_{_IWARA_SECRET}".encode()
    ).hexdigest()

    # 3. Fetch quality list
    dl_name      = quote(f"Iwara - {raw_title} [{video_id}].mp4")
    final_url    = f"{file_url}&download={dl_name}"
    file_headers = {**headers, "X-Version": x_version}

    req = urllib.request.Request(final_url, headers=file_headers)
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        files_data = json.loads(r.read().decode())

    # 4. Quality selection: prefer explicit match → "Source" → first entry
    available: list[tuple[str, str]] = []
    for src in files_data:
        q   = str(src.get("name") or src.get("resolution") or "Unknown")
        url = (src.get("src") or {}).get("download") or (src.get("src") or {}).get("view")
        if not url:
            continue
        if url.startswith("//"):
            url = "https:" + url
        available.append((q, url))

    if not available:
        raise RuntimeError("No valid download links found in API response.")

    target = None
    if quality:
        target = next((x for x in available if x[0].lower() == quality.lower()), None)
    if not target:
        target = next((x for x in available if x[0] == "Source"), available[0])

    quality_label, download_url = target
    return download_url, quality_label, raw_title


# ─── Pipeline Entry Point ─────────────────────────────────────────────────────

def download(url: str) -> None:
    """
    Called by download.py.
    Downloads the video to source.mkv, writes tg_fname.txt + iwara_source.txt.
    Exits with code 1 on any fatal error.
    """
    print("🎌 Iwara URL detected → iwara.py", flush=True)

    video_id = extract_video_id(url)
    if not video_id:
        _notify_error(None, f"Could not extract video ID from URL: {url}")
        print(f"❌ Could not extract video ID from: {url}", flush=True)
        sys.exit(1)

    token   = IWARA_TOKEN or None
    quality = IWARA_QUALITY or None

    print(
        f"▶ video_id={video_id}  "
        f"quality={quality or 'Source (auto)'}  "
        f"token={'yes' if token else 'no'}",
        flush=True,
    )

    # Resolve URL
    print("▶ Resolving download URL...", flush=True)
    try:
        download_url, quality_label, raw_title = resolve_download(video_id, token, quality)
    except RuntimeError as e:
        _notify_error(None, str(e))
        print(f"❌ {e}", flush=True)
        sys.exit(1)

    safe_title  = sanitize_filename(raw_title)
    tg_filename = f"{safe_title} [{video_id}] [{quality_label}].mp4"

    print(f"  Title  : {raw_title}", flush=True)
    print(f"  Quality: {quality_label}", flush=True)
    print(f"  File   : {tg_filename}", flush=True)

    total_size = _get_remote_size(download_url, _build_headers(token))
    total_mb   = total_size / 1_048_576 if total_size > 0 else 0
    if total_size:
        print(f"  Size   : {total_mb:.1f} MB", flush=True)
    else:
        print("  Size   : unknown", flush=True)

    msg_id = _notify_start(tg_filename)

    # Download headers (CDN-side, no JSON Content-Type)
    dl_headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.iwara.tv/",
        "Origin":  "https://www.iwara.tv",
    }
    if token:
        dl_headers["Authorization"] = f"Bearer {token}"

    req        = urllib.request.Request(download_url, headers=dl_headers)
    start_t    = time.time()
    downloaded = 0
    last_tg    = -1.0
    last_con   = time.time()

    try:
        with urllib.request.urlopen(req, timeout=60) as response, \
             open("source.mkv", "wb") as out:

            while True:
                chunk = response.read(CHUNK_SIZE)
                if not chunk:
                    break
                out.write(chunk)
                downloaded += len(chunk)

                now     = time.time()
                elapsed = now - start_t
                speed   = downloaded / elapsed if elapsed > 0 else 0
                done_mb = downloaded / 1_048_576
                pct     = (downloaded / total_size * 100) if total_size > 0 else 0

                # Console progress every 5 s
                if now - last_con >= 5:
                    print(
                        f"\r  {done_mb:.1f} MB  {pct:.0f}%  "
                        f"{speed / 1_048_576:.2f} MB/s",
                        end="", flush=True,
                    )
                    last_con = now

                # Telegram progress every 15 s
                if last_tg < 0 or now - last_tg >= 15:
                    _notify_progress(
                        msg_id, tg_filename, done_mb, total_mb,
                        pct, speed / 1_048_576,
                    )
                    last_tg = now

    except KeyboardInterrupt:
        print("\n❌ Download cancelled by user.", flush=True)
        _notify_error(msg_id, "Download cancelled by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Download failed: {e}", flush=True)
        _notify_error(msg_id, str(e))
        sys.exit(1)

    print(flush=True)
    size_mb = os.path.getsize("source.mkv") / 1_048_576
    print(f"✅ Downloaded → source.mkv  ({size_mb:.1f} MB)", flush=True)

    # Pipeline artefacts
    with open("tg_fname.txt", "w", encoding="utf-8") as f:
        f.write(tg_filename)
    print(f"📝 tg_fname.txt → {tg_filename}", flush=True)

    # Marker: tells main.py to skip anitopy auto-rename (filename isn't a standard
    # anime release name, so anitopy would produce garbage).
    with open("iwara_source.txt", "w", encoding="utf-8") as f:
        f.write(video_id)

    _notify_done(msg_id, tg_filename, size_mb)
    print(f"✅ iwara.app download complete → source.mkv  ({size_mb:.1f} MB)", flush=True)


# ─── Standalone CLI ───────────────────────────────────────────────────────────

def _cli() -> None:
    if len(sys.argv) < 2:
        print(f"{B}Usage:{R}  python3 iwara.py <url> [quality]")
        print(f"  {DIM}https://www.iwara.tv/video/laOLZIqV5BJA5W{R}")
        print(f"  {DIM}https://www.iwara.tv/video/laOLZIqV5BJA5W 1080{R}")
        sys.exit(1)

    url     = sys.argv[1]
    quality = sys.argv[2] if len(sys.argv) > 2 else None
    token   = IWARA_TOKEN or None

    video_id = extract_video_id(url)
    if not video_id:
        print(f"\n  {RD}Could not extract video ID from URL.{R}")
        sys.exit(1)

    print(f"\n{CY}{B}  ┌────────────────────────────────────┐")
    print(f"  │       Iwara.tv  Downloader         │")
    print(f"  └────────────────────────────────────┘{R}\n")
    print(f"  {DIM}Video ID: {video_id}{R}")

    try:
        download_url, quality_label, raw_title = resolve_download(video_id, token, quality)
    except RuntimeError as e:
        print(f"\n  {RD}Error: {e}{R}")
        sys.exit(1)

    safe_title = sanitize_filename(raw_title)
    out_name   = f"[{quality_label}] {safe_title} [{video_id}].mp4"
    total_size = _get_remote_size(download_url, _build_headers(token))
    total_mb   = total_size / 1_048_576 if total_size > 0 else 0

    print(f"\n  {B}Title:{R}   {CY}{raw_title}{R}")
    print(f"  {B}Quality:{R} {quality_label}")
    print(f"  {B}Output:{R}  {out_name}")
    if total_size:
        print(f"  {B}Size:{R}    ~{total_mb:.1f} MB\n")
    else:
        print(f"  {B}Size:{R}    unknown\n")

    dl_headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.iwara.tv/",
    }

    req        = urllib.request.Request(download_url, headers=dl_headers)
    start_t    = time.time()
    downloaded = 0

    print("\n" * 14)  # make room for the TUI box

    try:
        with urllib.request.urlopen(req, timeout=60) as response, \
             open(out_name, "wb") as out:

            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                out.write(chunk)
                downloaded += len(chunk)

                elapsed  = time.time() - start_t
                speed    = downloaded / elapsed if elapsed > 0 else 0
                done_mb  = downloaded / 1_048_576
                pct      = (downloaded / total_size * 100) if total_size > 0 else 0
                eta      = ((total_size - downloaded) / speed
                            if speed > 0 and total_size > 0 else float("inf"))
                speed_mb = speed / 1_048_576
                filled   = int(15 * pct / 100) if total_size else 0
                bar      = "▰" * filled + "▱" * (15 - filled)
                size_str = f"{total_mb:.2f} MB" if total_size else "Unknown"

                ui = (
                    f"\033[15A\r┌─── 🛰️ [ IWARA.DOWNLOAD.PROCESS ] ───┐\n"
                    f"│                                    \n"
                    f"│ 📂 FILE: {out_name[:55].ljust(55)}\n"
                    f"│ ⚡ SPEED: {speed_mb:.2f} MB/s\n"
                    f"│ ⏳ TIME: {format_time(elapsed)} / ETA: {format_time(eta)}\n"
                    f"│ 🕒 DONE: {done_mb:.2f} MB / {size_str}\n"
                    f"│                                    \n"
                    f"│ 📊 PROG: [{bar}] {pct:.1f}% \n"
                    f"│                                    \n"
                    f"│ 🛠️ SETTINGS: HTTPS | Chunk: 8KB | IPv4\n"
                    f"│ 🎞️ VIDEO: Iwara ({quality_label}) | H.264 | MP4\n"
                    f"│ 🔊 AUDIO: AAC @ 128k\n"
                    f"│ 📦 SIZE: {done_mb:.2f} MB → ~{total_mb:.2f} MB est\n"
                    f"│                                    \n"
                    f"└────────────────────────────────────┘"
                )
                sys.stdout.write(ui)
                sys.stdout.flush()

        # Final 100% frame
        elapsed  = time.time() - start_t
        done_mb  = downloaded / 1_048_576
        speed_mb = (downloaded / elapsed) / 1_048_576 if elapsed > 0 else 0
        ui = (
            f"\033[15A\r┌─── 🛰️ [ IWARA.DOWNLOAD.PROCESS ] ───┐\n"
            f"│                                    \n"
            f"│ 📂 FILE: {out_name[:55].ljust(55)}\n"
            f"│ ⚡ SPEED: {speed_mb:.2f} MB/s (Average)\n"
            f"│ ⏳ TIME: {format_time(elapsed)} / ETA: 00:00:00\n"
            f"│ 🕒 DONE: {done_mb:.2f} MB / {done_mb:.2f} MB\n"
            f"│                                    \n"
            f"│ 📊 PROG: [{'▰' * 15}] 100.0% \n"
            f"│                                    \n"
            f"│ 🛠️ SETTINGS: HTTPS | Chunk: 8KB | IPv4\n"
            f"│ 🎞️ VIDEO: Iwara ({quality_label}) | H.264 | MP4\n"
            f"│ 🔊 AUDIO: AAC @ 128k\n"
            f"│ 📦 SIZE: {done_mb:.2f} MB → ~{done_mb:.2f} MB est\n"
            f"│                                    \n"
            f"└────────────────────────────────────┘\n\n"
            f"{GR}[✓] Download Complete!{R}\n"
        )
        sys.stdout.write(ui)
        sys.stdout.flush()

    except KeyboardInterrupt:
        print(f"\n\n{RD}[!] Download Cancelled by User.{R}")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n{RD}[!] Download Failed: {e}{R}")
        sys.exit(1)


# ─── Entry Point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        _cli()
    except KeyboardInterrupt:
        print(f"\n\n  {RD}Terminated by user.{R}")
        sys.exit(1)
