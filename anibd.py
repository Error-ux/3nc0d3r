#!/usr/bin/env python3
"""
anibd.py
anibd.app episode downloader — integrated pipeline module + standalone CLI.

Pipeline usage (called by download.py):
    from anibd import download
    download(url)

CLI usage (Termux / standalone):
    python3 anibd.py                   # interactive CLI
    python3 anibd.py <url>             # pipeline mode (single episode)

Outputs (pipeline-standard):
    source.mkv      — downloaded episode
    tg_fname.txt    — human-readable filename for the encode step
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urljoin

# ─── Configuration ───────────────────────────────────────────────────────────
MAX_DOWNLOAD_WORKERS = 10
TIMEOUT = 20

# ─── ANSI Colors ─────────────────────────────────────────────────────────────
R   = "\033[0m"
B   = "\033[1m"
DIM = "\033[2m"
CY  = "\033[96m"
GR  = "\033[92m"
YL  = "\033[93m"
RD  = "\033[91m"

def clear():
    print("\033[2J\033[H", end="")

def banner():
    print(f"""
{CY}{B}  ┌─────────────────────────────────────┐
  │        anibd.app  Downloader        │
  └─────────────────────────────────────┘{R}
""")

def check_dependencies():
    """Ensure ffmpeg is installed before starting."""
    if shutil.which("ffmpeg") is None:
        print(f"\n  {RD}Error: 'ffmpeg' is not installed or not in PATH.{R}")
        print(f"  {DIM}Please install ffmpeg to mux the downloaded segments.{R}")
        sys.exit(1)

# ─── Telegram Env ────────────────────────────────────────────────────────────
BOT_TOKEN  = os.environ.get("TG_BOT_TOKEN", "").strip()
CHAT_ID    = os.environ.get("TG_CHAT_ID",   "").strip()
RUN_NUMBER = os.environ.get("GITHUB_RUN_NUMBER", "?")

# Episode/season set by the workflow bridge; defaults to 1
_EPISODE_ENV = os.environ.get("EPISODE", "1").strip()
_SEASON_ENV  = os.environ.get("SEASON",  "1").strip()

# ─── HTTP Helper ─────────────────────────────────────────────────────────────
def _fetch(url: str, headers: dict | None = None,
           binary: bool = False, as_json: bool = False):
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64)")
    req.add_header("Accept", "*/*")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            data = r.read()
            if binary:
                return data
            text = data.decode("utf-8", errors="ignore")
            if as_json:
                return json.loads(text)
            return text
    except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError):
        return None
    except Exception:
        return None

# ─── Telegram Notifications ──────────────────────────────────────────────────
def _tg_api(endpoint: str, payload: dict) -> dict | None:
    if not BOT_TOKEN or not CHAT_ID:
        return None
    try:
        data = json.dumps(payload).encode()
        result = subprocess.run(
            [
                "curl", "-s", "-X", "POST",
                f"https://api.telegram.org/bot{BOT_TOKEN}/{endpoint}",
                "-H", "Content-Type: application/json",
                "-d", data.decode(),
            ],
            check=False, timeout=10, capture_output=True,
        )
        return json.loads(result.stdout.decode()) if result.stdout else None
    except Exception:
        return None


def _tg_send_new(text: str) -> int | None:
    resp = _tg_api("sendMessage", {
        "chat_id":    CHAT_ID,
        "text":       text,
        "parse_mode": "HTML",
    })
    try:
        return resp["result"]["message_id"]
    except Exception:
        return None


def _tg_edit(msg_id: int, text: str) -> None:
    if not msg_id:
        return
    _tg_api("editMessageText", {
        "chat_id":    CHAT_ID,
        "message_id": msg_id,
        "text":       text,
        "parse_mode": "HTML",
    })


def _notify_start(filename: str) -> int | None:
    return _tg_send_new(
        "<code>"
        "┌─── 📥 [ ANIBD.DOWNLOADER ] ────────┐\n"
        "│\n"
        f"│ 📂 FILE : {filename}\n"
        f"│ 🔢 RUN  : #{RUN_NUMBER}\n"
        "│\n"
        "│ 🚀 STATUS: Acquiring from anibd.app...\n"
        "└────────────────────────────────────┘"
        "</code>"
    )


def _notify_progress(msg_id: int, filename: str, ep: int,
                     seg_done: int, seg_total: int, speed_mbs: float) -> None:
    pct    = (seg_done / seg_total * 100) if seg_total else 0
    filled = int(pct / 100 * 15)
    bar    = "▰" * filled + "▱" * (15 - filled)
    _tg_edit(msg_id,
        "<code>"
        "┌─── 🛰️ [ ANIBD.DOWNLOAD.ACTIVE ] ───┐\n"
        "│\n"
        f"│ 📂 FILE   : {filename}\n"
        f"│ 🎬 EP     : {ep:02d}\n"
        f"│ 📊 SEGS   : [{bar}] {pct:.0f}%\n"
        f"│            {seg_done}/{seg_total}\n"
        f"│ ⚡ SPEED  : {speed_mbs:.2f} MB/s\n"
        "│\n"
        "└────────────────────────────────────┘"
        "</code>"
    )


def _notify_done(msg_id: int, filename: str, size_mb: float) -> None:
    _tg_edit(msg_id,
        "<code>"
        "┌─── ✅ [ ANIBD.DOWNLOAD.COMPLETE ] ─┐\n"
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
        "┌─── ❌ [ ANIBD.DOWNLOAD.FAILED ] ───┐\n"
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
        _tg_send_new(text)

# ─── URL Parsers ─────────────────────────────────────────────────────────────
def parse_input_url(url: str) -> tuple[str | None, int, str | None]:
    """
    Parse any anibd.app URL.
    Returns (post_id, server_api_id, slug).  slug is None for base anime URLs.
    """
    m = re.search(r"playid/(\d+)/\??server=(\d+)&slug=(\w+)", url)
    if m:
        return m.group(1), int(m.group(2)), m.group(3)
    m = re.search(r'anibd\.app/(\d+)', url)
    if m:
        return m.group(1), 10, None
    return None, None, None


def _parse_playid_url(url: str) -> tuple[str, int, str] | None:
    """Returns (post_id, server_api_id, slug) for direct play URLs, else None."""
    m = re.search(r"playid/(\d+)/?\??server=(\d+)&slug=(\w+)", url)
    return (m.group(1), int(m.group(2)), m.group(3)) if m else None

# ─── Page Scrapers ───────────────────────────────────────────────────────────
def _get_anime_title(post_id: str) -> str:
    html = _fetch(f"https://anibd.app/{post_id}/",
                  headers={"Referer": "https://anibd.app/"})
    if not html:
        return "Unknown Anime"
    m = re.search(r'<title>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
    if not m:
        return "Unknown Anime"
    title = m.group(1)
    noise_patterns = [
        r'\s*[-|]\s*Uncensored\s*[-|/].*$',
        r'\s*BD\s*\(.*?\)',
        r'\s*Blu[-\s]?ray.*$',
        r'\s*1080[Pp].*$',
        r'\s*[Aa]nime\s+[Ee]nglish\s+[Ss]ubbed.*$',
        r'\s*[Ee]nglish\s+[Ss]ub.*$',
        r'\s*[Ee]pisode\s+\d+.*$',
        r'\s*-\s*\d+\s*EP\s*-.*$',
    ]
    for pat in noise_patterns:
        title = re.sub(pat, '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s+', ' ', title).strip()
    title = re.sub(r'[<>:"/\\|?*]', '', title).strip(' .-')
    return title or "Unknown Anime"


def _get_ep_id(post_id: str) -> str | None:
    html = _fetch(f"https://anibd.app/{post_id}/",
                  headers={"Referer": "https://anibd.app/"})
    if not html:
        return None
    m = re.search(r'const\s+EP_ID\s*=\s*["\']?(\d+)["\']?', html)
    return m.group(1) if m else None


def _fetch_episode_list(ep_id: str) -> list:
    url  = f"https://epeng.animeapps.top/api2.php?epid={ep_id}"
    data = _fetch(url, headers={"Referer": "https://anibd.app/"})
    if not data:
        return []
    try:
        return json.loads(data)
    except Exception:
        return []

# ─── M3U8 Resolver (multi-server, robust) ────────────────────────────────────
def _get_player_urls(post_id: str, server_api_id: int, slug: str) -> tuple[list[str], str | None]:
    """Return (player_urls, episode_title) from the play page."""
    play_page = (
        f"https://anibd.app/playid/{post_id}/"
        f"?server={server_api_id}&slug={slug}"
    )
    html = _fetch(play_page, headers={
        "Referer":                   f"https://anibd.app/{post_id}/",
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.5",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    if not html:
        return [], None

    # Scrape episode title from <h1 class="episode-title">
    ep_title = None
    tm = re.search(r'<h1[^>]*class=["\']episode-title["\'][^>]*>([^<]+)</h1>', html, re.IGNORECASE)
    if tm:
        ep_title = tm.group(1).strip()

    # Collect all playeng URLs: data-src buttons first, then iframe fallback
    urls = re.findall(r'data-src=["\']([^"\']+playeng[^"\']+)["\']', html)
    urls += re.findall(r'<iframe[^>]+src=["\']([^"\']+playeng[^"\']+)["\']', html)

    seen, result = set(), []
    for u in urls:
        u = u.replace("&#038;", "&").replace("&amp;", "&")
        if u not in seen:
            seen.add(u)
            result.append(u)
    return result, ep_title


def _fetch_m3u8_info(link: str, post_id: str, server_api_id: int = 10,
                     ep_num: int | None = None) -> dict | None:
    """
    Resolve the M3U8 URL and return segment list + metadata.
    Tries multiple server buttons in order (SR → SB → S3 → S4).
    Returns None if all servers fail.
    """
    # Use ep_num as slug (episode number), not the server suffix from link
    if ep_num is not None:
        slug = f"{ep_num:02d}"
    else:
        m    = re.search(r'(?:uc|ww)(\d+)$', link)
        slug = f"{int(m.group(1)):02d}" if m else "01"

    player_urls, ep_title = _get_player_urls(post_id, server_api_id, slug)
    if not player_urls:
        print(f"  {YL}⚠ No player URLs found on play page{R}", flush=True)
        return None

    server_labels = ["SR", "SB", "S3", "S4"]

    for i, player_url in enumerate(player_urls):
        label = server_labels[i] if i < len(server_labels) else f"S{i+1}"

        html = _fetch(player_url, headers={
            "Referer": f"https://anibd.app/playid/{post_id}/",
            "Origin":  "https://anibd.app",
        })
        if not html:
            print(f"  {YL}⚠ Server {label}: player unreachable, trying next...{R}", flush=True)
            continue

        all_m3u8 = re.findall(r"""["']([^"'\s]*\.m3u8[^"'\s]*)["']""", html)
        if not all_m3u8:
            print(f"  {YL}⚠ Server {label}: no m3u8 found in player page{R}", flush=True)
            continue

        # Derive origin dynamically from the working player URL
        from urllib.parse import urlparse
        parsed = urlparse(player_url)
        origin = f"{parsed.scheme}://{parsed.netloc}"

        for m3u8_raw in all_m3u8:
            m3u8_url = urljoin(player_url, m3u8_raw)

            data = _fetch(m3u8_url, headers={
                "Referer": player_url,
                "Origin":  origin,
            })
            if not data or not data.strip().startswith("#EXTM3U"):
                print(f"  {YL}⚠ Server {label}: m3u8 invalid ({m3u8_url}){R}", flush=True)
                continue

            segments = [l.strip() for l in data.splitlines()
                        if l.strip().startswith("https")]

            # Master playlist → follow first sub-playlist (highest quality)
            if not segments:
                sub_playlists = re.findall(r'^(?!#)(\S+\.m3u8)', data, re.MULTILINE)
                if sub_playlists:
                    sub_url  = urljoin(m3u8_url, sub_playlists[0])
                    sub_data = _fetch(sub_url, headers={
                        "Referer": player_url,
                        "Origin":  origin,
                    })
                    if sub_data and sub_data.strip().startswith("#EXTM3U"):
                        data     = sub_data
                        m3u8_url = sub_url
                        segments = [l.strip() for l in data.splitlines()
                                    if l.strip().startswith("https")]
                        if not segments:
                            seg_base = sub_url.rsplit('/', 1)[0] + '/'
                            rel_segs = [l.strip() for l in data.splitlines()
                                        if l.strip() and not l.strip().startswith('#')]
                            segments = [urljoin(seg_base, s) for s in rel_segs if s]

            if not segments:
                print(f"  {YL}⚠ Server {label}: m3u8 has no segments{R}", flush=True)
                continue

            durations = re.findall(r'#EXTINF:([\d.]+)', data)
            total_dur = sum(float(d) for d in durations)

            print(f"  {GR}✓ Server {label}: resolved M3U8 ({len(segments)} segments){R}",
                  flush=True)
            if ep_title:
                print(f"  {DIM}Title: {ep_title}{R}", flush=True)
            return {
                "url":        m3u8_url,
                "server":     label,
                "player_url": player_url,
                "segments":   segments,
                "count":      len(segments),
                "duration":   total_dur,
                "raw":        data,
                "title":      ep_title,
            }

        print(f"  {YL}⚠ Server {label}: all m3u8 candidates failed, trying next...{R}",
              flush=True)

    return None

# ─── Formatting ──────────────────────────────────────────────────────────────
def fmt_duration(secs):
    m, s = divmod(int(secs), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

def fmt_size(mb):
    return f"{mb/1024:.1f} GB" if mb >= 1024 else f"{mb:.0f} MB"

def progress(current, total, prefix="", width=35):
    pct    = current / total
    filled = int(width * pct)
    bar    = "█" * filled + "░" * (width - filled)
    print(f"\r  {prefix} [{CY}{bar}{R}] {current}/{total}", end="", flush=True)

# ─── Episode Selection Parser ────────────────────────────────────────────────
def parse_selection(selection: str, max_ep: int) -> list[int]:
    """Parse '1,4-6,10' or 'all' into a sorted list of valid episode numbers."""
    if selection.strip().lower() == "all":
        return list(range(1, max_ep + 1))
    result = set()
    for part in selection.split(","):
        part = part.strip()
        if "-" in part:
            try:
                a, b = map(int, part.split("-", 1))
                result.update(i for i in range(a, b + 1) if 1 <= i <= max_ep)
            except ValueError:
                pass
        else:
            try:
                i = int(part)
                if 1 <= i <= max_ep:
                    result.add(i)
            except ValueError:
                pass
    return sorted(result)

# ─── Parallel Segment Worker ─────────────────────────────────────────────────
def download_segment(seg_url: str, seg_file: Path) -> bool:
    """Download a single segment. Skips if already complete (resume support)."""
    if seg_file.exists() and seg_file.stat().st_size > 0:
        return True
    headers = {
        "Referer": "https://playeng.animeapps.top/",
        "Origin": "https://playeng.animeapps.top"
    }
    data = _fetch(seg_url, binary=True)
    if data:
        seg_file.write_bytes(data)
        return True
    return False

# ─── CLI Downloader (parallel, MP4 output) ───────────────────────────────────
def download_episode(ep_num: int, info: dict, output_path: Path) -> bool:
    """
    CLI mode: parallel download → concat → mux to MP4.
    Shows a progress bar. No Telegram notifications.
    """
    seg_dir = output_path.parent / f".tmp_ep{ep_num:02d}"
    raw_ts  = output_path.parent / f".tmp_ep{ep_num:02d}.ts"
    seg_dir.mkdir(parents=True, exist_ok=True)

    segments  = info["segments"]
    total     = len(segments)
    completed = 0

    try:
        print(f"\n  {YL}Downloading segments (parallel)...{R}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:
            future_to_idx = {
                executor.submit(download_segment, seg_url, seg_dir / f"seg-{i:03d}.ts"): i
                for i, seg_url in enumerate(segments)
            }
            for future in concurrent.futures.as_completed(future_to_idx):
                completed += 1
                progress(completed, total, "Segments")
                if not future.result():
                    print(f"\n  {RD}Warning: Failed segment {future_to_idx[future]}{R}")

        print(f"\n  {YL}Concatenating...{R}")
        with open(raw_ts, "wb") as out:
            for i in range(total):
                sf = seg_dir / f"seg-{i:03d}.ts"
                if sf.exists():
                    out.write(sf.read_bytes())

        print(f"  {YL}Muxing to MP4...{R}")
        result = subprocess.run(
            ["ffmpeg", "-loglevel", "error", "-i", str(raw_ts),
             "-c", "copy", str(output_path), "-y"],
            capture_output=True,
        )
        if result.returncode != 0:
            print(f"  {RD}ffmpeg error: {result.stderr.decode()}{R}")
            return False

        size_mb = output_path.stat().st_size / 1024 / 1024
        print(f"  {GR}✓ Saved: {output_path.name} ({size_mb:.1f} MB){R}")
        return True

    except KeyboardInterrupt:
        print(f"\n  {RD}Download interrupted by user.{R}")
        sys.exit(1)

    finally:
        shutil.rmtree(seg_dir, ignore_errors=True)
        raw_ts.unlink(missing_ok=True)

# ─── Pipeline Downloader (parallel, MKV output, TG notifications) ────────────
def _download_segments_pipeline(info: dict, output_mkv: str, ep_num: int,
                                 tg_filename: str, msg_id: int | None) -> bool:
    """
    Pipeline mode: parallel download → concat → mux to MKV.
    Sends Telegram progress updates every 15 seconds while downloading.
    """
    seg_dir = Path(f".tmp_anibd_ep{ep_num:02d}")
    raw_ts  = Path(f".tmp_anibd_ep{ep_num:02d}.ts")
    seg_dir.mkdir(parents=True, exist_ok=True)

    segments  = info["segments"]
    total     = len(segments)
    completed = 0
    bytes_dl  = 0
    start_t   = time.time()
    last_tg   = -1.0

    print(f"  ▶ Downloading {total} segments (parallel)...", flush=True)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:
            future_to_idx = {
                executor.submit(download_segment, seg_url, seg_dir / f"seg-{i:03d}.ts"): i
                for i, seg_url in enumerate(segments)
            }
            for future in concurrent.futures.as_completed(future_to_idx):
                i = future_to_idx[future]
                completed += 1

                sf = seg_dir / f"seg-{i:03d}.ts"
                if sf.exists():
                    bytes_dl += sf.stat().st_size

                if not future.result():
                    print(f"\n  ⚠ Segment {i} failed", flush=True)

                # Console progress every 25 completions
                if completed % 25 == 0 or completed == total:
                    pct = completed / total * 100
                    print(f"\r  Segments: {completed}/{total}  ({pct:.0f}%)",
                          end="", flush=True)

                # Telegram progress every 15 seconds
                now     = time.time()
                elapsed = now - start_t
                if (last_tg < 0 or now - last_tg >= 15) and elapsed > 0:
                    speed_mbs = bytes_dl / elapsed / 1_048_576
                    _notify_progress(msg_id, tg_filename, ep_num,
                                     completed, total, speed_mbs)
                    last_tg = now

        print(flush=True)

        print("  ▶ Concatenating segments...", flush=True)
        with open(raw_ts, "wb") as out:
            for i in range(total):
                sf = seg_dir / f"seg-{i:03d}.ts"
                if sf.exists():
                    out.write(sf.read_bytes())

        print("  ▶ Muxing to MKV...", flush=True)
        result = subprocess.run(
            ["ffmpeg", "-loglevel", "error", "-i", str(raw_ts),
             "-c", "copy", output_mkv, "-y"],
            capture_output=True,
        )
        if result.returncode != 0:
            err = result.stderr.decode(errors="replace")
            print(f"  ❌ ffmpeg error: {err}", flush=True)
            return False

        size_mb = Path(output_mkv).stat().st_size / 1_048_576
        print(f"  ✅ Muxed → {output_mkv}  ({size_mb:.1f} MB)", flush=True)
        return True

    finally:
        shutil.rmtree(seg_dir, ignore_errors=True)
        raw_ts.unlink(missing_ok=True)

# ─── Pipeline Entry Point ─────────────────────────────────────────────────────
def download(url: str) -> None:
    """
    Called by download.py.
    Resolves the episode, downloads segments, muxes to source.mkv,
    and writes tg_fname.txt. Exits with code 1 on any fatal error.
    """
    check_dependencies()
    print("🎌 anibd.app URL detected → anibd.py", flush=True)

    # ── 1. Parse URL ─────────────────────────────────────────────────────────
    playid_params = _parse_playid_url(url)
    if playid_params:
        post_id, server_api_id, slug = playid_params
        try:
            ep_num = int(slug.lstrip("0") or "1")
        except ValueError:
            ep_num = 1
        print(f"▶ Direct play URL  post_id={post_id}  ep={ep_num}  slug={slug}", flush=True)
    else:
        post_id, server_api_id, _ = parse_input_url(url)
        if not post_id:
            _notify_error(None, "Could not extract Post ID from URL.")
            print("❌ Could not extract Post ID from anibd.app URL.", flush=True)
            sys.exit(1)
        server_api_id = 10
        try:
            ep_num = max(1, int(_EPISODE_ENV))
        except ValueError:
            ep_num = 1
        print(f"▶ Anime page URL  post_id={post_id}  ep={ep_num} (from EPISODE env)",
              flush=True)

    # ── 2. Fetch metadata ────────────────────────────────────────────────────
    print("▶ Fetching anime metadata...", flush=True)
    title = _get_anime_title(post_id)
    ep_id = _get_ep_id(post_id)

    if not ep_id:
        _notify_error(None, "Could not find EP_ID on anibd.app page.")
        print("❌ Could not find EP_ID on anibd.app page.", flush=True)
        sys.exit(1)

    print(f"  Anime : {title}", flush=True)
    print(f"  EP_ID : {ep_id}", flush=True)

    # ── 3. Fetch episode list ────────────────────────────────────────────────
    print("▶ Fetching episode list...", flush=True)
    servers = _fetch_episode_list(ep_id)

    if not servers:
        _notify_error(None, "No episodes found from anibd.app API.")
        print("❌ No episodes found from anibd.app API.", flush=True)
        sys.exit(1)

    server_data   = servers[0]
    server_api_id = server_data.get("id", server_api_id)
    episodes      = server_data.get("server_data", [])
    total_eps     = len(episodes)

    print(f"  Server : {server_data.get('server_name', '?')}  |  Total eps: {total_eps}",
          flush=True)

    if ep_num < 1 or ep_num > total_eps:
        msg = f"Episode {ep_num} out of range (1-{total_eps})."
        _notify_error(None, msg)
        print(f"❌ {msg}", flush=True)
        sys.exit(1)

    ep_entry = episodes[ep_num - 1]
    link     = ep_entry["link"]
    print(f"  Episode {ep_num:02d} link: {link}", flush=True)

    # ── 4. Resolve M3U8 ──────────────────────────────────────────────────────
    safe_title = re.sub(r'[<>:"/\\|?*]', '', title).strip()
    try:
        season_num = max(1, int(_SEASON_ENV))
    except ValueError:
        season_num = 1
    tg_filename = f"[S{season_num:02d}-E{ep_num:02d}] {safe_title} [1080p].mkv"

    print("▶ Resolving M3U8...", flush=True)
    msg_id = _notify_start(tg_filename)
    info   = _fetch_m3u8_info(link, post_id, server_api_id, ep_num=ep_num)

    if not info:
        _notify_error(msg_id, f"All servers failed for episode {ep_num}.")
        print(f"❌ All servers failed for episode {ep_num}.", flush=True)
        sys.exit(1)

    print(
        f"  Server {info['server']}  |  "
        f"{info['count']} segments  |  "
        f"{info['duration']:.0f}s",
        flush=True,
    )

    # ── 5. Download (parallel) ───────────────────────────────────────────────
    ok = _download_segments_pipeline(info, "source.mkv", ep_num, tg_filename, msg_id)

    if not ok:
        _notify_error(msg_id, f"Segment download or mux failed for episode {ep_num}.")
        print("❌ Segment download / mux failed.", flush=True)
        sys.exit(1)

    # ── 6. Write tg_fname.txt (pipeline standard) ────────────────────────────
    with open("tg_fname.txt", "w", encoding="utf-8") as f:
        f.write(tg_filename)
    print(f"📝 tg_fname.txt → {tg_filename}", flush=True)

    size_mb = Path("source.mkv").stat().st_size / 1_048_576
    _notify_done(msg_id, tg_filename, size_mb)
    print(f"✅ anibd.app download complete → source.mkv  ({size_mb:.1f} MB)", flush=True)

# ─── Interactive CLI ──────────────────────────────────────────────────────────
def main():
    clear()
    banner()
    check_dependencies()

    print(f"{B}Paste URL:{R}")
    print(f"  {DIM}Anime page : https://anibd.app/407332/{R}")
    print(f"  {DIM}Play page  : https://anibd.app/playid/407332/?server=10&slug=01{R}\n")

    try:
        url = input(f"  {CY}URL:{R} ").strip()
    except KeyboardInterrupt:
        sys.exit(0)

    post_id, server_api_id, start_slug = parse_input_url(url)

    if not post_id:
        print(f"\n  {RD}Could not extract Post ID from URL.{R}")
        sys.exit(1)

    print(f"\n  {DIM}Fetching anime info...{R}")
    title = _get_anime_title(post_id)
    ep_id = _get_ep_id(post_id)

    if not ep_id:
        print(f"\n  {RD}Could not find EP_ID on page.{R}")
        sys.exit(1)

    print(f"\n  {B}Anime:{R} {CY}{title}{R}")
    print(f"  {B}Post ID:{R} {post_id}  |  {B}EP ID:{R} {ep_id}")
    print(f"\n  {DIM}Fetching episode list...{R}")

    servers = _fetch_episode_list(ep_id)
    if not servers:
        print(f"\n  {RD}No episodes found.{R}")
        sys.exit(1)

    server_data = servers[0]
    server_name = server_data.get("server_name", "S-sub")
    episodes    = server_data.get("server_data", [])
    total_eps   = len(episodes)

    if not start_slug:
        server_api_id = server_data.get("id", server_api_id)

    print(f"  {B}Server:{R} {server_name}  |  {B}Episodes:{R} {total_eps}")
    print(f"\n  {DIM}Available: 1 - {total_eps}{R}")

    # Episode selection
    if start_slug:
        default_sel = str(int(start_slug.lstrip("0") or "1"))
        print(f"\n  {GR}Auto-selected episode {default_sel} from URL{R}")
        sel = default_sel
    else:
        print(f"\n{B}Select episodes:{R}")
        print(f"  {DIM}Examples: all  |  1  |  1,3,5  |  4-8  |  1,4-6,10{R}\n")
        try:
            sel = input(f"  {CY}Episodes:{R} ").strip()
        except KeyboardInterrupt:
            sys.exit(0)

    selected = parse_selection(sel, total_eps)
    if not selected:
        print(f"\n  {RD}No valid episodes selected.{R}")
        sys.exit(1)

    # Fetch info for all selected episodes
    print(f"\n  {DIM}Fetching episode info ({len(selected)} episodes)...{R}\n")
    ep_infos      = {}
    total_size_mb = 0
    total_dur     = 0

    print(f"  {'EP':<6} {'Server':<8} {'Segments':<12} {'Duration':<12} {'Est. Size'}")
    print(f"  {'─'*6} {'─'*8} {'─'*12} {'─'*12} {'─'*10}")

    for idx in selected:
        ep   = episodes[idx - 1]
        link = ep["link"]
        info = _fetch_m3u8_info(link, post_id, server_api_id, ep_num=idx)

        if not info:
            print(f"  {RD}E{idx:02d}    All servers failed — skipping{R}")
            continue

        first_seg = _fetch(info["segments"][0], binary=True)
        seg_size  = (len(first_seg) / 1024 / 1024) if first_seg else 1.2
        est_mb    = seg_size * info["count"]

        total_size_mb += est_mb
        total_dur     += info["duration"]
        ep_infos[idx]  = {"info": info, "est_mb": est_mb}

        print(f"  {GR}E{idx:02d}{R}    "
              f"{info['server']:<8} "
              f"{info['count']:<12} "
              f"{fmt_duration(info['duration']):<12} "
              f"~{fmt_size(est_mb)}")

    if not ep_infos:
        print(f"\n  {RD}No episodes could be fetched.{R}")
        sys.exit(1)

    print(f"\n  {'─'*44}")
    print(f"  {B}Total:{R} {len(ep_infos)} ep  |  "
          f"~{fmt_size(total_size_mb)}  |  {fmt_duration(total_dur)}")

    out_dir = Path.home() / "Downloads" / "Anime" / title
    print(f"\n  {B}Output:{R} {CY}{out_dir}/{R}")

    try:
        print(f"\n{B}Start download?{R} {DIM}(y/n){R} ", end="")
        if input().strip().lower() != "y":
            print(f"\n  {YL}Cancelled.{R}")
            sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(0)

    out_dir.mkdir(parents=True, exist_ok=True)

    success = 0
    for idx, data in ep_infos.items():
        info        = data["info"]
        ep_name     = (info.get("title") or "").strip()
        ep_label    = f" - {ep_name}" if ep_name and not ep_name.isdigit() else ""
        filename    = f"[E{idx:02d}]{ep_label} {title} [1080p].mp4"
        filename    = re.sub(r'[<>:"/\\|?*]', '', filename)
        output_path = out_dir / filename

        print(f"\n{B}━━━ Episode {idx:02d} ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{R}")
        print(f"  {DIM}{filename}{R}")

        if output_path.exists():
            print(f"  {YL}Already exists, skipping.{R}")
            success += 1
            continue

        if download_episode(idx, info, output_path):
            success += 1

    print(f"\n{GR}{B}━━━ Complete: {success}/{len(ep_infos)} episodes downloaded ━━━{R}")
    print(f"  {CY}{out_dir}/{R}\n")

# ─── Entry Point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            download(sys.argv[1])
        else:
            main()
    except KeyboardInterrupt:
        print(f"\n\n  {RD}Script terminated by user.{R}")
        sys.exit(1)
