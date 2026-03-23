"""
anibd.py
anibd.app episode downloader - integrated pipeline module.

Handles URLs of two forms:
  • Anime page  : https://anibd.app/407332/
  • Direct play : https://anibd.app/playid/407332/?server=10&slug=01

Outputs (pipeline-standard):
  source.mkv    — downloaded episode (always this name)
  tg_fname.txt  — human-readable filename for the encode step

Episode selection (in order of priority):
  1. slug in a direct play URL  (e.g. slug=03 → episode 3)
  2. EPISODE env-var             (set by the workflow)
  3. defaults to episode 1

Telegram progress notifications mirror the style of tg_handler.py.
"""

from __future__ import annotations

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

# ─── Env ─────────────────────────────────────────────────────────────────────

BOT_TOKEN  = os.environ.get("TG_BOT_TOKEN", "").strip()
CHAT_ID    = os.environ.get("TG_CHAT_ID",   "").strip()
RUN_NUMBER = os.environ.get("GITHUB_RUN_NUMBER", "?")

# Episode to download when URL is an anime page (not a direct play link)
_EPISODE_ENV = os.environ.get("EPISODE", "1").strip()
_SEASON_ENV  = os.environ.get("SEASON",  "1").strip()

# ─── HTTP Helper ─────────────────────────────────────────────────────────────

def _fetch(url: str, headers: dict | None = None, binary: bool = False):
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Mozilla/5.0")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read() if binary else r.read().decode("utf-8", errors="ignore")
    except Exception:
        return None

# ─── Telegram Notifications ───────────────────────────────────────────────────

def _tg_api(endpoint: str, payload: dict) -> dict | None:
    """
    Call a Telegram Bot API endpoint.  Returns parsed JSON or None on failure.
    stdout/stderr suppressed — never leaks into the pipeline log.
    """
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
            check=False,
            timeout=10,
            capture_output=True,
        )
        return json.loads(result.stdout.decode()) if result.stdout else None
    except Exception:
        return None


def _tg_send_new(text: str) -> int | None:
    """Send a new message and return its message_id (or None on failure)."""
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
    """Edit an existing message in-place (best-effort, never raises)."""
    if not msg_id:
        return
    _tg_api("editMessageText", {
        "chat_id":    CHAT_ID,
        "message_id": msg_id,
        "text":       text,
        "parse_mode": "HTML",
    })


def _notify_start(filename: str) -> int | None:
    """Send the initial status message and return its message_id."""
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

# ─── URL Parsers ──────────────────────────────────────────────────────────────

def _parse_anime_url(url: str) -> str | None:
    """Extract post_id from https://anibd.app/<post_id>/"""
    m = re.search(r'anibd\.app/(\d+)', url)
    return m.group(1) if m else None


def _parse_playid_url(url: str) -> tuple[str, int, str] | None:
    """
    Parse https://anibd.app/playid/<post_id>/?server=<n>&slug=<s>
    Returns (post_id, server_api_id, slug) or None.
    """
    m = re.search(r"playid/(\d+)/?\??server=(\d+)&slug=(\w+)", url)
    if m:
        return m.group(1), int(m.group(2)), m.group(3)
    return None

# ─── Page Scrapers ────────────────────────────────────────────────────────────

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

# ─── M3U8 Resolver ───────────────────────────────────────────────────────────

def _get_iframe_urls(post_id: str, server_api_id: int, slug: str) -> list[str]:
    play_page = (
        f"https://anibd.app/playid/{post_id}/"
        f"?server={server_api_id}&slug={slug}"
    )
    html = _fetch(play_page, headers={"Referer": f"https://anibd.app/{post_id}/"})
    if not html:
        return []

    urls = re.findall(r'data-src=["\']([^"\']+playeng[^"\']+)["\']', html)
    m = re.search(r'<iframe[^>]+src=["\']([^"\']+playeng[^"\']+)["\']', html)
    if m and m.group(1) not in urls:
        urls.insert(0, m.group(1))

    return [u.replace("&#038;", "&").replace("&amp;", "&") for u in urls]


def _fetch_m3u8_info(link: str, post_id: str, server_api_id: int = 10) -> dict | None:
    """
    Resolve the M3U8 URL and return segment list + metadata.
    Tries multiple server buttons in order (SR → SB → …).
    Returns None if all servers fail.
    """
    m = re.search(r'(?:uc|ww)(\d+)$', link)
    slug = f"{int(m.group(1)):02d}" if m else "01"

    player_urls = _get_iframe_urls(post_id, server_api_id, slug)
    if not player_urls:
        print(f"  ⚠ No player URLs found on play page", flush=True)
        return None

    server_labels = ["SR", "SB", "S3", "S4"]

    for i, player_url in enumerate(player_urls):
        label = server_labels[i] if i < len(server_labels) else f"S{i+1}"

        html = _fetch(player_url, headers={
            "Referer": f"https://anibd.app/playid/{post_id}/",
            "Origin":  "https://anibd.app",
        })
        if not html:
            print(f"  ⚠ Server {label}: player unreachable, trying next...", flush=True)
            continue

        all_m3u8 = re.findall(r"""["']([^"'\s]*\.m3u8[^"'\s]*)["']""", html)
        if not all_m3u8:
            print(f"  ⚠ Server {label}: no m3u8 found in player page", flush=True)
            continue

        for m3u8_raw in all_m3u8:
            m3u8_url = urljoin(player_url, m3u8_raw)

            data = _fetch(m3u8_url, headers={
                "Referer": player_url,
                "Origin":  "https://playeng.animeapps.top",
            })
            if not data or not data.strip().startswith("#EXTM3U"):
                print(f"  ⚠ Server {label}: m3u8 invalid ({m3u8_url})", flush=True)
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
                        "Origin":  "https://playeng.animeapps.top",
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
                print(f"  ⚠ Server {label}: m3u8 has no segments", flush=True)
                continue

            durations = re.findall(r'#EXTINF:([\d.]+)', data)
            total_dur = sum(float(d) for d in durations)

            print(f"  ✓ Server {label}: resolved M3U8 ({len(segments)} segments)", flush=True)
            return {
                "url":        m3u8_url,
                "server":     label,
                "player_url": player_url,
                "segments":   segments,
                "count":      len(segments),
                "duration":   total_dur,
                "raw":        data,
            }

        print(f"  ⚠ Server {label}: all m3u8 candidates failed, trying next...", flush=True)

    return None

# ─── Segment Downloader ───────────────────────────────────────────────────────

def _download_segments(info: dict, output_mkv: str, ep_num: int,
                       tg_filename: str, msg_id: int | None) -> bool:
    """
    Download all HLS segments → concatenate → mux into output_mkv via ffmpeg.
    Edits the single status message for progress updates.
    Cleans up temp files on success or failure.
    """
    seg_dir = Path(f".tmp_anibd_ep{ep_num:02d}")
    raw_ts  = Path(f".tmp_anibd_ep{ep_num:02d}.ts")
    seg_dir.mkdir(parents=True, exist_ok=True)

    segments  = info["segments"]
    total     = len(segments)
    bytes_dl  = 0
    start_t   = time.time()
    last_tg   = -1            # -1 so first segment fires immediately

    print(f"  ▶ Downloading {total} segments...", flush=True)

    try:
        for i, seg_url in enumerate(segments):
            seg_file = seg_dir / f"seg-{i:03d}.ts"
            if seg_file.exists() and seg_file.stat().st_size > 0:
                # already downloaded (resume)
                bytes_dl += seg_file.stat().st_size
            else:
                data = _fetch(seg_url, binary=True)
                if data:
                    seg_file.write_bytes(data)
                    bytes_dl += len(data)
                else:
                    print(f"\n  ⚠ Segment {i} failed to download", flush=True)

            # Console progress every 25 segments
            if i % 25 == 0 or i == total - 1:
                pct   = (i + 1) / total * 100
                print(f"\r  Segments: {i+1}/{total}  ({pct:.0f}%)", end="", flush=True)

            # TG progress every 30 seconds
            now = time.time()
            elapsed = now - start_t
            if (last_tg == -1 or now - last_tg >= 15) and elapsed > 0:
                speed_mbs = bytes_dl / elapsed / 1_048_576
                _notify_progress(msg_id, tg_filename, ep_num, i + 1, total, speed_mbs)
                last_tg = now

        print(flush=True)

        # Concatenate segments
        print("  ▶ Concatenating segments...", flush=True)
        with open(raw_ts, "wb") as out:
            for i in range(total):
                sf = seg_dir / f"seg-{i:03d}.ts"
                if sf.exists():
                    out.write(sf.read_bytes())

        # Mux to MKV via ffmpeg (stream copy — no re-encode)
        print("  ▶ Muxing to MKV...", flush=True)
        result = subprocess.run(
            [
                "ffmpeg", "-loglevel", "error",
                "-i",     str(raw_ts),
                "-c",     "copy",
                output_mkv, "-y",
            ],
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

# ─── Public Entry Point ───────────────────────────────────────────────────────

def download(url: str) -> None:
    """
    Main entry point called by download.py.

    Resolves the episode, downloads segments, muxes to source.mkv,
    and writes tg_fname.txt.  Exits with code 1 on any fatal error.
    """
    print(f"🎌 anibd.app URL detected → anibd.py", flush=True)

    # ── 1. Parse URL ──────────────────────────────────────────────────────
    playid_params = _parse_playid_url(url)

    if playid_params:
        post_id, server_api_id, slug = playid_params
        # Derive episode number from slug (e.g. "03" → 3)
        try:
            ep_num = int(slug.lstrip("0") or "1")
        except ValueError:
            ep_num = 1
        print(f"▶ Direct play URL  post_id={post_id}  ep={ep_num}  slug={slug}", flush=True)
    else:
        post_id = _parse_anime_url(url)
        if not post_id:
            _notify_error(None, "Could not extract Post ID from URL.")
            print("❌ Could not extract Post ID from anibd.app URL.", flush=True)
            sys.exit(1)
        server_api_id = 10
        slug          = None
        # Episode from env-var (set by the workflow bridge)
        try:
            ep_num = max(1, int(_EPISODE_ENV))
        except ValueError:
            ep_num = 1
        print(f"▶ Anime page URL  post_id={post_id}  ep={ep_num} (from EPISODE env)", flush=True)

    # ── 2. Fetch page metadata ────────────────────────────────────────────
    print(f"▶ Fetching anime metadata...", flush=True)
    title  = _get_anime_title(post_id)
    ep_id  = _get_ep_id(post_id)

    if not ep_id:
        _notify_error(None, "Could not find EP_ID on anibd.app page.")
        print("❌ Could not find EP_ID on anibd.app page.", flush=True)
        sys.exit(1)

    print(f"  Anime : {title}", flush=True)
    print(f"  EP_ID : {ep_id}", flush=True)

    # ── 3. Fetch episode list ─────────────────────────────────────────────
    print(f"▶ Fetching episode list...", flush=True)
    servers = _fetch_episode_list(ep_id)

    if not servers:
        _notify_error(None, "No episodes found from anibd.app API.")
        print("❌ No episodes found from anibd.app API.", flush=True)
        sys.exit(1)

    server_data   = servers[0]
    server_api_id = server_data.get("id", server_api_id)
    episodes      = server_data.get("server_data", [])
    total_eps     = len(episodes)

    print(f"  Server : {server_data.get('server_name', '?')}  |  Total eps: {total_eps}", flush=True)

    if ep_num < 1 or ep_num > total_eps:
        msg = f"Episode {ep_num} out of range (1–{total_eps})."
        _notify_error(None, msg)
        print(f"❌ {msg}", flush=True)
        sys.exit(1)

    ep_entry = episodes[ep_num - 1]
    link     = ep_entry["link"]
    print(f"  Episode {ep_num:02d} link: {link}", flush=True)

    # ── 4. Resolve M3U8 ──────────────────────────────────────────────────
    safe_title  = re.sub(r'[<>:"/\\|?*]', '', title).strip()
    try:
        season_num = max(1, int(_SEASON_ENV))
    except ValueError:
        season_num = 1
    tg_filename = f"[S{season_num:02d}-E{ep_num:02d}] {safe_title} [1080p].mkv"

    print(f"▶ Resolving M3U8...", flush=True)
    msg_id = _notify_start(tg_filename)

    info = _fetch_m3u8_info(link, post_id, server_api_id)

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

    # ── 5. Download ───────────────────────────────────────────────────────
    ok = _download_segments(info, "source.mkv", ep_num, tg_filename, msg_id)

    if not ok:
        _notify_error(msg_id, f"Segment download or mux failed for episode {ep_num}.")
        print("❌ Segment download / mux failed.", flush=True)
        sys.exit(1)

    # ── 6. Write tg_fname.txt (pipeline standard) ─────────────────────────
    with open("tg_fname.txt", "w", encoding="utf-8") as f:
        f.write(tg_filename)
    # No anibd_source.txt flag — main.py will run the normal post-encode
    # rename pipeline (ffprobe → detect_audio_type → build_output_name)
    # to append the correct [Dual] / [Sub] / etc. audio tag.
    print(f"📝 tg_fname.txt → {tg_filename}", flush=True)

    size_mb = Path("source.mkv").stat().st_size / 1_048_576
    _notify_done(msg_id, tg_filename, size_mb)
    print(f"✅ anibd.app download complete → source.mkv  ({size_mb:.1f} MB)", flush=True)


# ─── Standalone usage (for testing outside the pipeline) ─────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 anibd.py <anibd.app URL>")
        sys.exit(1)
    download(sys.argv[1])
