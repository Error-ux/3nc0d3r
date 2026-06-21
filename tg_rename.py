"""
tg_rename.py — Download from Telegram, rename using structured format, upload back.

No re-encoding. Just:
  1. Download source file from Telegram
  2. ffprobe → detect quality, audio tracks, subtitle tracks
  3. Build structured output filename via rename.py logic
  4. mkvmerge remux (preserve metadata, apply new filename)
  5. Upload renamed file back to Telegram with a full track report

Environment variables (set by rename.yml):
  TG_API_ID, TG_API_HASH, TG_BOT_TOKEN, TG_CHAT_ID
  VIDEO_URL          — tg_file:<id>|<name>  or  https://t.me/...
  ANIME_NAME         — e.g. "Medalist"
  SEASON             — e.g. "2"
  EPISODE            — e.g. "7"
  AUDIO_TYPE         — Auto | Sub | Dual | Tri | Multi
  CONTENT_TYPE       — Anime | Donghua | Hentai | HMV | AMV | custom
  SUB_TRACKS         — user-supplied subtitle labels e.g. "English, Arabic"
  AUDIO_TRACKS       — user-supplied audio labels   e.g. "Japanese, English (Dub)"
  GITHUB_RUN_NUMBER  — for lane resolution
"""

import asyncio
import json
import os
import subprocess
import sys
import time
import traceback

from pyrogram import Client, enums
from pyrogram.errors import FloodWait

from utils.rename import (
    get_track_info, detect_audio_type, detect_quality,
    build_output_name, format_track_report
)
from utils.ui import get_download_ui, upload_progress, format_time
import utils.ui as _ui

# ── ENV ───────────────────────────────────────────────────────────────────────

import config

API_ID       = config.API_ID
API_HASH     = config.API_HASH
BOT_TOKEN    = config.BOT_TOKEN
CHAT_ID      = config.CHAT_ID
VIDEO_URL    = os.getenv("VIDEO_URL",        "").strip()

ANIME_NAME   = os.getenv("ANIME_NAME",      "").strip()
SEASON       = os.getenv("SEASON",          "1").strip()
EPISODE      = os.getenv("EPISODE",         "1").strip()
AUDIO_TYPE   = os.getenv("AUDIO_TYPE",      "Auto").strip()
CONTENT_TYPE = os.getenv("CONTENT_TYPE",    "Anime").strip()
SUB_TRACKS   = os.getenv("SUB_TRACKS",      "").strip()
AUDIO_TRACKS = os.getenv("AUDIO_TRACKS",    "").strip()
# Resolution override — when set (e.g. "720"), use it instead of probing source height.
# Passed from the bridge when the user explicitly chose a resolution in the config UI.
RES_CHOICE   = os.getenv("RES_CHOICE",      "").strip()

_RES_CHOICE_MAP = {
    "360":  "360p",
    "480":  "480p",
    "720":  "720p",
    "1080": "1080p",
    "2160": "4K",
}

SOURCE_FILE  = "./rename_source.mkv"
THUMBNAIL    = "./rename_thumb.jpg"

# Fraction of total duration to grab the thumbnail from (0.20 = 20% in — past OP)
THUMB_AT     = 0.20

# ── LANE RESOLUTION ───────────────────────────────────────────────────────────

def resolve_lane() -> str:
    """Convert run number to unique alphabetic lane: 1→A, 26→Z, 27→AA, 28→AB, ..."""
    run_number = int(os.getenv("GITHUB_RUN_NUMBER", "0"))
    if run_number <= 0:
        return "A"
    result = ""
    n = run_number
    while n > 0:
        n -= 1
        result = chr(ord('A') + (n % 26)) + result
        n //= 26
    return result

# ── TELEGRAM HELPERS ──────────────────────────────────────────────────────────

async def tg_edit(app, chat_id, msg_id, text, reply_markup=None):
    try:
        kwargs = dict(parse_mode=enums.ParseMode.HTML)
        if reply_markup:
            kwargs["reply_markup"] = reply_markup
            
        import utils.tg_utils as tg_utils
        if tg_utils._active_tg_state and tg_utils._active_tg_state.get("app") is not app:
            app = tg_utils._active_tg_state["app"]
            status = tg_utils._active_tg_state.get("status")
            if status:
                msg_id = status.id
                
        await app.edit_message_text(chat_id, msg_id, text, **kwargs)
    except Exception as e:
        if "FloodWait" in type(e).__name__ or getattr(e, "value", None) is not None:
            wait_val = getattr(e, "value", 10)
            await asyncio.sleep(wait_val + 1)
        else:
            pass

async def dl_progress(current, total, app, chat_id, status_msg, start_time):
    if total <= 0: return
    now = time.time()
    # If progress updates are muted in Telegram channels, bypass intermediate edits
    mute_progress = os.getenv("TG_MUTE_PROGRESS", "true").lower() == "true"
    if mute_progress and current != total:
        return

    if not hasattr(dl_progress, "last_update"): dl_progress.last_update = 0
    interval = int(os.getenv("TG_PROGRESS_INTERVAL", "15"))
    if now - dl_progress.last_update < interval and current != total:
        return
    dl_progress.last_update = now

    pct = (current / total) * 100
    elapsed    = time.time() - start_time
    speed_mb   = (current / elapsed / 1_048_576) if elapsed > 0 else 0
    size_mb    = total / 1_048_576
    eta        = (total - current) / (current / elapsed) if current > 0 and elapsed > 0 else 0
    await tg_edit(app, chat_id, status_msg.id,
                  get_download_ui(pct, speed_mb, size_mb, elapsed, eta))

# ── DOWNLOAD FROM TELEGRAM ────────────────────────────────────────────────────

async def download_from_tg(app, status_msg) -> str:
    """Download the source file. Returns the original filename."""
    start   = time.time()
    dl_progress.last_pct = -1

    if VIDEO_URL.startswith("tg_file:"):
        raw = VIDEO_URL.replace("tg_file:", "")
        file_id, orig_name = (raw.split("|", 1) if "|" in raw else (raw, "source.mkv"))
        dl_result = await app.download_media(
            message=file_id.strip(), file_name=SOURCE_FILE,
            progress=dl_progress, progress_args=(app, CHAT_ID, status_msg, start)
        )
        if dl_result and dl_result != SOURCE_FILE and os.path.exists(dl_result):
            try:
                if os.path.exists(SOURCE_FILE):
                    os.remove(SOURCE_FILE)
                os.rename(dl_result, SOURCE_FILE)
            except Exception as e:
                print(f"WARNING: Failed to rename {dl_result} to {SOURCE_FILE}: {e}")
        return orig_name

    if "t.me/" in VIDEO_URL:
        parts = VIDEO_URL.rstrip("/").split("/")
        msg_id = int(parts[-1].split("?")[0])
        target_chat = int(f"-100{parts[-2]}") if len(parts) >= 4 and parts[-3] == "c" else parts[-2]
        msg  = await app.get_messages(target_chat, msg_id)
        media = getattr(msg, "video", None) or getattr(msg, "document", None)
        orig_name = getattr(media, "file_name", "source.mkv") if media else "source.mkv"
        dl_result = await app.download_media(
            msg, file_name=SOURCE_FILE,
            progress=dl_progress, progress_args=(app, CHAT_ID, status_msg, start)
        )
        if dl_result and dl_result != SOURCE_FILE and os.path.exists(dl_result):
            try:
                if os.path.exists(SOURCE_FILE):
                    os.remove(SOURCE_FILE)
                os.rename(dl_result, SOURCE_FILE)
            except Exception as e:
                print(f"WARNING: Failed to rename {dl_result} to {SOURCE_FILE}: {e}")
        return orig_name

    raise ValueError(f"Unsupported URL format: {VIDEO_URL}")

# ── PROBE + RENAME ────────────────────────────────────────────────────────────

def probe_and_build_name() -> tuple[str, str, list, list]:
    """
    ffprobe the source, build the structured filename.
    Returns (output_filename, audio_type_label, audio_tracks, sub_tracks).
    """
    audio_tracks, sub_tracks = get_track_info(SOURCE_FILE)

    # Audio type — use override unless "Auto"
    if AUDIO_TYPE and AUDIO_TYPE.lower() != "auto":
        audio_type_label = AUDIO_TYPE.strip().capitalize()
    else:
        audio_type_label = detect_audio_type(audio_tracks, sub_tracks)

    # Quality — read from actual video height via ffprobe
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-select_streams", "v:0",
        os.path.abspath(SOURCE_FILE)
    ]
    import json
    try:
        raw  = subprocess.check_output(cmd, stderr=subprocess.PIPE).decode()
        data = json.loads(raw)
        height = int(data["streams"][0].get("height", 1080))
    except subprocess.CalledProcessError as e:
        print(f"[rename] ffprobe failed (rc={e.returncode}): {e.stderr.decode().strip()}")
        height = 1080
    except Exception as e:
        print(f"[rename] ffprobe error: {e}")
        height = 1080

    # If the user explicitly chose a resolution in the config UI, honour it;
    # otherwise derive quality from the actual source height.
    if RES_CHOICE and RES_CHOICE in _RES_CHOICE_MAP:
        quality = _RES_CHOICE_MAP[RES_CHOICE]
        print(f"[rename] Using user-set quality override: {quality} (res_choice={RES_CHOICE})")
    else:
        quality = detect_quality(height)
        print(f"[rename] Auto-detected quality from height {height}px: {quality}")

    filename = build_output_name(
        anime_name   = ANIME_NAME or "Unknown",
        season       = SEASON,
        episode      = EPISODE,
        quality      = quality,
        audio_type   = audio_type_label,
        content_type = CONTENT_TYPE or "Anime",
        ext          = "mkv",
    )
    return filename, audio_type_label, audio_tracks, sub_tracks

def capture_thumbnail(source: str) -> bool:
    """
    Grab a single frame at THUMB_AT % of total duration.
    Scales to 1280px wide (keeps AR), saves as JPEG.
    Returns True on success.
    """
    # Get duration via ffprobe
    try:
        cmd = ["ffprobe", "-v", "quiet", "-print_format", "json",
               "-show_format", os.path.abspath(source)]
        raw  = subprocess.check_output(cmd, stderr=subprocess.PIPE).decode()
        duration = float(json.loads(raw)["format"].get("duration", 0))
    except Exception as e:
        print(f"[thumb] ffprobe duration failed: {e}")
        duration = 0

    ts = max(duration * THUMB_AT, 5.0) if duration > 10 else 5.0
    hms = f"{int(ts//3600):02d}:{int((ts%3600)//60):02d}:{ts%60:06.3f}"

    cmd = [
        "ffmpeg", "-ss", hms, "-i", os.path.abspath(source),
        "-frames:v", "1",
        "-vf", "scale=1280:-1",
        "-q:v", "3",
        os.path.abspath(THUMBNAIL), "-y"
    ]
    ret = subprocess.run(cmd, capture_output=True)
    ok  = os.path.exists(THUMBNAIL) and os.path.getsize(THUMBNAIL) > 0
    if not ok:
        print(f"[thumb] capture failed (rc={ret.returncode}): {ret.stderr.decode()[:200]}")
    else:
        print(f"[thumb] captured at {hms} → {os.path.getsize(THUMBNAIL)//1024}KB")
    return ok


# ── REMUX (apply new name + clean metadata) ───────────────────────────────────

def remux(output_name: str) -> bool:
    """mkvmerge: copy all streams into a new container with the structured filename."""
    src = os.path.abspath(SOURCE_FILE)
    dst = os.path.abspath(output_name)
    tmp = os.path.abspath("_remux_tmp.mkv")

    if not os.path.exists(src):
        raise FileNotFoundError(f"Source file missing before remux: {src}")

    if os.path.exists(tmp): os.remove(tmp)

    try:
        # Check if mkvmerge exists
        if subprocess.run(["which", "mkvmerge"], stdout=subprocess.DEVNULL).returncode == 0:
            ret = subprocess.run(["mkvmerge", "-o", tmp, src], capture_output=True)
            if os.path.exists(tmp) and os.path.getsize(tmp) > 0:
                if os.path.exists(src): os.remove(src)
                os.rename(tmp, dst)
                return True
            print(f"[remux] mkvmerge failed (rc={ret.returncode}), falling back to rename")
        else:
            print("[remux] Warning: mkvmerge not found, skipping remux.")
    except Exception as e:
        print(f"[remux] Error: {e}. Falling back to rename.")

    if os.path.exists(tmp): os.remove(tmp)
    os.rename(src, dst)
    return False

# ── MAIN ──────────────────────────────────────────────────────────────────────

async def main():
    from utils.tg_utils import _resolve_session_names
    session_names = _resolve_session_names()
    print(f"DEBUG: Prioritized session names to try: {session_names}")

    app = None
    flood_waits = {}
    chosen_session = None

    for session_name in session_names:
        print(f"DEBUG: Trying session: {session_name}...")
        try:
            candidate = Client(
                session_name,
                api_id=API_ID,
                api_hash=API_HASH,
                bot_token=BOT_TOKEN or None,
                max_concurrent_transmissions=16
            )
            await candidate.start()
            app = candidate
            chosen_session = session_name
            print(f"DEBUG: TG auth OK with session: {session_name}")
            break
        except Exception as e:
            if "FloodWait" in type(e).__name__ or getattr(e, "value", None) is not None:
                wait_val = getattr(e, "value", 10)
                flood_waits[session_name] = wait_val
                print(f"DEBUG: FloodWait {wait_val}s on '{session_name}' — trying next...")
                continue
            print(f"DEBUG: TG auth error on '{session_name}': {e} — trying next...")
            continue

    if app is None and flood_waits:
        best_session = min(flood_waits, key=flood_waits.get)
        wait_secs = flood_waits[best_session]
        attempt = 0
        while True:
            attempt += 1
            print(f"All sessions flooded. Sleeping {wait_secs}s for '{best_session}' (attempt {attempt})...")
            await asyncio.sleep(wait_secs + 5)
            try:
                candidate = Client(
                    best_session,
                    api_id=API_ID,
                    api_hash=API_HASH,
                    bot_token=BOT_TOKEN or None,
                    max_concurrent_transmissions=16
                )
                await candidate.start()
                app = candidate
                chosen_session = best_session
                print(f"DEBUG: TG auth OK (post-flood attempt {attempt}): {best_session}")
                break
            except Exception as e:
                if "FloodWait" in type(e).__name__ or getattr(e, "value", None) is not None:
                    wait_secs = getattr(e, "value", 10)
                    print(f"DEBUG: Another FloodWait: {wait_secs}s — retrying...")
                    continue
                print(f"DEBUG: TG auth failed on post-flood attempt {attempt}: {e}")
                break

    if app is None:
        print("❌ Could not authorize with Telegram. No usable session found.")
        sys.exit(1)

    import utils.tg_utils as tg_utils
    tg_utils._active_tg_state = {
        "app": app,
        "session_name": chosen_session,
        "label": "RENAME"
    }

    try:
        from utils.tg_utils import run_with_flood_retry
        status = await run_with_flood_retry(
            app.send_message,
            CHAT_ID,
            "<code>┌─── 🏷️  [ RENAME.MISSION ] ──────────┐\n"
            "│                                    \n"
            "│ 📡 Establishing Telegram downlink...\n"
            "│                                    \n"
            "└────────────────────────────────────┘</code>",
            parse_mode=enums.ParseMode.HTML
        )
        tg_utils._active_tg_state["status"] = status

        # ── 1. DOWNLOAD ────────────────────────────────────────────────────
        await tg_edit(app, CHAT_ID, status.id,
            "<code>┌─── 📥 [ DOWNLOADING ] ──────────────┐\n"
            "│ Fetching file from Telegram...     \n"
            "└────────────────────────────────────┘</code>")

        try:
            orig_name = await download_from_tg(app, status)
        except Exception as e:
            await tg_edit(app, CHAT_ID, status.id,
                f"<b>❌ DOWNLOAD FAILED:</b>\n<code>{e}</code>")
            sys.exit(1)

        # Verify the file actually landed
        if not os.path.exists(SOURCE_FILE) or os.path.getsize(SOURCE_FILE) == 0:
            await tg_edit(app, CHAT_ID, status.id,
                "<b>❌ DOWNLOAD ERROR:</b> File not found after download.\n"
                f"<code>Expected: {SOURCE_FILE}</code>")
            sys.exit(1)

        dl_time = time.time() - start_total
        print(f"[rename] Downloaded in {dl_time:.1f}s → {SOURCE_FILE}")

        # ── 2. PROBE + BUILD NAME ──────────────────────────────────────────
        await tg_edit(app, CHAT_ID, status.id,
            "<code>┌─── 🔬 [ PROBING ] ──────────────────┐\n"
            "│ Reading track info...              \n"
            "└────────────────────────────────────┘</code>")

        if not ANIME_NAME:
            await tg_edit(app, CHAT_ID, status.id,
                "<b>⚠️ ANIME_NAME not set — aborting rename.</b>")
            sys.exit(1)

        output_name, audio_type_label, audio_tracks, sub_tracks = probe_and_build_name()
        print(f"[rename] Output filename: {output_name}")

        # ── 3. REMUX ───────────────────────────────────────────────────────
        await tg_edit(app, CHAT_ID, status.id,
            "<code>┌─── 🛠️  [ REMUXING ] ────────────────┐\n"
            f"│ {output_name[:34]}\n"
            "│ Repackaging streams...             \n"
            "└────────────────────────────────────┘</code>")

        remux_ok = remux(output_name)
        if not remux_ok:
            await tg_edit(app, CHAT_ID, status.id,
                "<b>⚠️ Remux failed — uploading raw source file.</b>")

        # ── 4. THUMBNAIL ───────────────────────────────────────────────────
        await tg_edit(app, CHAT_ID, status.id,
            "<code>┌─── 🖼️  [ THUMBNAIL ] ──────────────┐\n"
            "│ Capturing frame preview...         \n"
            "└────────────────────────────────────┘</code>")

        has_thumb = capture_thumbnail(output_name)

        # ── 5. UPLOAD ──────────────────────────────────────────────────────
        final_size = os.path.getsize(output_name) / 1_048_576
        await tg_edit(app, CHAT_ID, status.id,
            "<b>🚀 [ UPLINK ] Transmitting renamed file...</b>")

        # Build track report
        track_report = format_track_report(audio_tracks, sub_tracks)
        user_notes   = ""
        if SUB_TRACKS:
            user_notes += f"\n🔤 <b>SUB LABELS:</b>  <code>{SUB_TRACKS}</code>"
        if AUDIO_TRACKS:
            user_notes += f"\n🔊 <b>AUDIO LABELS:</b> <code>{AUDIO_TRACKS}</code>"

        total_time = time.time() - start_total
        report = (
            f"✅ <b>RENAME COMPLETE</b>\n\n"
            f"📄 <b>ORIGINAL:</b> <code>{orig_name[:60]}</code>\n"
            f"🏷️  <b>RENAMED TO:</b> <code>{output_name}</code>\n\n"
            f"📦 <b>SIZE:</b> <code>{final_size:.2f} MB</code>\n"
            f"⏱ <b>TIME:</b> <code>{format_time(total_time)}</code>\n\n"
            f"📂 <b>TYPE:</b> {CONTENT_TYPE or 'Anime'}  |  "
            f"🔈 <b>AUDIO:</b> {audio_type_label}\n\n"
            f"{track_report}"
            f"{user_notes}"
        )

        _ui.last_up_pct = -1; _ui.last_up_update = 0; _ui.up_start_time = 0

        # Send phase notification to private bot/chat
        try:
            from utils.tg_simple import notify_private
            notify_private(f"🚀 <b>[ UPLINK STARTED ]</b>\n📄 <b>FILE:</b> <code>{output_name}</code>")
        except Exception:
            pass

        # Use Telethon for resilient parallel upload (replaces Pyrogram fast_upload)
        from utils.telethon_upload import telethon_upload_file
        from utils.tg_utils import run_with_flood_retry

        sent_msg_id = await telethon_upload_file(
            file_path=output_name,
            chat_id=CHAT_ID,
            caption=report,
            thumb=THUMBNAIL if has_thumb else None,
            progress_callback=upload_progress,
            progress_args=(app, CHAT_ID, status, output_name),
        )

        # Send full rename report to private bot/chat
        try:
            from utils.tg_simple import notify_private
            notify_private(report)
        except Exception:
            pass

        # Forward/Copy to channels if configured
        if sent_msg_id and getattr(config, "FORWARD_CHATS", None):
            print(f"[FORWARD] Copying message to {len(config.FORWARD_CHATS)} target channel(s)...", flush=True)
            for target_chat in config.FORWARD_CHATS:
                try:
                    await run_with_flood_retry(
                        app.copy_message,
                        chat_id=target_chat,
                        from_chat_id=CHAT_ID,
                        message_id=sent_msg_id
                    )
                    print(f"[FORWARD] Successfully copied message to {target_chat}", flush=True)
                except Exception as fe:
                    print(f"[FORWARD] copy_message failed to {target_chat} ({fe}). Trying standard forward...", flush=True)
                    try:
                        await run_with_flood_retry(
                            app.forward_messages,
                            chat_id=target_chat,
                            from_chat_id=CHAT_ID,
                            message_ids=sent_msg_id
                        )
                        print(f"[FORWARD] Successfully forwarded message to {target_chat}", flush=True)
                    except Exception as fe2:
                        print(f"[FORWARD ERROR] Failed to forward to {target_chat}: {fe2}", flush=True)


        try: await status.delete()
        except: pass

        # Cleanup
        for f in [SOURCE_FILE, output_name, THUMBNAIL]:
            if os.path.exists(f): os.remove(f)

        print(f"[rename] Mission complete → {output_name}")

    except Exception as e:
        traceback.print_exc()
        try:
            await tg_edit(app, CHAT_ID, status.id,
                f"<b>❌ RENAME MISSION FAILED</b>\n<code>{e}</code>")
        except: pass
        sys.exit(1)
    finally:
        # Disconnect Telethon upload client
        try:
            from utils.telethon_upload import disconnect_client
            await disconnect_client()
        except: pass
        try: await app.stop()
        except: pass

if __name__ == "__main__":
    asyncio.run(main())
