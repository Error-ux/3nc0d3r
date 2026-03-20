"""
upload.py — Phase 3: Remux → VMAF → Gofile → Telegram
Reads encode_results.json written by main.py.
TG connection logic is identical to main.py.
"""
import asyncio
import json
import os
import subprocess
import time
import traceback

from pyrogram import Client, enums
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

import config
from media import async_generate_grid, get_vmaf, upload_to_cloud
from rename import format_track_report
from ui import format_time, upload_progress, get_failure_ui
import ui as _ui


# ---------------------------------------------------------------------------
# LANE RESOLUTION — identical to main.py
# ---------------------------------------------------------------------------
ALL_LANES = [chr(ord("A") + i) for i in range(20)]

def _resolve_lane(run_number: int) -> str:
    return ALL_LANES[run_number % 20]

def _resolve_session_names() -> list[str]:
    run_number = int(os.environ.get("GITHUB_RUN_NUMBER", "0"))
    lane = _resolve_lane(run_number)
    print(f"Upload session lane: {lane} (run #{run_number})")
    other_lanes = [l for l in ALL_LANES if l != lane]
    sessions = []
    sessions.append(f"tg_session_dir/enc_session_{lane}")
    sessions.append(f"tg_session_dir/tg_dl_session_{lane}")
    for other in other_lanes:
        sessions.append(f"tg_session_dir/enc_session_{other}")
        sessions.append(f"tg_session_dir/tg_dl_session_{other}")
    sessions.append(config.SESSION_NAME)
    return sessions


# ---------------------------------------------------------------------------
# CONNECT TELEGRAM — identical to main.py
# ---------------------------------------------------------------------------
async def connect_telegram(tg_state: dict, tg_ready: asyncio.Event, label: str):
    session_names = _resolve_session_names()
    flood_waits: dict[str, int] = {}

    app = None
    for session_name in session_names:
        try:
            candidate = Client(
                session_name,
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                bot_token=config.BOT_TOKEN,
            )
            await candidate.start()
            app = candidate
            print(f"TG auth OK with session: {session_name}")
            break
        except FloodWait as e:
            flood_waits[session_name] = e.value
            print(f"FloodWait {e.value}s on '{session_name}' — trying next...")
            continue
        except Exception as e:
            print(f"TG auth error on '{session_name}': {e} — trying next...")
            continue

    if app is None and flood_waits:
        best_session = min(flood_waits, key=flood_waits.get)
        wait_secs = flood_waits[best_session]
        attempt = 0
        while True:
            attempt += 1
            print(f"All sessions flooded. Sleeping {wait_secs}s (attempt {attempt})...")
            await asyncio.sleep(wait_secs + 5)
            try:
                candidate = Client(
                    best_session,
                    api_id=config.API_ID,
                    api_hash=config.API_HASH,
                    bot_token=config.BOT_TOKEN,
                )
                await candidate.start()
                app = candidate
                print(f"TG auth OK (post-flood attempt {attempt}): {best_session}")
                break
            except FloodWait as e:
                wait_secs = e.value
                print(f"Another FloodWait: {wait_secs}s — retrying...")
                continue
            except Exception as e:
                print(f"TG auth failed on post-flood attempt {attempt}: {e}")
                return

    if app is None:
        print("TG auth failed: no usable session found.")
        return

    try:
        status = await app.send_message(
            config.CHAT_ID,
            f"<b>[ UPLINK PHASE ] Preparing: {label}</b>",
            parse_mode=enums.ParseMode.HTML,
        )
    except FloodWait as e:
        await asyncio.sleep(e.value)
        status = await app.send_message(
            config.CHAT_ID,
            f"<b>[ UPLINK PHASE ] Preparing: {label}</b>",
            parse_mode=enums.ParseMode.HTML,
        )

    tg_state["app"] = app
    tg_state["status"] = status
    tg_ready.set()
    print("Telegram connected.")


# ---------------------------------------------------------------------------
# TG EDIT — identical to main.py
# ---------------------------------------------------------------------------
async def tg_edit(tg_state: dict, tg_ready: asyncio.Event, text: str, reply_markup=None):
    if not tg_ready.is_set():
        return
    app    = tg_state.get("app")
    status = tg_state.get("status")
    if not app or not status:
        return
    try:
        kwargs = dict(parse_mode=enums.ParseMode.HTML)
        if reply_markup:
            kwargs["reply_markup"] = reply_markup
        await app.edit_message_text(config.CHAT_ID, status.id, text, **kwargs)
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# FAILURE NOTIFIER — identical to main.py
# ---------------------------------------------------------------------------
async def tg_notify_failure(tg_state: dict, tg_ready: asyncio.Event,
                            file_name: str, reason: str):
    app    = tg_state.get("app")
    status = tg_state.get("status")
    if not app or not status:
        print(f"[TG-FAIL] TG unavailable — reason: {reason}")
        return
    try:
        await app.edit_message_text(
            config.CHAT_ID, status.id,
            get_failure_ui(file_name, reason, phase="UPLOAD"),
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as e:
        print(f"[TG-FAIL] Could not edit status: {e}")
    if os.path.exists(config.LOG_FILE):
        try:
            await app.send_document(
                config.CHAT_ID, config.LOG_FILE,
                caption="<b>FULL MISSION LOG</b>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception as e:
            print(f"[TG-FAIL] Could not send log: {e}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
async def main():
    # ── Load encode results ───────────────────────────────────────────────
    if not os.path.exists("encode_results.json"):
        raise FileNotFoundError("encode_results.json missing — encode phase may have failed.")

    with open("encode_results.json") as f:
        r = json.load(f)

    # Resolve final filename — output_fname.txt is the most reliable source,
    # encode_results.json["file_name"] is the fallback, env var is last resort.
    if os.path.exists("output_fname.txt"):
        config.FILE_NAME = open("output_fname.txt").read().strip()
        print(f"[upload] FILE_NAME from output_fname.txt: {config.FILE_NAME}")
    elif r.get("file_name"):
        config.FILE_NAME = r["file_name"]
        print(f"[upload] FILE_NAME from encode_results.json: {config.FILE_NAME}")
    else:
        print(f"[upload] FILE_NAME from env/config: {config.FILE_NAME}")

    duration            = r["duration"]
    width               = r["width"]
    height              = r["height"]
    fps_val             = r["fps_val"]
    crop_val            = r["crop_val"]
    total_mission_time  = r["total_mission_time"]
    res_label           = r["res_label"]
    final_crf           = r["final_crf"]
    final_preset        = r["final_preset"]
    hdr_label           = r["hdr_label"]
    grain_label         = r["grain_label"]
    final_audio_bitrate = r["final_audio_bitrate"]
    audio_type_label    = r.get("audio_type_label")
    demo_mode           = r["demo_mode"]
    demo_duration       = r["demo_duration"]
    demo_start          = r["demo_start"]
    audio_tracks        = r["audio_tracks"]
    sub_tracks          = r["sub_tracks"]

    if not os.path.exists(config.FILE_NAME):
        raise FileNotFoundError(f"Encoded file not found: {config.FILE_NAME}")

    start_time = time.time()

    # ── Connect Telegram ──────────────────────────────────────────────────
    tg_state: dict = {}
    tg_ready = asyncio.Event()
    await connect_telegram(tg_state, tg_ready, config.FILE_NAME)

    app    = tg_state.get("app")
    status = tg_state.get("status")

    if not app or not status:
        print("TG unavailable — proceeding headlessly.")

    try:
        # 1. REMUX — copy chapters/attachments from source, stamp encoder title
        await tg_edit(tg_state, tg_ready, "<b>[ SYSTEM.OPTIMIZE ] Finalizing Metadata...</b>")
        fixed_file  = f"FIXED_{config.FILE_NAME}"
        source      = config.SOURCE if os.path.exists(config.SOURCE) else config.FILE_NAME
        title_args  = ["--title", config.ENCODER_TITLE] if config.ENCODER_TITLE.strip() else []
        subprocess.run(
            ["mkvmerge", "-o", fixed_file, *title_args,
             config.FILE_NAME,
             "--no-video", "--no-audio", "--no-subtitles", "--no-attachments", source],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        if os.path.exists(fixed_file):
            os.remove(config.FILE_NAME)
            os.rename(fixed_file, config.FILE_NAME)

        # 2. GRID + GOFILE concurrently
        final_size = os.path.getsize(config.FILE_NAME) / (1024 * 1024)

        grid_task = asyncio.create_task(async_generate_grid(duration, config.FILE_NAME))

        if config.RUN_UPLOAD:
            await tg_edit(tg_state, tg_ready, "<b>[ SYSTEM.CLOUD ] Uploading to Gofile...</b>")
            cloud_task = asyncio.create_task(
                upload_to_cloud(config.FILE_NAME, app, config.CHAT_ID, status)
            )
        else:
            cloud_task = None

        # 3. VMAF
        if config.RUN_VMAF:
            vmaf_val, ssim_val = await get_vmaf(
                config.FILE_NAME, crop_val, width, height, duration, fps_val
            )
        else:
            vmaf_val, ssim_val = "N/A", "N/A"

        await grid_task
        cloud = await cloud_task if cloud_task else {"direct": None, "page": None, "source": "disabled"}

        # 4. BUILD BUTTONS
        btn_row = []
        if cloud["source"] == "gofile":
            if cloud.get("page"):
                btn_row.append(InlineKeyboardButton("Gofile", url=cloud["page"]))
            if cloud.get("direct"):
                btn_row.append(InlineKeyboardButton("Direct", url=cloud["direct"]))
        elif cloud["source"] == "litterbox" and cloud.get("direct"):
            btn_row.append(InlineKeyboardButton("Litterbox", url=cloud["direct"]))
        buttons = InlineKeyboardMarkup([btn_row]) if btn_row else None

        # 5. SIZE OVERFLOW
        if final_size > 2000:
            await tg_edit(
                tg_state, tg_ready,
                "<b>[ SIZE OVERFLOW ]</b> File too large for Telegram. Cloud link below.",
                reply_markup=buttons,
            )
            return

        # 6. BUILD REPORT
        thumb             = config.SCREENSHOT if os.path.exists(config.SCREENSHOT) else None
        crop_label_report = " | Cropped" if crop_val else ""
        track_report      = format_track_report(audio_tracks, sub_tracks)

        user_track_notes = ""
        if config.SUB_TRACKS and config.SUB_TRACKS.strip():
            user_track_notes += f"\n🔤 <b>SUB LABELS:</b>  <code>{config.SUB_TRACKS}</code>"
        if config.AUDIO_TRACKS and config.AUDIO_TRACKS.strip():
            user_track_notes += f"\n🔊 <b>AUDIO LABELS:</b> <code>{config.AUDIO_TRACKS}</code>"

        audio_mode_line = (
            f"{audio_type_label.upper()} ({config.AUDIO_MODE.upper()} @ {final_audio_bitrate})"
            if audio_type_label
            else f"{config.AUDIO_MODE.upper()} @ {final_audio_bitrate}"
        )
        content_line     = f"└ Type: {config.CONTENT_TYPE}\n" if config.CONTENT_TYPE else ""
        demo_report_line = (
            f"⚡ <b>DEMO MODE:</b> <code>{demo_duration}s from {demo_start}</code>\n"
            if demo_mode else ""
        )

        report = (
            f"✅ <b>MISSION ACCOMPLISHED</b>\n\n"
            f"📄 <b>FILE:</b> <code>{config.FILE_NAME}</code>\n"
            f"⏱ <b>TIME:</b> <code>{format_time(total_mission_time)}</code>\n"
            f"⏳<b>DURATION:</b> <code>{format_time(duration)}</code>\n"
            f"📦 <b>SIZE:</b> <code>{final_size:.2f} MB</code>\n"
            f"📊 <b>QUALITY:</b> VMAF: <code>{vmaf_val}</code> | SSIM: <code>{ssim_val}</code>\n\n"
            f"🛠 <b>SPECS:</b>\n"
            f"└ Preset: {final_preset} | CRF: {final_crf}\n"
            f"└ Video: {res_label}{crop_label_report} | {hdr_label}{grain_label}\n"
            f"└ Audio: {audio_mode_line}\n"
            f"{content_line}"
            f"{demo_report_line}"
            f"\n{track_report}"
            f"{user_track_notes}"
        )

        # 7. TRANSMIT
        _ui.last_up_pct = -1; _ui.last_up_update = 0; _ui.up_start_time = 0

        await tg_edit(tg_state, tg_ready, "<b>[ SYSTEM.UPLINK ] Transmitting Final Video...</b>")

        await app.send_document(
            chat_id=config.CHAT_ID,
            document=config.FILE_NAME,
            file_name=config.FILE_NAME,
            thumb=thumb,
            caption=report,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=buttons,
            progress=upload_progress,
            progress_args=(app, config.CHAT_ID, status, config.FILE_NAME),
        )

        # 8. CLEANUP
        try: await status.delete()
        except: pass
        for f in [config.SOURCE, config.FILE_NAME, config.LOG_FILE,
                  config.SCREENSHOT, "encode_results.json", "output_fname.txt"]:
            if os.path.exists(f):
                os.remove(f)

    except Exception as exc:
        tb = traceback.format_exc()
        print(f"[FATAL] Unexpected error: {exc}\n{tb}")
        elapsed_total = time.time() - start_time
        reason = (
            f"Unexpected error after {format_time(elapsed_total)}:\n"
            f"{type(exc).__name__}: {exc}\n\n"
            f"{tb[-300:]}"
        )
        await tg_notify_failure(tg_state, tg_ready, config.FILE_NAME, reason)
        raise
    finally:
        app = tg_state.get("app")
        if app:
            try: await app.stop()
            except: pass


if __name__ == "__main__":
    asyncio.run(main())
