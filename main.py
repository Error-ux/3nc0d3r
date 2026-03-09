import asyncio
import os
import subprocess
import time
import shutil
import psutil
from pyrogram import Client, enums
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

import config
from media import get_video_info, get_crop_params, select_params, async_generate_grid, get_vmaf, upload_to_cloud
from ui import get_encode_ui, format_time, upload_progress, get_failure_ui
from rename import resolve_output_name, format_track_report


# ---------------------------------------------------------------------------
# KV FLAG CHECKER
# main.py never writes to KV. It only checks for a poll_request flag (GET).
# When the flag is found, main.py sends a TG message directly and deletes
# the flag. The Worker only ever does 1 KV write per /p call.
#
# Daily KV reads: 12 encodes x poll every 5s x 3h = ~25,920 reads
# Daily KV writes: 0 from main.py. Only from Worker when /p is sent.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# TELEGRAM AUTH — runs concurrently with encoding.
# Sets tg_ready when the client is connected and initial message is sent.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# LANE RESOLUTION — derive A–T (20 lanes) from GITHUB_RUN_NUMBER to match tg_handler.py
# 20 lanes comfortably supports 15+ simultaneous encodes with no session collisions.
# ---------------------------------------------------------------------------
ALL_LANES = [chr(ord("A") + i) for i in range(20)]  # ["A", "B", ..., "T"]

def _resolve_lane(run_number: int) -> str:
    return ALL_LANES[run_number % 20]

def _resolve_session_names() -> list[str]:
    """
    Return an ordered list of session names to try, most-preferred first.
    Own lane is tried first (enc + tg_dl), then every other lane as cross-lane
    fallbacks, then the legacy bare session last.
    With 20 lanes this gives up to 41 sessions before giving up.
    """
    run_number = int(os.environ.get("GITHUB_RUN_NUMBER", "0"))
    lane = _resolve_lane(run_number)
    print(f"Encoder session lane: {lane} (run #{run_number})")

    other_lanes = [l for l in ALL_LANES if l != lane]
    sessions = []
    # Own lane — highest priority
    sessions.append(f"tg_session_dir/enc_session_{lane}")
    sessions.append(f"tg_session_dir/tg_dl_session_{lane}")
    # Cross-lane fallbacks (enc first, then tg_dl for each)
    for other in other_lanes:
        sessions.append(f"tg_session_dir/enc_session_{other}")
        sessions.append(f"tg_session_dir/tg_dl_session_{other}")
    # Legacy bare session as final fallback
    sessions.append(config.SESSION_NAME)
    return sessions


async def connect_telegram(tg_state: dict, tg_ready: asyncio.Event, label: str):
    """
    Connect to Telegram trying each session in priority order.
    If a session gets a FloodWait we skip to the next one immediately —
    the flooded session is noted so it won't be retried.
    Falls back to sleeping out the shortest FloodWait only if every session
    is flooded.
    tg_state keys set on success: 'app', 'status'
    """
    session_names = _resolve_session_names()
    flood_waits: dict[str, int] = {}   # session_name → seconds to wait

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
            print(f"FloodWait {e.value}s on session '{session_name}' — trying next session...")
            continue
        except Exception as e:
            print(f"TG auth error on session '{session_name}': {e} — trying next session...")
            continue

    # All sessions flooded — sleep out the shortest wait then keep retrying
    # with the same sleep-and-retry loop until auth succeeds (original fallback
    # behaviour: Telegram tells us exactly how long to wait, so we always obey).
    if app is None and flood_waits:
        best_session = min(flood_waits, key=flood_waits.get)
        wait_secs    = flood_waits[best_session]
        attempt = 0
        while True:
            attempt += 1
            print(f"All sessions flooded. Sleeping {wait_secs}s for '{best_session}' (attempt {attempt})...")
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
                print(f"TG auth OK (post-flood attempt {attempt}) with session: {best_session}")
                break
            except FloodWait as e:
                # Telegram issued a fresh FloodWait — obey it and loop again
                wait_secs = e.value
                print(f"Another FloodWait: {wait_secs}s — will keep waiting...")
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
            f"<b>[ SYSTEM ONLINE ] Encoding: {label}</b>",
            parse_mode=enums.ParseMode.HTML,
        )
    except FloodWait as e:
        await asyncio.sleep(e.value)
        status = await app.send_message(
            config.CHAT_ID,
            f"<b>[ SYSTEM ONLINE ] Encoding: {label}</b>",
            parse_mode=enums.ParseMode.HTML,
        )

    tg_state["app"] = app
    tg_state["status"] = status
    tg_ready.set()
    print("Telegram connected.")


# ---------------------------------------------------------------------------
# SAFE TG EDIT — no-ops silently if TG not ready yet
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
# RESOURCE MONITOR — logs CPU + RAM every 5s during encoding
# ---------------------------------------------------------------------------
async def resource_monitor(stop_event: asyncio.Event, stats: dict, interval: int = 5):
    proc = psutil.Process(os.getpid())
    psutil.cpu_percent(interval=None)  # baseline

    while not stop_event.is_set():
        await asyncio.sleep(interval)
        sys_cpu = psutil.cpu_percent(interval=None)
        sys_ram = psutil.virtual_memory()
        ram_mb  = proc.memory_info().rss / 1024 ** 2

        stats["sys_cpu"] = sys_cpu
        stats["ram_mb"]  = ram_mb
        stats["sys_ram"] = sys_ram.percent
        print(
            f"[MONITOR] CPU: {sys_cpu:5.1f}% | "
            f"RAM: {ram_mb:6.1f}MB proc | {sys_ram.percent:5.1f}% sys"
        )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
async def main():
    # 1. PRE-FLIGHT DISK CHECK
    if os.path.exists(config.SOURCE):
        total, used, free = shutil.disk_usage("/")
        source_size = os.path.getsize(config.SOURCE)
        if (source_size * 2.1) > free:
            print(f"DISK WARNING: {source_size/(1024**3):.2f}GB source might exceed {free/(1024**3):.2f}GB free space.")

    # 2. METADATA EXTRACTION
    try:
        duration, width, height, is_hdr, total_frames, channels, fps_val = get_video_info()
    except Exception as e:
        print(f"Metadata error: {e}")
        return

    # 3. RENAME — build structured output filename if ANIME_NAME is set
    if config.ANIME_NAME and config.ANIME_NAME.strip():
        resolved_name, audio_type_label, audio_tracks, sub_tracks = resolve_output_name(
            source               = config.SOURCE,
            anime_name           = config.ANIME_NAME,
            season               = config.SEASON,
            episode              = config.EPISODE,
            height               = height,
            audio_type_override  = config.AUDIO_TYPE,
            content_type         = config.CONTENT_TYPE,
        )
        config.FILE_NAME = resolved_name
        print(f"[rename] Output → {resolved_name}  |  Audio: {audio_type_label}")
    else:
        # No rename requested — probe tracks for report only
        from rename import get_track_info
        audio_tracks, sub_tracks = get_track_info(config.SOURCE)
        audio_type_label = None

    # 4. PARAMETER CONFIGURATION
    def_crf, def_preset = select_params(height)
    final_crf    = config.USER_CRF if (config.USER_CRF and config.USER_CRF.strip()) else def_crf
    final_preset = config.USER_PRESET if (config.USER_PRESET and config.USER_PRESET.strip()) else def_preset

    res_label = config.USER_RES if (config.USER_RES and config.USER_RES.strip()) else None
    crop_val  = get_crop_params(duration)

    # -- VIDEO FILTERS --
    vf_filters = ["hqdn3d=1.5:1.2:3:3"]
    if crop_val: vf_filters.append(f"crop={crop_val}")
    if res_label: vf_filters.append(f"scale=-1:{res_label}")  # skip when ORIGINAL
    video_filters = ["-vf", ",".join(vf_filters)]

    # Display label — show actual source height when no downscale requested
    from rename import detect_quality
    res_label = res_label or f"Original({detect_quality(height)})"

    # -- AUDIO CONFIGURATION --
    audio_cmd           = ["-af", "aformat=channel_layouts=stereo", "-c:a", "libopus", "-b:a", "32k", "-vbr", "on"]
    final_audio_bitrate = "32k"

    # -- SVT-AV1 PARAMETERS --
    # pin=0 is required for GitHub Actions (virtualized VMs don't honour CPU affinity).
    # Without it SVT-AV1 tries to pin threads to specific cores and hangs indefinitely.
    svtav1_tune = "tune=0:film-grain=0:enable-overlays=1:aq-mode=1:pin=0:lp=8:tile-columns=2:tile-rows=1:la-depth=60"

    # UI Labels
    hdr_label      = "HDR10" if is_hdr else "SDR"
    grain_label    = " | Grain: 0"
    crop_label_txt = " | Cropped" if crop_val else ""

    # 4. LAUNCH TG AUTH AS A BACKGROUND TASK — encoding starts immediately.
    # If FloodWait fires, connect_telegram sleeps it out on its own while
    # FFmpeg keeps running. Progress messages are sent the instant TG is ready.
    tg_state = {}
    tg_ready = asyncio.Event()
    tg_task  = asyncio.create_task(
        connect_telegram(tg_state, tg_ready, config.FILE_NAME)
    )
    tg_connect_start = time.time()   # record when we started waiting for TG

    # 5. ENCODING EXECUTION (starts immediately, does not wait for TG)
    cmd = [
        "ffmpeg", "-i", config.SOURCE,
        "-map", "0:v:0",
        "-map", "0:a?",
        "-map", "0:s?",
        *video_filters,
        "-c:v", "libsvtav1",
        "-pix_fmt", "yuv420p10le",
        "-crf", str(final_crf),
        "-preset", str(final_preset),
        "-svtav1-params", svtav1_tune,
        "-threads", "0",
        *audio_cmd,
        "-c:s", "copy",
        "-map_chapters", "0",
        "-progress", "pipe:1",
        "-nostats",
        "-y", config.FILE_NAME
    ]

    # asyncio subprocess so TG auth task can make progress on the same loop
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    # Start resource monitor alongside encoding
    monitor_stop  = asyncio.Event()
    monitor_stats = {}
    monitor_task  = asyncio.create_task(resource_monitor(monitor_stop, monitor_stats))

    start_time        = time.time()
    last_progress_pct = -1
    last_update_time  = 0
    last_ui_text      = None   # latest snapshot; pushed to TG when it connects mid-encode

    with open(config.LOG_FILE, "w") as f_log:
        async for raw_line in process.stdout:
            line = raw_line.decode("utf-8", errors="replace")
            f_log.write(line)
            if config.CANCELLED:
                process.terminate()
                break

            if "out_time_ms" in line:
                try:
                    curr_sec = int(line.split("=")[1]) / 1_000_000
                    percent  = (curr_sec / duration) * 100
                    elapsed  = time.time() - start_time
                    speed    = curr_sec / elapsed if elapsed > 0 else 0
                    fps      = (percent / 100 * total_frames) / elapsed if elapsed > 0 else 0
                    eta      = (elapsed / percent) * (100 - percent) if percent > 0 else 0
                    size_mb  = os.path.getsize(config.FILE_NAME) / (1024 * 1024) if os.path.exists(config.FILE_NAME) else 0

                    milestone   = int(percent // 1) * 1
                    now         = time.time()
                    pct_crossed = milestone > last_progress_pct
                    time_due    = now - last_update_time >= 10

                    scifi_ui     = get_encode_ui(
                        config.FILE_NAME, speed, fps, elapsed, eta,
                        curr_sec, duration, percent,
                        final_crf, final_preset, res_label,
                        crop_label_txt, hdr_label, grain_label,
                        config.AUDIO_MODE, final_audio_bitrate, size_mb,
                        cpu=monitor_stats.get("sys_cpu"),
                        ram=monitor_stats.get("sys_ram"),
                    )
                    last_ui_text = scifi_ui   # always keep the freshest snapshot

                    if pct_crossed or time_due:
                        last_progress_pct = milestone
                        last_update_time  = now
                        # Only sends if TG is already ready; otherwise silently buffered
                        await tg_edit(tg_state, tg_ready, scifi_ui)

                except Exception:
                    continue

    await process.wait()
    monitor_stop.set()
    await monitor_task
    total_mission_time = time.time() - start_time

    # If TG is still waiting out a FloodWait, block here until it connects.
    # Encoding is done so we have all the time we need.
    if not tg_ready.is_set():
        print("Encode finished. Waiting for Telegram to become available...")
        try:
            await asyncio.wait_for(tg_ready.wait(), timeout=7200)  # max 2 hours
        except asyncio.TimeoutError:
            print("Telegram never connected within 2 hours. Exiting without upload.")
            tg_task.cancel()
            return

    await tg_task   # ensure connect_telegram fully finished

    app    = tg_state.get("app")
    status = tg_state.get("status")

    if not app or not status:
        print("TG connected but no status message — cannot send results.")
        if app:
            await app.stop()
        return

    try:
        # Push the last progress frame in case TG connected after encoding ended
        if last_ui_text:
            await tg_edit(tg_state, tg_ready, last_ui_text)

        # 6. ERROR HANDLING
        if process.returncode != 0:
            error_snippet = (
                "".join(open(config.LOG_FILE).readlines()[-10:])
                if os.path.exists(config.LOG_FILE)
                else "Unknown Engine Crash."
            )
            await app.edit_message_text(
                config.CHAT_ID, status.id,
                get_failure_ui(config.FILE_NAME, error_snippet),
                parse_mode=enums.ParseMode.HTML,
            )
            await app.send_document(config.CHAT_ID, config.LOG_FILE, caption="<b>FULL MISSION LOG</b>",
                                    parse_mode=enums.ParseMode.HTML)
            return

        # 7. POST-PROCESSING (Remux)
        await tg_edit(tg_state, tg_ready, "<b>[ SYSTEM.OPTIMIZE ] Finalizing Metadata...</b>")
        fixed_file = f"FIXED_{config.FILE_NAME}"
        subprocess.run([
            "mkvmerge", "-o", fixed_file, config.FILE_NAME,
            "--no-video", "--no-audio", "--no-subtitles", "--no-attachments", config.SOURCE
        ])
        if os.path.exists(fixed_file):
            os.remove(config.FILE_NAME)
            os.rename(fixed_file, config.FILE_NAME)

        # 8. METRICS + CLOUD UPLOAD (concurrent)
        final_size = os.path.getsize(config.FILE_NAME) / (1024 * 1024)

        await tg_edit(tg_state, tg_ready, "<b>[ SYSTEM.CLOUD ] Uploading to Gofile...</b>")

        grid_task  = asyncio.create_task(async_generate_grid(duration, config.FILE_NAME))
        cloud_task = asyncio.create_task(upload_to_cloud(config.FILE_NAME, app, config.CHAT_ID, status))

        if config.RUN_VMAF:
            vmaf_val, ssim_val = await get_vmaf(config.FILE_NAME, crop_val, width, height, duration, fps_val)
        else:
            vmaf_val, ssim_val = "N/A", "N/A"

        await grid_task
        cloud = await cloud_task   # dict: {direct, page, source}

        # 9. Build inline buttons from cloud result
        btn_row = []
        if cloud["source"] == "gofile":
            if cloud.get("page"):
                btn_row.append(InlineKeyboardButton("Gofile", url=cloud["page"]))
            if cloud.get("direct"):
                btn_row.append(InlineKeyboardButton("Direct", url=cloud["direct"]))
        elif cloud["source"] == "litterbox" and cloud.get("direct"):
            btn_row.append(InlineKeyboardButton("Litterbox", url=cloud["direct"]))
        buttons = InlineKeyboardMarkup([btn_row]) if btn_row else None

        # 10. FINAL UPLINK
        if final_size > 2000:
            await tg_edit(
                tg_state, tg_ready,
                "<b>[ SIZE OVERFLOW ]</b> File too large for Telegram. Cloud link below.",
                reply_markup=buttons,
            )
            return

        thumb = config.SCREENSHOT if os.path.exists(config.SCREENSHOT) else None

        crop_label_report = " | Cropped" if crop_val else ""
        track_report = format_track_report(audio_tracks, sub_tracks)

        # Append user-supplied track label notes if provided
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
        content_line = f"└ Type: {config.CONTENT_TYPE}\n" if config.CONTENT_TYPE else ""
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
            f"\n{track_report}"
            f"{user_track_notes}"
        )

        import ui as _ui; _ui.last_up_pct = -1; _ui.last_up_update = 0; _ui.up_start_time = 0

        await tg_edit(tg_state, tg_ready, "<b>[ SYSTEM.UPLINK ] Transmitting Final Video...</b>")

        await app.send_document(
            chat_id=config.CHAT_ID,
            document=config.FILE_NAME,
            thumb=thumb,
            caption=report,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=buttons,
            progress=upload_progress,
            progress_args=(app, config.CHAT_ID, status, config.FILE_NAME),
        )

        # CLEANUP
        try: await status.delete()
        except: pass
        for f in [config.SOURCE, config.FILE_NAME, config.LOG_FILE, config.SCREENSHOT]:
            if os.path.exists(f): os.remove(f)

    finally:
        if app:
            await app.stop()


if __name__ == "__main__":
    asyncio.run(main())