import asyncio
import json
import os
import subprocess
import time
import shutil
import urllib.request
import urllib.error
from pyrogram import Client, enums
from pyrogram.errors import FloodWait

import config
from media import get_video_info, get_crop_params, select_params, async_generate_grid, get_vmaf, upload_to_cloud
from ui import get_encode_ui, format_time, upload_progress, get_failure_ui


# ---------------------------------------------------------------------------
# KV FLAG CHECKER
# main.py never writes to KV. It only checks for a poll_request flag (GET).
# When the flag is found, main.py sends a TG message directly and deletes
# the flag. The Worker only ever does 1 KV write per /p call.
#
# Daily KV reads: 12 encodes × poll every 5s × 3h = ~25,920 reads
# Daily KV writes: 0 from main.py. Only from Worker when /p is sent.
# ---------------------------------------------------------------------------

def _kv_configured():
    return all([config.CF_ACCOUNT_ID, config.CF_KV_NAMESPACE_ID, config.CF_KV_TOKEN])


def _kv_headers():
    return {"Authorization": f"Bearer {config.CF_KV_TOKEN}"}


def _kv_base():
    return (
        f"https://api.cloudflare.com/client/v4/accounts/{config.CF_ACCOUNT_ID}"
        f"/storage/kv/namespaces/{config.CF_KV_NAMESPACE_ID}/values"
    )


def _kv_poll_check():
    """
    Checks for poll_request flag in KV.
    If found and timestamp is fresh (within 60s): returns that timestamp.
    Does NOT delete — all running encodes must each see the same flag and fire.
    If not found (404) or stale: returns False.
    """
    if not _kv_configured():
        return False
    try:
        get_req = urllib.request.Request(
            f"{_kv_base()}/poll_request",
            method="GET",
            headers=_kv_headers()
        )
        resp = urllib.request.urlopen(get_req, timeout=3)
        ts = int(resp.read().decode().strip())
        # Only respond if the flag was set within the last 60s
        if (time.time() * 1000) - ts < 60_000:
            return ts
        return False
    except urllib.error.HTTPError:
        return False  # 404 = no flag, expected most of the time
    except Exception:
        return False


# ---------------------------------------------------------------------------
# PROGRESS MESSAGE SENDER
# Sends a fresh TG message with the current encode state.
# Schedules deletion after 30s as a background task — never blocks encode loop.
# ---------------------------------------------------------------------------
async def send_progress_and_autodelete(app, payload: dict):
    """Sends a progress box to TG then deletes it after 30s."""
    try:
        bar_ui = get_encode_ui(
            payload["file"],
            payload["speed"],
            payload["fps"],
            payload["elapsed"],
            payload["eta"],
            payload["curr_sec"],
            payload["duration"],
            payload["percent"],
            payload["crf"],
            payload["preset"],
            payload["res"],
            payload["crop_label"],
            payload["hdr"],
            payload["grain_label"],
            payload["audio"],
            payload["abitrate"],
            payload["size_mb"],
        )
        msg = await app.send_message(
            config.CHAT_ID,
            bar_ui,
            parse_mode=enums.ParseMode.HTML
        )
        await asyncio.sleep(30)
        try:
            await msg.delete()
        except Exception:
            pass
    except Exception:
        pass


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
async def main():
    loop = asyncio.get_event_loop()

    # 1. PRE-FLIGHT DISK CHECK
    if os.path.exists(config.SOURCE):
        total, used, free = shutil.disk_usage("/")
        source_size = os.path.getsize(config.SOURCE)
        if (source_size * 2.1) > free:
            print(f"⚠️ DISK WARNING: {source_size/(1024**3):.2f}GB source might exceed {free/(1024**3):.2f}GB free space.")

    # 2. METADATA EXTRACTION
    try:
        duration, width, height, is_hdr, total_frames, channels, fps_val = get_video_info()
    except Exception as e:
        print(f"Metadata error: {e}")
        return

    # 3. PARAMETER CONFIGURATION
    def_crf, def_preset = select_params(height)
    final_crf    = config.USER_CRF if (config.USER_CRF and config.USER_CRF.strip()) else def_crf
    final_preset = config.USER_PRESET if (config.USER_PRESET and config.USER_PRESET.strip()) else def_preset

    res_label = config.USER_RES if config.USER_RES else "1080"
    crop_val  = get_crop_params(duration)

    # -- VIDEO FILTERS --
    vf_filters = ["hqdn3d=1.5:1.2:3:3"]
    if crop_val: vf_filters.append(f"crop={crop_val}")
    vf_filters.append(f"scale=-1:{res_label}")
    video_filters = ["-vf", ",".join(vf_filters)]

    # -- AUDIO CONFIGURATION --
    audio_cmd           = ["-c:a", "libopus", "-b:a", "32k", "-vbr", "on"]
    final_audio_bitrate = "32k"

    # -- SVT-AV1 PARAMETERS --
    svtav1_tune = "tune=0:film-grain=0:enable-overlays=1:aq-mode=1"

    # UI Labels
    hdr_label       = "HDR10" if is_hdr else "SDR"
    grain_label     = " | Grain: 0"
    crop_label_txt  = " | Cropped" if crop_val else ""

    # 4. TELEGRAM UPLINK INITIALIZATION
    async with Client(config.SESSION_NAME, api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.BOT_TOKEN) as app:
        try:
            status = await app.send_message(
                config.CHAT_ID,
                f"📡 <b>[ SYSTEM ONLINE ] Encoding: {config.FILE_NAME}</b>\n"
                f"<i>Send /p to check live progress.</i>",
                parse_mode=enums.ParseMode.HTML
            )
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
            status = await app.send_message(
                config.CHAT_ID,
                f"📡 <b>[ SYSTEM RECOVERY ] Encoding: {config.FILE_NAME}</b>\n"
                f"<i>Send /p to check live progress.</i>",
                parse_mode=enums.ParseMode.HTML
            )

        # 5. ENCODING EXECUTION
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

        start_time      = time.time()
        last_poll_check  = 0
        last_poll_ts     = 0   # timestamp of the last /p we already responded to
        # Track in-flight progress sends so we don't stack them
        progress_task    = None

        with open(config.LOG_FILE, "w") as f_log:
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True
            )

            for line in process.stdout:
                f_log.write(line)
                if config.CANCELLED:
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

                        # Check KV flag every 2s — fast 404 when /p not sent
                        if time.time() - last_poll_check >= 2:
                            last_poll_check = time.time()
                            # Run in executor so it never blocks ffmpeg stdout reading
                            flag = await loop.run_in_executor(None, _kv_poll_check)
                            if flag and flag != last_poll_ts and (progress_task is None or progress_task.done()):
                                last_poll_ts = flag
                                # Fire-and-forget: send progress box + auto-delete in 10s
                                progress_task = asyncio.create_task(
                                    send_progress_and_autodelete(app, {
                                        "file":        config.FILE_NAME,
                                        "speed":       round(speed, 2),
                                        "fps":         int(fps),
                                        "elapsed":     int(elapsed),
                                        "eta":         int(eta),
                                        "curr_sec":    int(curr_sec),
                                        "duration":    int(duration),
                                        "percent":     round(percent, 1),
                                        "crf":         final_crf,
                                        "preset":      final_preset,
                                        "res":         res_label,
                                        "crop_label":  crop_label_txt,
                                        "hdr":         hdr_label,
                                        "grain_label": grain_label,
                                        "audio":       config.AUDIO_MODE,
                                        "abitrate":    final_audio_bitrate,
                                        "size_mb":     round(size_mb, 2),
                                    })
                                )

                    except Exception:
                        continue

        process.wait()
        total_mission_time = time.time() - start_time

        # 6. ERROR HANDLING
        if process.returncode != 0:
            error_snippet = "".join(open(config.LOG_FILE).readlines()[-10:]) if os.path.exists(config.LOG_FILE) else "Unknown Engine Crash."
            await app.edit_message_text(config.CHAT_ID, status.id, get_failure_ui(config.FILE_NAME, error_snippet), parse_mode=enums.ParseMode.HTML)
            await app.send_document(config.CHAT_ID, config.LOG_FILE, caption="📑 <b>FULL MISSION LOG</b>")
            return

        # 7. POST-PROCESSING (Remux)
        await app.edit_message_text(config.CHAT_ID, status.id, "🛠️ <b>[ SYSTEM.OPTIMIZE ] Finalizing Metadata...</b>", parse_mode=enums.ParseMode.HTML)
        fixed_file = f"FIXED_{config.FILE_NAME}"
        subprocess.run(["mkvmerge", "-o", fixed_file, config.FILE_NAME, "--no-video", "--no-audio", "--no-subtitles", "--no-attachments", config.SOURCE])
        if os.path.exists(fixed_file):
            os.remove(config.FILE_NAME)
            os.rename(fixed_file, config.FILE_NAME)

        # 8. METRICS + GOFILE UPLOAD (concurrent)
        final_size = os.path.getsize(config.FILE_NAME) / (1024 * 1024)

        await app.edit_message_text(
            config.CHAT_ID, status.id,
            "☁️ <b>[ SYSTEM.CLOUD ] Uploading to Gofile...</b>",
            parse_mode=enums.ParseMode.HTML
        )

        grid_task  = asyncio.create_task(async_generate_grid(duration, config.FILE_NAME))
        cloud_task = asyncio.create_task(upload_to_cloud(config.FILE_NAME))

        if config.RUN_VMAF:
            vmaf_val, ssim_val = await get_vmaf(config.FILE_NAME, crop_val, width, height, duration, fps_val)
        else:
            vmaf_val, ssim_val = "N/A", "N/A"

        await grid_task
        cloud = await cloud_task  # dict: {direct, page, source}

        # Build cloud link lines
        if cloud["source"] == "gofile":
            cloud_lines = (
                f"\n\n☁️ <b>GOFILE:</b>\n"
                f"└ 🔗 <b>Direct:</b> {cloud['direct']}\n"
                f"└ 📄 <b>Page:</b> {cloud['page']}"
            )
        elif cloud["source"] == "litterbox":
            cloud_lines = f"\n\n☁️ <b>LITTERBOX (fallback):</b> {cloud['direct']}"
        else:
            cloud_lines = "\n\n⚠️ <b>Cloud upload failed.</b>"

        # 9. FINAL UPLINK
        if final_size > 2000:
            await app.edit_message_text(
                config.CHAT_ID, status.id,
                f"⚠️ <b>[ SIZE OVERFLOW ]</b> File too large for Telegram.{cloud_lines}",
                parse_mode=enums.ParseMode.HTML
            )
            return

        photo_msg = None
        if os.path.exists(config.SCREENSHOT):
            photo_msg = await app.send_photo(
                config.CHAT_ID, config.SCREENSHOT,
                caption=f"🖼 <b>PROXIMITY GRID:</b> <code>{config.FILE_NAME}</code>",
                parse_mode=enums.ParseMode.HTML
            )

        crop_label_report = " | Cropped" if crop_val else ""
        report = (
            f"✅ <b>MISSION ACCOMPLISHED</b>\n\n"
            f"📄 <b>FILE:</b> <code>{config.FILE_NAME}</code>\n"
            f"⏱ <b>TIME:</b> <code>{format_time(total_mission_time)}</code>\n"
            f"📦 <b>SIZE:</b> <code>{final_size:.2f} MB</code>\n"
            f"📊 <b>QUALITY:</b> VMAF: <code>{vmaf_val}</code> | SSIM: <code>{ssim_val}</code>\n\n"
            f"🛠 <b>SPECS:</b>\n"
            f"└ <b>Preset:</b> {final_preset} | <b>CRF:</b> {final_crf}\n"
            f"└ <b>Video:</b> {res_label}{crop_label_report} | {hdr_label}{grain_label}\n"
            f"└ <b>Audio:</b> {config.AUDIO_MODE.upper()} @ {final_audio_bitrate}"
            f"{cloud_lines}"
        )

        await app.edit_message_text(config.CHAT_ID, status.id, "🚀 <b>[ SYSTEM.UPLINK ] Transmitting Final Video...</b>", parse_mode=enums.ParseMode.HTML)

        await app.send_document(
            chat_id=config.CHAT_ID,
            document=config.FILE_NAME,
            caption=report,
            parse_mode=enums.ParseMode.HTML,
            reply_to_message_id=photo_msg.id if photo_msg else None,
            progress=upload_progress,
            progress_args=(app, config.CHAT_ID, status, config.FILE_NAME)
        )

        # CLEANUP
        try: await status.delete()
        except: pass
        for f in [config.SOURCE, config.FILE_NAME, config.LOG_FILE, config.SCREENSHOT]:
            if os.path.exists(f): os.remove(f)


if __name__ == "__main__":
    asyncio.run(main())
