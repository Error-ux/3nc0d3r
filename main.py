import asyncio
import json
import os
import subprocess
import sys
import time
import shutil
from pyrogram import Client, enums
from pyrogram.errors import FloodWait

import config
from media import get_video_info, get_crop_params, select_params
from ui import get_encode_ui, format_time, get_failure_ui


# ---------------------------------------------------------------------------
# KV FLAG CHECKER
# main.py never writes to KV. It only checks for a poll_request flag (GET).
# When the flag is found, main.py sends a TG message directly and deletes
# the flag. The Worker only ever does 1 KV write per /p call.
#
# Daily KV reads: 12 encodes × poll every 5s × 3h = ~25,920 reads
# Daily KV writes: 0 from main.py. Only from Worker when /p is sent.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
async def main():
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
    audio_cmd           = ["-af", "aformat=channel_layouts=stereo", "-c:a", "libopus", "-b:a", "32k", "-vbr", "on"]
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
                f"📡 <b>[ SYSTEM ONLINE ] Encoding: {config.FILE_NAME}</b>",
                parse_mode=enums.ParseMode.HTML
            )
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
            status = await app.send_message(
                config.CHAT_ID,
                f"📡 <b>[ SYSTEM RECOVERY ] Encoding: {config.FILE_NAME}</b>",
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

        start_time        = time.time()
        last_progress_pct = -1
        last_update_time  = 0

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

                        milestone   = int(percent // 5) * 5
                        now         = time.time()
                        pct_crossed = milestone > last_progress_pct
                        time_due    = now - last_update_time >= 30

                        if pct_crossed or time_due:
                            last_progress_pct = milestone
                            last_update_time  = now
                            scifi_ui = get_encode_ui(config.FILE_NAME, speed, fps, elapsed, eta, curr_sec, duration, percent, final_crf, final_preset, res_label, crop_label_txt, hdr_label, grain_label, config.AUDIO_MODE, final_audio_bitrate, size_mb)
                            try:
                                await app.edit_message_text(config.CHAT_ID, status.id, scifi_ui, parse_mode=enums.ParseMode.HTML)
                            except FloodWait as e:
                                await asyncio.sleep(e.value + 1)
                            except Exception:
                                pass

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

        # Post-encode steps (remux, gofile, TG send) are handled by upload.py (Phase 3).
        # main.py only signals completion so the runner knows encoding succeeded.
        await app.edit_message_text(
            config.CHAT_ID, status.id,
            f"✅ <b>[ ENCODE COMPLETE ]</b> <code>{config.FILE_NAME}</code>\n"
            f"<i>Handing off to upload phase...</i>",
            parse_mode=enums.ParseMode.HTML
        )

        # Cleanup source — encoded file is kept for the upload artifact
        for f in [config.SOURCE, config.LOG_FILE]:
            if os.path.exists(f): os.remove(f)


if __name__ == "__main__":
    asyncio.run(main())
