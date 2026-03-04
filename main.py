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
# KV PROGRESS WRITER
# Pushes a live snapshot to Cloudflare KV so /p can read it on demand.
# Runs in a thread executor so it never blocks the async encode loop.
# Key:   progress_{GITHUB_RUN_ID}
# TTL:   600 seconds (auto-cleanup if the runner dies unexpectedly)
# ---------------------------------------------------------------------------
def _kv_put(payload: dict):
    """Synchronous KV write — called via run_in_executor."""
    if not all([config.CF_ACCOUNT_ID, config.CF_KV_NAMESPACE_ID, config.CF_KV_TOKEN]):
        return  # KV not configured — skip silently

    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{config.CF_ACCOUNT_ID}"
        f"/storage/kv/namespaces/{config.CF_KV_NAMESPACE_ID}"
        f"/values/progress_{config.GITHUB_RUN_ID}"
        f"?expiration_ttl=600"
    )
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, method="PUT",
        headers={
            "Authorization": f"Bearer {config.CF_KV_TOKEN}",
            "Content-Type": "application/json",
        }
    )
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # Never crash the encode over a progress update


def _kv_delete():
    """Synchronous KV delete — called via run_in_executor on encode end."""
    if not all([config.CF_ACCOUNT_ID, config.CF_KV_NAMESPACE_ID, config.CF_KV_TOKEN]):
        return

    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{config.CF_ACCOUNT_ID}"
        f"/storage/kv/namespaces/{config.CF_KV_NAMESPACE_ID}"
        f"/values/progress_{config.GITHUB_RUN_ID}"
    )
    req = urllib.request.Request(url, method="DELETE",
        headers={"Authorization": f"Bearer {config.CF_KV_TOKEN}"}
    )
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


async def write_progress(loop, payload: dict):
    await loop.run_in_executor(None, _kv_put, payload)


async def delete_progress(loop):
    await loop.run_in_executor(None, _kv_delete)


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
    final_crf = config.USER_CRF if (config.USER_CRF and config.USER_CRF.strip()) else def_crf
    final_preset = config.USER_PRESET if (config.USER_PRESET and config.USER_PRESET.strip()) else def_preset

    res_label = config.USER_RES if config.USER_RES else "1080"
    crop_val = get_crop_params(duration)

    # -- VIDEO FILTERS --
    vf_filters = ["hqdn3d=1.5:1.2:3:3"]
    if crop_val: vf_filters.append(f"crop={crop_val}")
    vf_filters.append(f"scale=-1:{res_label}")
    video_filters = ["-vf", ",".join(vf_filters)]

    # -- AUDIO CONFIGURATION --
    audio_cmd = ["-c:a", "libopus", "-b:a", "32k", "-vbr", "on"]
    final_audio_bitrate = "32k"

    # -- SVT-AV1 PARAMETERS --
    svtav1_tune = "tune=0:film-grain=0:enable-overlays=1:aq-mode=1"

    # UI Labels
    hdr_label = "HDR10" if is_hdr else "SDR"
    grain_label = " | Grain: 0"
    crop_label_txt = " | Cropped" if crop_val else ""

    # 4. TELEGRAM UPLINK INITIALIZATION
    async with Client(config.SESSION_NAME, api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.BOT_TOKEN) as app:
        try:
            status = await app.send_message(
                config.CHAT_ID,
                f"📡 <b>[ SYSTEM ONLINE ] Encoding: {config.FILE_NAME}</b>\n"
                f"<i>Send /p in chat to check live progress.</i>",
                parse_mode=enums.ParseMode.HTML
            )
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
            status = await app.send_message(
                config.CHAT_ID,
                f"📡 <b>[ SYSTEM RECOVERY ] Encoding: {config.FILE_NAME}</b>\n"
                f"<i>Send /p in chat to check live progress.</i>",
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

        start_time = time.time()
        last_kv_write = 0

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

                        # Write to KV every 10 seconds — no Telegram involved, no FloodWait
                        if time.time() - last_kv_write >= 10:
                            await write_progress(loop, {
                                "file":     config.FILE_NAME,
                                "run_id":   config.GITHUB_RUN_ID,
                                "percent":  round(percent, 1),
                                "speed":    round(speed, 2),
                                "fps":      int(fps),
                                "elapsed":  int(elapsed),
                                "eta":      int(eta),
                                "curr_sec": int(curr_sec),
                                "duration": int(duration),
                                "crf":      final_crf,
                                "preset":   final_preset,
                                "res":      res_label,
                                "crop":     bool(crop_val),
                                "hdr":      hdr_label,
                                "audio":    config.AUDIO_MODE,
                                "abitrate": final_audio_bitrate,
                                "size_mb":  round(size_mb, 2),
                                "ts":       int(time.time()),   # so Worker can show "data age"
                            })
                            last_kv_write = time.time()

                    except Exception:
                        continue

        process.wait()
        total_mission_time = time.time() - start_time

        # Always clean up KV entry when encode ends (success or fail)
        await delete_progress(loop)

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

        # 8. METRICS (VMAF & SSIM)
        final_size = os.path.getsize(config.FILE_NAME) / (1024 * 1024)
        grid_task = asyncio.create_task(async_generate_grid(duration, config.FILE_NAME))

        if config.RUN_VMAF:
            vmaf_writer = lambda payload: write_progress(loop, payload)
            vmaf_val, ssim_val = await get_vmaf(config.FILE_NAME, crop_val, width, height, duration, fps_val, kv_writer=vmaf_writer)
        else:
            vmaf_val, ssim_val = "N/A", "N/A"

        await grid_task

        # 9. FINAL UPLINK
        if final_size > 2000:
            await app.edit_message_text(config.CHAT_ID, status.id, "⚠️ <b>[ SIZE OVERFLOW ] Rerouting to Cloud...</b>", parse_mode=enums.ParseMode.HTML)
            cloud_url = await upload_to_cloud(config.FILE_NAME)
            await app.send_message(config.CHAT_ID, f"☁️ <b>EXTERNAL LINK:</b>\n{cloud_url}", parse_mode=enums.ParseMode.HTML)
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
