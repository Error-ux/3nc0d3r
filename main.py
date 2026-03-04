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
# KV HELPERS
# On-demand only — main.py NEVER writes to KV on a timer.
# Instead it polls for a poll_request flag every 5s (cheap GET).
# The Worker sets the flag when /p is sent; main.py writes once and stops.
# Daily KV ops: ~25,920 reads (poll checks) + ~120 writes (per 10 /p calls)
# ---------------------------------------------------------------------------

def _kv_url(key, ttl=None):
    base = (
        f"https://api.cloudflare.com/client/v4/accounts/{config.CF_ACCOUNT_ID}"
        f"/storage/kv/namespaces/{config.CF_KV_NAMESPACE_ID}/values/{key}"
    )
    return base + (f"?expiration_ttl={ttl}" if ttl else "")


def _kv_headers():
    return {
        "Authorization": f"Bearer {config.CF_KV_TOKEN}",
        "Content-Type": "application/json",
    }


def _kv_configured():
    return all([config.CF_ACCOUNT_ID, config.CF_KV_NAMESPACE_ID, config.CF_KV_TOKEN])


def _kv_check_poll():
    """
    Returns True if the Worker has set a poll_request flag in KV.
    Fast 404 when no /p is pending — barely any latency on the encode loop.
    """
    if not _kv_configured():
        return False
    req = urllib.request.Request(
        _kv_url("poll_request"), method="GET", headers=_kv_headers()
    )
    try:
        urllib.request.urlopen(req, timeout=3)
        return True   # HTTP 200 — flag present
    except urllib.error.HTTPError:
        return False  # HTTP 404 — no pending poll
    except Exception:
        return False


def _kv_put(payload: dict):
    """Writes progress snapshot. Called only when poll_request flag is detected."""
    if not _kv_configured():
        return
    req = urllib.request.Request(
        _kv_url(f"progress_{config.GITHUB_RUN_ID}", ttl=120),
        data=json.dumps(payload).encode(),
        method="PUT",
        headers=_kv_headers()
    )
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


def _kv_delete():
    """Cleans up this run's progress key when encode ends."""
    if not _kv_configured():
        return
    req = urllib.request.Request(
        _kv_url(f"progress_{config.GITHUB_RUN_ID}"),
        method="DELETE", headers=_kv_headers()
    )
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


async def write_progress(loop, payload: dict):
    await loop.run_in_executor(None, _kv_put, payload)


async def delete_progress(loop):
    await loop.run_in_executor(None, _kv_delete)


async def check_and_write_if_polled(loop, payload: dict):
    """
    Checks for poll_request flag; writes progress only if flag is set.
    Called every 5s in encode loop and VMAF loop.
    """
    flag = await loop.run_in_executor(None, _kv_check_poll)
    if flag:
        await write_progress(loop, payload)


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

        start_time      = time.time()
        last_poll_check = 0   # Gate poll checks to every 5s — avoids excess KV reads

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

                        # On-demand only: check for poll_request flag every 5s.
                        # Zero writes unless user actually sends /p.
                        if time.time() - last_poll_check >= 5:
                            last_poll_check = time.time()
                            await check_and_write_if_polled(loop, {
                                "phase":    "encode",
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
                                "ts":       int(time.time()),
                            })

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

        # 8. METRICS + GOFILE UPLOAD (run in parallel — no reason to wait on each other)
        final_size  = os.path.getsize(config.FILE_NAME) / (1024 * 1024)

        await app.edit_message_text(
            config.CHAT_ID, status.id,
            "☁️ <b>[ SYSTEM.CLOUD ] Uploading to Gofile...</b>",
            parse_mode=enums.ParseMode.HTML
        )

        # Kick off grid + Gofile concurrently
        grid_task  = asyncio.create_task(async_generate_grid(duration, config.FILE_NAME))
        cloud_task = asyncio.create_task(upload_to_cloud(config.FILE_NAME))

        if config.RUN_VMAF:
            # Same on-demand approach: VMAF also only writes when /p is pending
            vmaf_writer = lambda payload: check_and_write_if_polled(loop, payload)
            vmaf_val, ssim_val = await get_vmaf(config.FILE_NAME, crop_val, width, height, duration, fps_val, kv_writer=vmaf_writer)
        else:
            vmaf_val, ssim_val = "N/A", "N/A"

        # Wait for both background tasks
        await grid_task
        cloud = await cloud_task   # dict: {direct, page, source}

        # Build cloud link lines for caption
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
        # For files > 2 GB: TG can't receive them so cloud-only
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
