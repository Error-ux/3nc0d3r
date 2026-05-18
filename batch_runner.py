"""
batch_runner.py — Phased per-episode runner for fast_encode=false mode.

Each GitHub Actions matrix runner (one per episode) calls this script
with a --phase argument. Phases map to named workflow steps for clean
logging and failure isolation.

Phases:
    download      → download URL to source.mkv
    probe         → extract metadata, write encode_params.json
    notify_start  → post opening card to TG channel, save message_id.txt
    encode        → SVT-AV1 encode with live TG channel edits every 4s
    vmaf          → VMAF + SSIM, edit channel message
    upload        → Gofile upload, edit channel message
    notify_end    → edit channel message with final completion card
    notify_fail   → edit channel message with failure card + alert TG_CHAT_ID

Environment variables set by batch.yml per matrix runner:
    VIDEO_URL, EPISODE, ANIME_NAME, SEASON, CONTENT_TYPE, AUDIO_TYPE,
    USER_CRF, USER_PRESET, USER_GRAIN, USER_RES, AUDIO_MODE, AUDIO_BITRATE,
    RUN_VMAF, RUN_UPLOAD, ENCODER_TITLE, SUB_TRACKS, AUDIO_TRACKS,
    TG_BOT_TOKEN, TG_CHANNEL_ID, TG_CHAT_ID, TG_API_ID, TG_API_HASH,
    GITHUB_REPOSITORY, GITHUB_RUN_ID
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import config
from utils.media import (
    get_video_info, get_crop_params, async_generate_thumbnail,
    get_vmaf, upload_to_cloud, fmt_size, fmt_duration,
    check_ffmpeg, FFMPEG,
)
from utils.ui import (
    generate_progress_bar, format_time, get_encode_ui, get_vmaf_ui,
)

# ─── TG helpers ───────────────────────────────────────────────────────────────

CHANNEL_ID   = int(os.environ.get("TG_CHANNEL_ID", "0"))
BOT_TOKEN    = os.environ.get("TG_BOT_TOKEN", config.BOT_TOKEN)
ALERT_CHAT   = int(os.environ.get("TG_CHAT_ID", str(config.CHAT_ID)))
MESSAGE_FILE = Path("message_id.txt")
PARAMS_FILE  = Path("encode_params.json")
SOURCE       = Path("source.mkv")


async def _bot_send(text: str) -> int:
    """Post a new message to the channel. Returns message_id."""
    try:
        from pyrogram import Client, enums
        async with Client(
            "br_bot",
            bot_token=BOT_TOKEN,
            api_id=config.API_ID,
            api_hash=config.API_HASH,
        ) as app:
            msg = await app.send_message(
                CHANNEL_ID, text,
                parse_mode=enums.ParseMode.HTML,
            )
            return msg.id
    except Exception as e:
        print(f"[TG] send failed: {e}")
        return 0


async def _bot_edit(message_id: int, text: str, buttons=None) -> None:
    """Edit the episode's channel message."""
    if not message_id:
        return
    try:
        from pyrogram import Client, enums
        async with Client(
            "br_bot",
            bot_token=BOT_TOKEN,
            api_id=config.API_ID,
            api_hash=config.API_HASH,
        ) as app:
            kwargs = dict(parse_mode=enums.ParseMode.HTML)
            if buttons:
                kwargs["reply_markup"] = buttons
            await app.edit_message_text(CHANNEL_ID, message_id, text, **kwargs)
    except Exception as e:
        print(f"[TG] edit failed: {e}")


async def _bot_alert(text: str) -> None:
    """Send a plain alert to the personal/group chat (TG_CHAT_ID)."""
    if not ALERT_CHAT:
        return
    try:
        from pyrogram import Client, enums
        async with Client(
            "br_bot",
            bot_token=BOT_TOKEN,
            api_id=config.API_ID,
            api_hash=config.API_HASH,
        ) as app:
            await app.send_message(
                ALERT_CHAT, text,
                parse_mode=enums.ParseMode.HTML,
            )
    except Exception as e:
        print(f"[TG] alert failed: {e}")


def _load_message_id() -> int:
    try:
        return int(MESSAGE_FILE.read_text().strip())
    except Exception:
        return 0


def _save_message_id(mid: int) -> None:
    MESSAGE_FILE.write_text(str(mid))


def _load_params() -> dict:
    if PARAMS_FILE.exists():
        with open(PARAMS_FILE) as f:
            return json.load(f)
    return {}


# ─── Phase: download ──────────────────────────────────────────────────────────

def phase_download() -> None:
    url = os.environ.get("VIDEO_URL", "").strip()
    if not url:
        print("[ERROR] VIDEO_URL is not set.")
        sys.exit(1)

    print(f"[download] URL: {url}", flush=True)
    # Delegate to the existing download.py which handles all URL types
    result = subprocess.run([sys.executable, "download.py"], check=False)
    if result.returncode != 0:
        print("[ERROR] download.py exited with non-zero code.")
        sys.exit(1)

    if not SOURCE.exists():
        print("[ERROR] source.mkv not found after download.")
        sys.exit(1)

    size_mb = SOURCE.stat().st_size / 1_048_576
    print(f"[download] ✅  source.mkv  ({fmt_size(size_mb)})", flush=True)


# ─── Phase: probe ─────────────────────────────────────────────────────────────

def phase_probe() -> None:
    if not SOURCE.exists():
        print("[ERROR] source.mkv not found — run download phase first.")
        sys.exit(1)

    # get_video_info reads config.SOURCE which is "source.mkv"
    duration, width, height, is_hdr, total_frames, channels, fps_val = get_video_info()

    # Auto-downscale > 1080p if no explicit res
    res = os.environ.get("USER_RES", "").strip()
    if not res and height > 1080:
        res = "1080"
        print(f"[probe] Auto-downscale: {height}p → 1080p")

    crop_val = get_crop_params(duration)

    crf           = int(os.environ.get("USER_CRF",      "50"))
    preset        = int(os.environ.get("USER_PRESET",   "6"))
    grain         = int(os.environ.get("USER_GRAIN",    "0"))
    audio_bitrate = os.environ.get("AUDIO_BITRATE",     "64k")
    episode       = os.environ.get("EPISODE",           "1")

    params = {
        "duration":      duration,
        "width":         width,
        "height":        height,
        "is_hdr":        is_hdr,
        "total_frames":  total_frames,
        "fps_val":       fps_val,
        "crop_val":      crop_val,
        "crf":           crf,
        "preset":        preset,
        "grain":         grain,
        "res":           res,
        "audio_bitrate": audio_bitrate,
        "episode":       episode,
    }

    with open(PARAMS_FILE, "w") as f:
        json.dump(params, f, indent=2)

    print(f"[probe] ✅  encode_params.json written", flush=True)
    print(f"         duration={fmt_duration(duration)}  {width}x{height}  "
          f"hdr={is_hdr}  crop={crop_val}  crf={crf}  preset={preset}", flush=True)


# ─── Phase: notify_start ──────────────────────────────────────────────────────

async def phase_notify_start_async() -> None:
    params  = _load_params()
    episode = os.environ.get("EPISODE",     params.get("episode", "1"))
    crf     = params.get("crf",     os.environ.get("USER_CRF",    "50"))
    preset  = params.get("preset",  os.environ.get("USER_PRESET", "6"))
    height  = params.get("height",  0)
    res     = params.get("res",     os.environ.get("USER_RES", ""))
    res_label = f"{res or height}p" if (res or height) else "?"

    audio_bitrate = params.get("audio_bitrate", os.environ.get("AUDIO_BITRATE", "64k"))
    audio_mode    = os.environ.get("AUDIO_MODE", "opus")
    grain         = params.get("grain", 0)
    hdr_label     = "HDR10" if params.get("is_hdr") else "SDR"
    anime_name    = os.environ.get("ANIME_NAME", "")
    season        = os.environ.get("SEASON",     "1")

    display_name = f"{anime_name} S{int(season):02d}E{int(episode):02d}" if anime_name else f"Episode {episode}"

    card = (
        f"<code>┌─── ⚙️ [ ENCODE.STARTING ] ───────────────┐\n"
        f"│                                            \n"
        f"│ 📺 {display_name}\n"
        f"│ 🛠 CRF: {crf} | Preset: {preset} | {res_label}\n"
        f"│ 🎵 {audio_mode.upper()} @ {audio_bitrate} | Grain: {grain}\n"
        f"│ 🎨 {hdr_label}\n"
        f"│                                            \n"
        f"│ ░░░░░░░░░░░░░░░  0%\n"
        f"│ Waiting for encoder…\n"
        f"└────────────────────────────────────────────┘</code>"
    )

    mid = await _bot_send(card)
    if mid:
        _save_message_id(mid)
        print(f"[notify_start] message_id={mid}", flush=True)
    else:
        print("[notify_start] WARNING: could not post to channel.", flush=True)


def phase_notify_start() -> None:
    asyncio.run(phase_notify_start_async())


# ─── Phase: encode ────────────────────────────────────────────────────────────

async def phase_encode_async() -> None:
    check_ffmpeg()
    params  = _load_params()
    mid     = _load_message_id()

    duration     = params.get("duration",      0.0)
    total_frames = params.get("total_frames",  0)
    fps_val      = params.get("fps_val",       24.0)
    is_hdr       = params.get("is_hdr",        False)
    crop_val     = params.get("crop_val",      None)
    crf          = params.get("crf",           50)
    preset       = params.get("preset",        6)
    grain        = params.get("grain",         0)
    res          = params.get("res",           "")
    audio_bitrate= params.get("audio_bitrate", "64k")
    height       = params.get("height",        0)

    episode    = os.environ.get("EPISODE",    params.get("episode", "1"))
    anime_name = os.environ.get("ANIME_NAME", "")
    season     = os.environ.get("SEASON",     "1")
    audio_mode = os.environ.get("AUDIO_MODE", "opus")

    display_name = f"{anime_name} S{int(season):02d}E{int(episode):02d}" if anime_name else f"Episode {episode}"
    res_label    = f"{res or height}p" if (res or height) else "?"
    hdr_label    = "HDR10" if is_hdr else "SDR"
    crop_label   = " | Cropped" if crop_val else ""
    grain_label  = f" | Grain: {grain}" if grain else ""

    # Build output filename
    config.FILE_NAME = f"output_ep{episode}.mkv"

    # SVT-AV1 params (same logic as encode.py / main.py)
    grain_val = max(0, min(50, int(grain)))
    if duration < 300:   la_depth = 90
    elif duration < 1500: la_depth = 60
    else:                 la_depth = 40
    svtav1_params = (
        f"tune=2:film-grain={grain_val}:enable-overlays=1:"
        f"aq-mode=2:variance-boost-strength=3:variance-octile=6:"
        f"enable-qm=1:qm-min=0:qm-max=8:sharpness=1:"
        f"scd=1:scd-sensitivity=10:enable-tf=1:"
        f"pin=0:lp=2:tile-columns=2:tile-rows=1:la-depth={la_depth}:fast-decode=1"
    )

    # Video filters
    vf_parts = []
    if crop_val:
        vf_parts.append(f"crop={crop_val}")
    if res and str(res).strip().isdigit():
        vf_parts.append(f"scale=-2:{res}:flags=lanczos")
    if is_hdr:
        vf_parts += [
            "zscale=t=linear:npl=100", "format=gbrpf32le",
            "zscale=p=bt709", "tonemap=hable:desat=0",
            "zscale=t=bt709:m=bt709:r=tv", "format=yuv420p10le",
        ]
    vf_args = ["-vf", ",".join(vf_parts)] if vf_parts else []

    # Subtitle handling
    from utils.media import get_all_subtitle_info
    from utils.rename import lang_code_to_name
    _PGS = {"hdmv_pgs_subtitle", "dvd_subtitle", "pgssub", "hdmv_pgs_bitmap"}
    sub_info       = get_all_subtitle_info(SOURCE)
    pgs_exclusions = []
    sub_title_meta = []
    out_sub_idx    = 0
    for i, st in enumerate(sub_info):
        if st.get("codec", "").lower() in _PGS:
            pgs_exclusions += ["-map", f"-0:s:{i}"]
            continue
        lang_name = lang_code_to_name(st.get("lang", "und"))
        sub_title_meta += [f"-metadata:s:s:{out_sub_idx}", f"title={lang_name}"]
        out_sub_idx += 1

    # Audio
    if audio_mode.lower() == "aac":
        audio_cmd = ["-c:a", "aac", "-b:a", audio_bitrate]
    else:
        audio_cmd = ["-c:a", "libopus", "-b:a", audio_bitrate, "-vbr", "on"]

    sdr_tags = [] if is_hdr else [
        "-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709",
    ]

    # GitHub Actions log URL for inline button
    gh_repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_id  = os.environ.get("GITHUB_RUN_ID",     "")
    log_url = f"https://github.com/{gh_repo}/actions/runs/{run_id}" if gh_repo and run_id else None

    try:
        from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        encode_buttons = InlineKeyboardMarkup([[
            InlineKeyboardButton("📋 Actions Log", url=log_url)
        ]]) if log_url else None
    except Exception:
        encode_buttons = None

    cmd = [
        FFMPEG,
        "-i", str(SOURCE),
        "-map", "0:v:0", "-map", "0:a?", "-map", "0:s?",
        *pgs_exclusions,
        *vf_args,
        "-c:v", "libsvtav1",
        "-pix_fmt", "yuv420p10le",
        "-crf", str(crf),
        *sdr_tags,
        "-preset", str(preset),
        "-svtav1-params", svtav1_params,
        "-threads", "0",
        *audio_cmd,
        *sub_title_meta,
        "-c:s", "copy",
        "-map_chapters", "0",
        "-progress", "pipe:1",
        "-nostats",
        "-y", config.FILE_NAME,
    ]

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    import psutil
    start_time        = time.time()
    last_update_time  = 0
    last_progress_pct = -1

    with open(config.LOG_FILE, "w") as f_log:
        async for raw_line in process.stdout:
            line = raw_line.decode("utf-8", errors="replace")
            f_log.write(line)

            if "out_time_ms" in line:
                try:
                    curr_sec = int(line.split("=")[1]) / 1_000_000
                    percent  = min(100.0, (curr_sec / duration) * 100) if duration else 0
                    elapsed  = time.time() - start_time
                    speed    = curr_sec / elapsed if elapsed > 0 else 0
                    fps      = (percent / 100 * total_frames) / elapsed if elapsed > 0 else 0
                    eta      = (elapsed / percent) * (100 - percent) if percent > 0 else 0
                    size_mb  = (
                        os.path.getsize(config.FILE_NAME) / (1024 * 1024)
                        if os.path.exists(config.FILE_NAME) else 0
                    )
                    now      = time.time()
                    # Edit every 4 seconds
                    if now - last_update_time >= 4:
                        last_update_time = now
                        try:
                            sys_cpu = psutil.cpu_percent(interval=None)
                            sys_ram = psutil.virtual_memory().percent
                        except Exception:
                            sys_cpu = sys_ram = None
                        ui = get_encode_ui(
                            display_name, speed, fps, elapsed, eta,
                            curr_sec, duration, percent,
                            crf, preset, res_label,
                            crop_label, hdr_label, grain_label,
                            audio_mode, audio_bitrate, size_mb,
                            cpu=sys_cpu, ram=sys_ram,
                        )
                        await _bot_edit(mid, ui, buttons=encode_buttons)
                except Exception:
                    continue

    await process.wait()

    if process.returncode != 0:
        print(f"[encode] ❌ ffmpeg exited {process.returncode}")
        sys.exit(1)

    size_mb = os.path.getsize(config.FILE_NAME) / (1024 * 1024) if os.path.exists(config.FILE_NAME) else 0
    print(f"[encode] ✅  {config.FILE_NAME}  ({fmt_size(size_mb)})", flush=True)


def phase_encode() -> None:
    asyncio.run(phase_encode_async())


# ─── Phase: vmaf ─────────────────────────────────────────────────────────────

async def phase_vmaf_async() -> None:
    params = _load_params()
    mid    = _load_message_id()

    output_file = config.FILE_NAME if os.path.exists(config.FILE_NAME) else \
        f"output_ep{os.environ.get('EPISODE','1')}.mkv"

    async def _vmaf_progress(payload):
        ui = get_vmaf_ui(payload["vmaf_percent"], payload["fps"], payload["eta"])
        await _bot_edit(mid, ui)

    vmaf_val, ssim_val = await get_vmaf(
        output_file,
        params.get("crop_val"),
        params.get("width",   0),
        params.get("height",  0),
        params.get("duration", 1.0),
        params.get("fps_val",  24.0),
        kv_writer=_vmaf_progress,
    )
    # Store for notify_end
    params["vmaf_val"] = vmaf_val
    params["ssim_val"] = ssim_val
    with open(PARAMS_FILE, "w") as f:
        json.dump(params, f, indent=2)
    print(f"[vmaf] VMAF={vmaf_val}  SSIM={ssim_val}", flush=True)


def phase_vmaf() -> None:
    asyncio.run(phase_vmaf_async())


# ─── Phase: upload ────────────────────────────────────────────────────────────

async def phase_upload_async() -> None:
    params      = _load_params()
    mid         = _load_message_id()
    output_file = config.FILE_NAME if os.path.exists(config.FILE_NAME) else \
        f"output_ep{os.environ.get('EPISODE','1')}.mkv"

    await _bot_edit(mid,
        "<code>┌─── ☁️ [ GOFILE.UPLINK ] ─────────────────┐\n"
        "│ Uploading to Gofile…\n"
        "└────────────────────────────────────────────┘</code>"
    )

    cloud = await upload_to_cloud(output_file)
    params["cloud"] = cloud
    with open(PARAMS_FILE, "w") as f:
        json.dump(params, f, indent=2)
    print(f"[upload] {cloud['source']} — {cloud.get('page', 'N/A')}", flush=True)


def phase_upload() -> None:
    asyncio.run(phase_upload_async())


# ─── Phase: notify_end ────────────────────────────────────────────────────────

async def phase_notify_end_async() -> None:
    params    = _load_params()
    mid       = _load_message_id()
    episode   = os.environ.get("EPISODE",    params.get("episode", "1"))
    anime_name= os.environ.get("ANIME_NAME", "")
    season    = os.environ.get("SEASON",     "1")

    display_name  = f"{anime_name} S{int(season):02d}E{int(episode):02d}" if anime_name else f"Episode {episode}"
    crf           = params.get("crf",           "?")
    preset        = params.get("preset",        "?")
    grain         = params.get("grain",         0)
    is_hdr        = params.get("is_hdr",        False)
    crop_val      = params.get("crop_val",      None)
    audio_bitrate = params.get("audio_bitrate", "64k")
    duration      = params.get("duration",      0.0)
    height        = params.get("height",        0)
    res           = params.get("res",           "")
    vmaf_val      = params.get("vmaf_val",      "N/A")
    ssim_val      = params.get("ssim_val",      "N/A")
    cloud         = params.get("cloud",         {})
    audio_mode    = os.environ.get("AUDIO_MODE", "opus")
    content_type  = os.environ.get("CONTENT_TYPE", "Anime")

    output_file = config.FILE_NAME if os.path.exists(config.FILE_NAME) else \
        f"output_ep{episode}.mkv"
    size_mb = os.path.getsize(output_file) / (1024 * 1024) if os.path.exists(output_file) else 0

    hdr_label   = "HDR10" if is_hdr else "SDR"
    grain_label = f" | Grain: {grain}" if grain else ""
    crop_label  = " | Cropped" if crop_val else ""
    res_label   = f"{res or height}p" if (res or height) else "?"

    cloud_line = ""
    if cloud.get("source") == "gofile" and cloud.get("page"):
        cloud_line = f"\n🔗 <b>GOFILE:</b> {cloud['page']}"
    elif cloud.get("source") == "litterbox" and cloud.get("direct"):
        cloud_line = f"\n🔗 <b>LITTERBOX:</b> {cloud['direct']}"

    gh_repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_id  = os.environ.get("GITHUB_RUN_ID",     "")
    log_url = f"https://github.com/{gh_repo}/actions/runs/{run_id}" if gh_repo and run_id else None

    try:
        from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        btn_row = []
        if log_url:
            btn_row.append(InlineKeyboardButton("📋 Actions Log", url=log_url))
        if cloud.get("source") == "gofile" and cloud.get("page"):
            btn_row.append(InlineKeyboardButton("Gofile", url=cloud["page"]))
        elif cloud.get("source") == "litterbox" and cloud.get("direct"):
            btn_row.append(InlineKeyboardButton("Litterbox", url=cloud["direct"]))
        buttons = InlineKeyboardMarkup([btn_row]) if btn_row else None
    except Exception:
        buttons = None

    final_report = (
        f"✅ <b>EPISODE COMPLETE</b>\n\n"
        f"📺 <b>{display_name}</b>\n"
        f"📄 <b>FILE:</b> <code>{os.path.basename(output_file)}</code>\n"
        f"📦 <b>SIZE:</b> <code>{fmt_size(size_mb)}</code>\n"
        f"⏱ <b>DURATION:</b> <code>{fmt_duration(duration)}</code>\n\n"
        f"📊 <b>QUALITY:</b> VMAF: <code>{vmaf_val}</code> | SSIM: <code>{ssim_val}</code>\n\n"
        f"🛠 <b>SPECS:</b>\n"
        f"└ CRF: {crf} | Preset: {preset} | {res_label}\n"
        f"└ Video: {hdr_label}{crop_label}{grain_label}\n"
        f"└ Audio: {audio_mode.upper()} @ {audio_bitrate}\n"
        f"└ Type: {content_type}"
        f"{cloud_line}"
    )

    await _bot_edit(mid, final_report, buttons=buttons)
    print(f"[notify_end] ✅  Episode {episode} final report sent.", flush=True)


def phase_notify_end() -> None:
    asyncio.run(phase_notify_end_async())


# ─── Phase: notify_fail ───────────────────────────────────────────────────────

async def phase_notify_fail_async(step: str) -> None:
    mid        = _load_message_id()
    episode    = os.environ.get("EPISODE",    "?")
    anime_name = os.environ.get("ANIME_NAME", "")
    season     = os.environ.get("SEASON",     "1")

    display_name = f"{anime_name} S{int(season):02d}E{int(episode):02d}" if anime_name else f"Episode {episode}"

    gh_repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_id  = os.environ.get("GITHUB_RUN_ID",     "")
    log_url = f"https://github.com/{gh_repo}/actions/runs/{run_id}" if gh_repo and run_id else None

    fail_card = (
        f"<code>┌─── ⚠️ [ MISSION.CRITICAL.FAILURE ] ───┐\n"
        f"│                                    \n"
        f"│ 📺 {display_name}\n"
        f"│ ❌ PHASE: {step.upper()} FAILED\n"
        f"│                                    \n"
        f"│ 🛠️ STATUS: Core dumped.\n"
        f"│ 📑 Check Actions log for details.\n"
        f"└────────────────────────────────────┘</code>"
    )

    try:
        from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        buttons = InlineKeyboardMarkup([[
            InlineKeyboardButton("📋 Actions Log", url=log_url)
        ]]) if log_url else None
    except Exception:
        buttons = None

    await _bot_edit(mid, fail_card, buttons=buttons)
    await _bot_alert(
        f"❌ <b>Batch episode failed</b>\n"
        f"Episode: {display_name}\n"
        f"Step: {step}\n"
        f"{'Log: ' + log_url if log_url else ''}"
    )
    print(f"[notify_fail] Failure card sent for episode {episode} at step {step}.", flush=True)


def phase_notify_fail(step: str) -> None:
    asyncio.run(phase_notify_fail_async(step))


# ─── CLI ──────────────────────────────────────────────────────────────────────

def _parse_args():
    p = argparse.ArgumentParser(description="Phased per-episode batch runner")
    p.add_argument("--phase", required=True,
                   choices=["download", "probe", "notify_start", "encode",
                             "vmaf", "upload", "notify_end", "notify_fail"],
                   help="Pipeline phase to execute")
    p.add_argument("--fail-step", default="unknown",
                   help="Step name to include in failure notification")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.phase == "download":       phase_download()
    elif args.phase == "probe":        phase_probe()
    elif args.phase == "notify_start": phase_notify_start()
    elif args.phase == "encode":       phase_encode()
    elif args.phase == "vmaf":         phase_vmaf()
    elif args.phase == "upload":       phase_upload()
    elif args.phase == "notify_end":   phase_notify_end()
    elif args.phase == "notify_fail":  phase_notify_fail(args.fail_step)
