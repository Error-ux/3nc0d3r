import asyncio
import os
import subprocess
import time
import shutil
import psutil
from pyrogram import enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

import config
from media import get_video_info, get_crop_params, async_generate_thumbnail, get_vmaf, upload_to_cloud
from rename import lang_code_to_name
from ui import get_encode_ui, format_time, upload_progress, get_vmaf_ui
from rename import resolve_output_name, format_track_report
from tg_utils import connect_telegram, tg_edit, tg_notify_failure


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
        duration, width, height, is_hdr, total_frames, _, fps_val = get_video_info()
    except Exception as e:
        print(f"Metadata error: {e}")
        # TG not up yet — spin up a minimal client just to fire the alert
        _tg_s: dict = {}
        _tg_r = asyncio.Event()
        await connect_telegram(_tg_s, _tg_r, config.FILE_NAME)
        await tg_notify_failure(_tg_s, _tg_r, config.FILE_NAME,
                                f"Metadata extraction failed: {e}")
        _a = _tg_s.get("app")
        if _a:
            await _a.stop()
        return

    # 2b. AUTO-DOWNSCALE — cap sources above 1080p to 1080p when no explicit
    # resolution was requested. This prevents enormous 4K encodes on runners
    # with limited disk.  USER_RES overrides this entirely (user is explicit).
    _source_height   = height          # keep original for display label
    _auto_downscaled = False
    if not (config.USER_RES and config.USER_RES.strip().isdigit()) and height > 1080:
        config.USER_RES  = "1080"
        _auto_downscaled = True
        print(f"[auto-scale] Source is {height}p — exceeds 1080p, capping to 1080p")

    # 3. RENAME — build structured output filename if ANIME_NAME is set.
    # If ANIME_NAME is blank, attempt to auto-parse it from the source URL's
    # filename= query param (or path) using anitopy as a fallback.
    # Skip entirely for anibd.app downloads — filename is already final.
    anime_name = config.ANIME_NAME.strip() if config.ANIME_NAME else ""
    is_special = False
    _managed_source = (
        os.path.exists("anibd_source.txt") or
        os.path.exists("iwara_source.txt")
    )

    if not anime_name and not _managed_source:
        # ── Auto-detect from filename (anitopy) ────────────────────────────
        # Priority: FILE_NAME (Content-Disposition) → VIDEO_URL query param → URL path
        from urllib.parse import urlparse, parse_qs, unquote
        from rename import parse_from_filename

        raw_filename = ""

        # Best source: filename resolved by resolve_filename.py in the workflow
        if config.FILE_NAME and any(c.isalpha() for c in config.FILE_NAME):
            raw_filename = config.FILE_NAME

        # Fallback: extract from VIDEO_URL query param / path
        if not raw_filename:
            source_url = os.getenv("VIDEO_URL", "")
            if source_url:
                qs = parse_qs(urlparse(source_url).query)
                raw_filename = (
                    qs.get("filename", [None])[0]
                    or qs.get("file",     [None])[0]
                    or unquote(urlparse(source_url).path.split("/")[-1])
                    or ""
                )

        if raw_filename:
            parsed = parse_from_filename(raw_filename)
            if parsed:
                anime_name = parsed["anime_name"]
                is_special = parsed["is_special"]
                # Rename OFF: bridge's episode/season are sequential placeholders —
                # always trust what was detected from the actual filename.
                config.SEASON  = str(parsed["season"])
                config.EPISODE = str(parsed["episode"])

    if anime_name:
        rename_height = int(config.USER_RES) if (config.USER_RES and config.USER_RES.strip().isdigit()) else height
        resolved_name, audio_type_label, audio_tracks, sub_tracks = resolve_output_name(
            source               = config.SOURCE,
            anime_name           = anime_name,
            season               = config.SEASON,
            episode              = config.EPISODE,
            height               = rename_height,
            audio_type_override  = config.AUDIO_TYPE,
            content_type         = config.CONTENT_TYPE,
            is_special           = is_special,
        )
        config.FILE_NAME = resolved_name
        print(f"[rename] Output → {resolved_name}  |  Audio: {audio_type_label}")
    else:
        # No rename requested — probe tracks for report only
        from rename import get_track_info
        audio_tracks, sub_tracks = get_track_info(config.SOURCE)
        audio_type_label = None

    # 4. PARAMETER CONFIGURATION
    # CRF and preset come directly from bridge inputs — no auto-selection.
    # Bridge always sends explicit values (defaults: CRF 50, Preset 8).
    final_crf    = config.USER_CRF    if (config.USER_CRF    and config.USER_CRF.strip())    else "48"
    final_preset = config.USER_PRESET if (config.USER_PRESET and config.USER_PRESET.strip()) else "6"

    res_label = config.USER_RES if (config.USER_RES and config.USER_RES.strip()) else None
    crop_val  = get_crop_params(duration)

    # -- VIDEO FILTERS --
    # Correct filter order: crop → scale → tonemap (HDR only) → hqdn3d.
    # Crop removes unwanted pixels first, scale resizes only what's kept,
    # tonemapping converts HDR10 → SDR before denoise runs on the final pixels.
    vf_filters = []
    if crop_val: vf_filters.append(f"crop={crop_val}")
    if res_label: vf_filters.append(f"scale=-2:{res_label}:flags=lanczos")  # lanczos preserves anime line art better than bicubic
    if is_hdr:
        # HDR10 → SDR tonemap: convert to linear light, apply hable tonemap,
        # then back to bt709 for SDR display. Requires zscale + tonemap + zscale.
        vf_filters += [
            "zscale=t=linear:npl=100",
            "format=gbrpf32le",
            "zscale=p=bt709",
            "tonemap=hable:desat=0",
            "zscale=t=bt709:m=bt709:r=tv",
            "format=yuv420p10le",
        ]
    video_filters = ["-vf", ",".join(vf_filters)] if vf_filters else []

    # Display label — show actual source height when no downscale requested.
    # When auto-downscaled show both output and original (e.g. "1080p (↓4K)").
    from rename import detect_quality
    if _auto_downscaled:
        res_label = f"1080p (↓{detect_quality(_source_height)})"
    else:
        res_label = res_label or f"Original({detect_quality(height)})"

    # -- AUDIO CONFIGURATION --
    # Always re-encode to Opus at the configured bitrate.
    final_audio_bitrate = config.AUDIO_BITRATE if (config.AUDIO_BITRATE and config.AUDIO_BITRATE.strip()) else "48k"
    audio_cmd = ["-af", "aformat=channel_layouts=stereo", "-c:a", "libopus",
                 "-b:a", final_audio_bitrate, "-vbr", "on"]
    print(f"[audio] Re-encoding to Opus @ {final_audio_bitrate}")

    # -- SVT-AV1 PARAMETERS --
    # pin=0 is required for GitHub Actions (virtualized VMs don't honour CPU affinity).
    # Without it SVT-AV1 tries to pin threads to specific cores and hangs indefinitely.
    # Film grain — use the user's setting, clamped to valid SVT-AV1 range (0–50)
    try:
        grain_val = max(0, min(50, int(config.USER_GRAIN or 0)))
    except (ValueError, TypeError):
        grain_val = 0

    # -- DYNAMIC LA-DEPTH --
    # Scale lookahead to content length. la-depth=60 is the original safe default.
    # Only bump up for short content where extra lookahead won't cause timeouts.
    if duration < 300:       # < 5 min  — shorts, OPs, EDs, demos
        la_depth = 90
    elif duration < 1500:    # < 25 min — standard episodes
        la_depth = 60
    else:                    # 25 min+  — movies, long OVAs
        la_depth = 40
    print(f"[svtav1] la-depth={la_depth} (duration={duration:.0f}s)")

    # -- SVT-AV1 PARAMETERS --
    # Optimizing for 4-core GitHub Action Runners:
    # lp=2: Better workload distribution than lp=0 on 4 cores.
    # tile-columns=1: Parallelizes frame processing.
    # fast-decode=1: Speeds up the internal loops without hitting quality.
    svtav1_tune = (
        f"tune=2:film-grain={grain_val}:enable-overlays=1:"
        f"aq-mode=2:variance-boost-strength=3:variance-octile=6:"
        f"enable-qm=1:qm-min=0:qm-max=8:sharpness=1:"
        f"scd=1:scd-sensitivity=10:enable-tf=1:"
        f"pin=0:lp=2:tile-columns=2:tile-rows=1:la-depth={la_depth}:"
        f"fast-decode=1"
    )


    # UI Labels
    hdr_label      = "HDR10" if is_hdr else "SDR"
    grain_label    = f" | Grain: {grain_val}"
    crop_label_txt = " | Cropped" if crop_val else ""

    # -- DEMO / PARTIAL ENCODE --
    # When DEMO_DURATION is set, override the progress-tracking duration and
    # inject -ss / -t into the FFmpeg command so only that slice is encoded.
    demo_mode     = bool(config.DEMO_DURATION and config.DEMO_DURATION.strip())
    demo_start    = config.DEMO_START.strip() if config.DEMO_START else "0"
    demo_duration = config.DEMO_DURATION.strip() if demo_mode else None

    if demo_mode:
        # Convert demo_start and demo_duration to seconds for progress math.
        def _hms_to_sec(val: str) -> float:
            parts = val.split(":")
            if len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
            return float(val)

        demo_start_sec    = _hms_to_sec(demo_start)
        demo_duration_sec = _hms_to_sec(demo_duration)
        # Clamp so we don't exceed the source
        demo_duration_sec = min(demo_duration_sec, duration - demo_start_sec)
        # Override duration so progress % is calculated against the slice only
        duration   = demo_duration_sec
        print(f"[DEMO MODE] Encoding {demo_duration_sec:.0f}s from {demo_start_sec:.0f}s")

    demo_label = f" | ⚡ DEMO {demo_duration}s" if demo_mode else ""

    # 4. LAUNCH TG AUTH AS A BACKGROUND TASK — encoding starts immediately.
    # If FloodWait fires, connect_telegram sleeps it out on its own while
    # FFmpeg keeps running. Progress messages are sent the instant TG is ready.
    tg_state = {}
    tg_ready = asyncio.Event()
    tg_task  = asyncio.create_task(
        connect_telegram(tg_state, tg_ready, config.FILE_NAME)
    )
    tg_connect_start = time.time()   # record when we started waiting for TG

    # Build action buttons once — shown on every progress edit during encoding.
    # Button 1: URL → opens GitHub Actions log directly (no callback needed).
    # Button 2: kill_{run_id} → bridge Worker cancels the run.
    _gh_repo = os.getenv("GITHUB_REPOSITORY", "")
    _run_id  = config.GITHUB_RUN_ID
    _log_url = f"https://github.com/{_gh_repo}/actions/runs/{_run_id}"
    encode_buttons = InlineKeyboardMarkup([[
        InlineKeyboardButton("📋 Open Log",  url=_log_url),
        InlineKeyboardButton("🛑 Terminate", callback_data=f"kill_{_run_id}"),
    ]])

    # 5. ENCODING EXECUTION (starts immediately, does not wait for TG)

    # -- PGS SUBTITLE REMOVAL --
    # PGS (hdmv_pgs_bitmap / pgssub) are bitmap image subtitles — large and
    # uneditable. Strip all of them from the output.

    def _is_pgs(codec: str) -> bool:
        return "pgs" in codec.lower()

    pgs_exclusions: list[str] = []

    for sub_idx, st in enumerate(sub_tracks):
        if not _is_pgs(st.get("codec", "")):
            continue
        pgs_exclusions += ["-map", f"-0:s:{sub_idx}"]
        print(f"[pgs] Stripping PGS s:{sub_idx} (lang: {st['lang']}, title: '{st['title']}')")

    if pgs_exclusions:
        print(f"[encode] {len(pgs_exclusions)//2} PGS track(s) stripped")

    # -- SUBTITLE TITLE RENAME --
    # Set each kept native (non-PGS) subtitle track's title to its language name.
    sub_title_meta: list[str] = []
    out_sub_idx = 0
    for st in sub_tracks:
        if _is_pgs(st.get("codec", "")):
            continue   # all PGS removed — either stripped or replaced by ASS via ocr_meta
        lang_name = lang_code_to_name(st["lang"])
        sub_title_meta += [f"-metadata:s:s:{out_sub_idx}", f"title={lang_name}"]
        print(f"[encode] Subtitle #s:{out_sub_idx} title set to '{lang_name}' (lang: {st['lang']})")
        out_sub_idx += 1

    cmd = [
        "ffmpeg",
        # Input-side seeking (fast; placed BEFORE -i)
        *([ "-ss", demo_start, "-t", demo_duration ] if demo_mode else []),
        "-i", config.SOURCE,
        "-map", "0:v:0",
        "-map", "0:a?",
        "-map", "0:s?",
        *pgs_exclusions,          # exclude original PGS streams
        *video_filters,
        "-c:v", "libsvtav1",
        "-pix_fmt", "yuv420p10le",
        "-crf", str(final_crf),
        # Explicit SDR color tagging — prevents players from misreading levels
        *(["-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709"] if not is_hdr else []),
        "-preset", str(final_preset),
        "-svtav1-params", svtav1_tune,
        "-threads", "0",
        *audio_cmd,
        *sub_title_meta,          # rename native subtitle titles
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
            # Cancel is handled externally: the bridge Worker cancels the GitHub
            # run via API (kill_{run_id}), which terminates this process directly.
            # config.CANCELLED is never set — the check below is removed.

            if "out_time_ms" in line:
                try:
                    curr_sec = int(line.split("=")[1]) / 1_000_000
                    percent  = (curr_sec / duration) * 100
                    elapsed  = time.time() - start_time
                    speed    = curr_sec / elapsed if elapsed > 0 else 0
                    fps      = (percent / 100 * total_frames) / elapsed if elapsed > 0 else 0
                    eta      = (elapsed / percent) * (100 - percent) if percent > 0 else 0
                    size_mb  = os.path.getsize(config.FILE_NAME) / (1024 * 1024) if os.path.exists(config.FILE_NAME) else 0

                    milestone   = int(percent // 10) * 10
                    now         = time.time()
                    pct_crossed = milestone > last_progress_pct
                    time_due    = now - last_update_time >= 25

                    scifi_ui     = get_encode_ui(
                        config.FILE_NAME, speed, fps, elapsed, eta,
                        curr_sec, duration, percent,
                        final_crf, final_preset, res_label,
                        crop_label_txt, hdr_label, grain_label,
                        config.AUDIO_MODE, final_audio_bitrate, size_mb,
                        cpu=monitor_stats.get("sys_cpu"),
                        ram=monitor_stats.get("sys_ram"),
                        demo_label=demo_label,
                    )
                    last_ui_text = scifi_ui   # always keep the freshest snapshot

                    if pct_crossed or time_due:
                        last_progress_pct = milestone
                        last_update_time  = now
                        # Only sends if TG is already ready; otherwise silently buffered
                        await tg_edit(tg_state, tg_ready, scifi_ui, reply_markup=encode_buttons)

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
            await tg_edit(tg_state, tg_ready, last_ui_text, reply_markup=encode_buttons)

        # 6. ERROR HANDLING
        if process.returncode != 0:
            error_snippet = (
                "".join(open(config.LOG_FILE).readlines()[-10:])
                if os.path.exists(config.LOG_FILE)
                else "Unknown Engine Crash."
            )
            await tg_notify_failure(tg_state, tg_ready, config.FILE_NAME, error_snippet)
            return

        # 7. POST-PROCESSING (Remux)
        await tg_edit(tg_state, tg_ready, "<b>[ SYSTEM.OPTIMIZE ] Finalizing Metadata...</b>")
        fixed_file = f"FIXED_{config.FILE_NAME}"
        mkvmerge_title_args = ["--title", config.ENCODER_TITLE] if config.ENCODER_TITLE.strip() else []
        mkvmerge_result = subprocess.run([
            "mkvmerge", "-o", fixed_file,
            *mkvmerge_title_args,
            config.FILE_NAME,
            "--no-video", "--no-audio", "--no-subtitles", "--no-attachments", config.SOURCE
        ], capture_output=True, text=True)
        if mkvmerge_result.returncode != 0:
            # mkvmerge failed (e.g. missing libmatroska) — log and skip remux.
            # The encoded file is still valid; continue without chapter/attachment merge.
            print(f"[mkvmerge] WARNING: remux failed (exit {mkvmerge_result.returncode}): "
                  f"{mkvmerge_result.stderr.strip()[-200:]}")
            if os.path.exists(fixed_file):
                os.remove(fixed_file)  # partial output
        elif os.path.exists(fixed_file):
            os.remove(config.FILE_NAME)
            os.rename(fixed_file, config.FILE_NAME)

        # 8. METRICS + CLOUD UPLOAD (concurrent)
        final_size = os.path.getsize(config.FILE_NAME) / (1024 * 1024)

        grid_task = asyncio.create_task(async_generate_thumbnail(duration, config.FILE_NAME))

        if config.RUN_UPLOAD:
            await tg_edit(tg_state, tg_ready, "<b>[ SYSTEM.CLOUD ] Uploading to Gofile...</b>")
            cloud_task = asyncio.create_task(upload_to_cloud(config.FILE_NAME, app, config.CHAT_ID, status))
        else:
            cloud_task = None

        if config.RUN_VMAF:
            async def vmaf_tg_writer(payload):
                ui = get_vmaf_ui(payload["vmaf_percent"], payload["fps"], payload["eta"])
                await tg_edit(tg_state, tg_ready, ui)

            vmaf_val, ssim_val = await get_vmaf(config.FILE_NAME, crop_val, width, height, duration, fps_val, kv_writer=vmaf_tg_writer)
        else:
            vmaf_val, ssim_val = "N/A", "N/A"

        await grid_task
        cloud = await cloud_task if cloud_task else {"direct": None, "page": None, "source": "disabled"}

        # 9. Build inline buttons from cloud result
        btn_row = []
        if cloud["source"] == "gofile":
            if cloud.get("page"):
                btn_row.append(InlineKeyboardButton("Gofile", url=cloud["page"]))
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
            # Cleanup even on overflow — runner disk is finite
            for _f in [config.SOURCE, config.FILE_NAME, config.LOG_FILE, config.SCREENSHOT,
                       "anibd_source.txt", "iwara_source.txt"]:
                if os.path.exists(_f):
                    try: os.remove(_f)
                    except: pass
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

        import ui as _ui; _ui.last_up_pct = -1; _ui.last_up_update = 0; _ui.up_start_time = 0

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

        # CLEANUP
        try: await status.delete()
        except: pass
        for f in [config.SOURCE, config.FILE_NAME, config.LOG_FILE, config.SCREENSHOT,
                   "anibd_source.txt", "iwara_source.txt"]:
            if os.path.exists(f): os.remove(f)

    except Exception as exc:
        import traceback
        tb = traceback.format_exc()
        print(f"[FATAL] Unexpected error: {exc}\n{tb}")
        elapsed_total = time.time() - start_time
        reason = (
            f"Unexpected error after {format_time(elapsed_total)}:\n"
            f"{type(exc).__name__}: {exc}\n\n"
            f"{tb[-300:]}"
        )
        await tg_notify_failure(tg_state, tg_ready, config.FILE_NAME, reason)
    finally:
        if app:
            await app.stop()


if __name__ == "__main__":
    asyncio.run(main())