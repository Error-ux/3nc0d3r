import asyncio
import os
import subprocess
import json
from urllib.parse import quote
import time
from collections import Counter

import config


def get_video_info():
    cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", "-show_format", config.SOURCE]
    res = json.loads(subprocess.check_output(cmd).decode())
    video_stream = next(s for s in res['streams'] if s['codec_type'] == 'video')
    audio_stream = next((s for s in res['streams'] if s['codec_type'] == 'audio'), {})

    channels     = int(audio_stream.get('channels', 0))
    duration     = float(res['format'].get('duration', 0))
    width        = int(video_stream.get('width', 0))
    height       = int(video_stream.get('height', 0))

    # Safe fraction parser — never eval() untrusted ffprobe output
    fps_raw = video_stream.get('r_frame_rate', '24/1')
    try:
        if '/' in fps_raw:
            num, den = fps_raw.split('/')
            fps_val = int(num) / int(den)
        else:
            fps_val = float(fps_raw)
    except (ValueError, ZeroDivisionError):
        fps_val = 24.0

    total_frames = int(video_stream.get('nb_frames', duration * fps_val))
    is_hdr       = 'bt2020' in video_stream.get('color_primaries', 'bt709')
    return duration, width, height, is_hdr, total_frames, channels, fps_val


async def async_generate_grid(duration, target_file):
    loop = asyncio.get_event_loop()
    def sync_grid():
        interval      = duration / 10
        select_filter = (
            "select='" +
            "+".join([f"between(t,{i*interval}-0.1,{i*interval}+0.1)" for i in range(1, 10)]) +
            "',setpts=N/FRAME_RATE/TB"
        )
        cmd = [
            "ffmpeg", "-i", target_file,
            "-vf", f"{select_filter},scale=480:-1,tile=3x3",
            "-frames:v", "1", "-q:v", "3", config.SCREENSHOT, "-y"
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    await loop.run_in_executor(None, sync_grid)


def get_crop_params(duration):
    if duration < 10: return None
    test_points    = [duration * 0.15, duration * 0.35, duration * 0.55, duration * 0.75]
    detected_crops = []
    for ts in test_points:
        time_str = time.strftime('%H:%M:%S', time.gmtime(ts))
        cmd = [
            "ffmpeg", "-skip_frame", "nokey", "-ss", time_str,
            "-i", config.SOURCE, "-vframes", "20",
            "-vf", "cropdetect=limit=24:round=2", "-f", "null", "-"
        ]
        try:
            res          = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            found_at_ts  = [line.split("crop=")[1].split(" ")[0] for line in res.stderr.split('\n') if "crop=" in line]
            if found_at_ts: detected_crops.append(Counter(found_at_ts).most_common(1)[0][0])
        except: continue
    if not detected_crops: return None
    most_common_crop, count = Counter(detected_crops).most_common(1)[0]
    if count >= 3:
        w, h, x, y = most_common_crop.split(':')
        if int(x) == 0 and int(y) == 0: return None
        return most_common_crop
    return None


async def get_vmaf(output_file, crop_val, width, height, duration, fps, kv_writer=None):
    """
    Runs VMAF + SSIM analysis.

    kv_writer: optional async callable that accepts a dict payload.
               Receives the same progress_ key format used during encoding,
               but with phase="vmaf" so /p can render the correct box.
               If None, progress updates are silently skipped (no TG edits).
    """
    ref_w, ref_h = width, height
    if crop_val:
        try:
            parts        = crop_val.split(':')
            ref_w, ref_h = parts[0], parts[1]
        except: pass

    interval       = duration / 6
    select_parts   = [
        f"between(t,{(i*interval)+(interval/2)-2.5},{(i*interval)+(interval/2)+2.5})"
        for i in range(6)
    ]
    select_filter   = f"select='{'+'.join(select_parts)}',setpts=N/FRAME_RATE/TB"
    total_vmaf_frames = int(30 * fps)
    ref_filters     = f"crop={crop_val},{select_filter}" if crop_val else select_filter
    dist_filters    = f"{select_filter},scale={ref_w}:{ref_h}:flags=bicubic"

    filter_graph = (
        f"[1:v]{ref_filters}[r];"
        f"[0:v]{dist_filters}[d];"
        f"[d]split=2[d1][d2];"
        f"[r]split=2[r1][r2];"
        f"[d1][r1]libvmaf;"
        f"[d2][r2]ssim"
    )

    cmd = [
        "ffmpeg", "-threads", "0",
        "-i", output_file, "-i", config.SOURCE,
        "-filter_complex", filter_graph,
        "-progress", "pipe:1", "-nostats", "-f", "null", "-"
    ]

    vmaf_score, ssim_score = "N/A", "N/A"

    try:
        proc       = await asyncio.create_subprocess_exec(
            *cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        start_time = time.time()
        last_write = 0

        async def read_progress():
            nonlocal last_write
            while True:
                line = await proc.stdout.readline()
                if not line: break
                line_str = line.decode().strip()
                if line_str.startswith("frame="):
                    try:
                        curr_frame = int(line_str.split("=")[1].strip())
                        percent    = min(100.0, (curr_frame / total_vmaf_frames) * 100)
                        now        = time.time()
                        if kv_writer and (now - last_write > 5):
                            elapsed = now - start_time
                            speed   = curr_frame / elapsed if elapsed > 0 else 0
                            eta     = (total_vmaf_frames - curr_frame) / speed if speed > 0 else 0
                            # Reuses the same progress key so /p shows VMAF phase inline
                            await kv_writer({
                                "phase":        "vmaf",
                                "file":         output_file,
                                "run_id":       config.GITHUB_RUN_ID,
                                "vmaf_percent": round(percent, 1),
                                "fps":          int(speed),
                                "elapsed":      int(elapsed),
                                "eta":          int(eta),
                                "ts":           int(now),
                            })
                            last_write = now
                    except: pass

        async def read_stderr():
            nonlocal vmaf_score, ssim_score
            while True:
                line     = await proc.stderr.readline()
                if not line: break
                line_str = line.decode('utf-8', errors='ignore').strip()
                if "VMAF score:" in line_str:
                    vmaf_score = line_str.split("VMAF score:")[1].strip()
                if "SSIM Y:" in line_str and "All:" in line_str:
                    try:
                        ssim_score = line_str.split("All:")[1].split(" ")[0]
                    except: pass

        await asyncio.gather(read_progress(), read_stderr())
        await proc.wait()
        return vmaf_score, ssim_score

    except:
        return "N/A", "N/A"


def select_params(height):
    if height >= 2000: return 28, 10
    elif height >= 1000: return 42, 6
    elif height >= 700:  return 32, 6
    return 24, 4



async def upload_to_cloud(filepath, app=None, chat_id=None, status_msg=None):
    """
    Uploads to Gofile (primary) and returns a dict:
        {
            "page":   "https://gofile.io/d/{id}",
            "source": "gofile" | "litterbox" | "error"
        }
    """
    filename = os.path.basename(filepath)

    # ── Step 1: Get best upload server ──────────────────────────────────────
    try:
        server_proc = await asyncio.create_subprocess_exec(
            "curl", "-s", "https://api.gofile.io/servers",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        server_out, _ = await server_proc.communicate()
        server_data   = json.loads(server_out.decode())

        if server_data.get("status") != "ok":
            raise ValueError(f"Gofile server API error: {server_data}")

        server = server_data["data"]["servers"][0]["name"]

    except Exception as e:
        print(f"[Gofile] Step 1 failed: {e}")
        return await _litterbox_fallback(filepath)

    # ── Step 2: Upload file with progress ───────────────────────────────────
    try:
        file_size     = os.path.getsize(filepath)
        last_edit     = 0
        last_pct      = -1
        upload_proc   = await asyncio.create_subprocess_exec(
            "curl", "-s",
            "--progress-bar",
            "-F", f"file=@{filepath}",
            f"https://{server}.gofile.io/contents/uploadfile",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        # Read stderr for curl progress while stdout accumulates JSON response
        uploaded_bytes = 0
        start_up       = time.time()

        async def _read_progress():
            nonlocal uploaded_bytes, last_edit, last_pct
            async for line in upload_proc.stderr:
                # curl --progress-bar writes lines like: "##  3.1%  ..."
                text = line.decode("utf-8", errors="ignore").strip()
                # parse percentage from curl progress output
                parts = text.split()
                for part in parts:
                    if part.endswith("%"):
                        try:
                            pct = float(part.rstrip("%"))
                            uploaded_bytes = int(file_size * pct / 100)
                            now         = time.time()
                            pct_crossed = int(pct // 5) * 5 > last_pct
                            time_due    = now - last_edit >= 30
                            if app and status_msg and (pct_crossed or time_due):
                                last_pct  = int(pct // 5) * 5
                                elapsed   = now - start_up
                                speed_mbs = (uploaded_bytes / elapsed) / (1024*1024) if elapsed > 0 else 0
                                eta       = ((file_size - uploaded_bytes) / (uploaded_bytes / elapsed)) if uploaded_bytes > 0 else 0
                                from ui import generate_progress_bar, format_time
                                bar = generate_progress_bar(pct)
                                ui  = (
                                    f"<code>┌─── ☁️ [ GOFILE.UPLINK ] ───────────┐\n"
                                    f"│                                    \n"
                                    f"│ 📂 FILE: {os.path.basename(filepath)}\n"
                                    f"│ 📊 PROG: {bar} {pct:.1f}%\n"
                                    f"│ 📦 SIZE: {uploaded_bytes/(1024*1024):.1f} / {file_size/(1024*1024):.1f} MB\n"
                                    f"│ ⚡ SPEED: {speed_mbs:.2f} MB/s\n"
                                    f"│ ⏳ ETA: {format_time(eta)}\n"
                                    f"│                                    \n"
                                    f"└────────────────────────────────────┘</code>"
                                )
                                try:
                                    from pyrogram import enums as _enums
                                    await app.edit_message_text(chat_id, status_msg.id, ui, parse_mode=_enums.ParseMode.HTML)
                                    last_edit = now
                                except Exception:
                                    pass
                        except ValueError:
                            pass

        await asyncio.gather(_read_progress(), asyncio.shield(upload_proc.wait()))
        upload_out = await upload_proc.stdout.read()
        upload_data   = json.loads(upload_out.decode())

        if upload_data.get("status") != "ok":
            raise ValueError(f"Gofile upload error: {upload_data}")

        file_id    = upload_data["data"]["id"]
        page_url   = upload_data["data"]["downloadPage"]

        # Direct link: https://{server}.gofile.io/download/web/{fileId}/{fileName}
        encoded_name = quote(filename, safe="")
        direct_url   = f"https://{server}.gofile.io/download/web/{file_id}/{encoded_name}"

        return {
            "direct": direct_url,
            "page":   page_url,
            "source": "gofile"
        }

    except Exception as e:
        print(f"[Gofile] Step 2 failed: {e}")
        return await _litterbox_fallback(filepath)


async def _litterbox_fallback(filepath):
    """Fallback uploader: litterbox.catbox.moe — stable, no size cap under 1 GB."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "curl", "-s",
            "-F", "reqtype=fileupload",
            "-F", "time=72h",
            "-F", f"fileToUpload=@{filepath}",
            "https://litterbox.catbox.moe/resources/internals/api.php",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        url       = stdout.decode().strip()
        if url.startswith("https://"):
            return {"direct": url, "page": url, "source": "litterbox"}
    except Exception as e:
        print(f"[Litterbox] Fallback failed: {e}")

    return {"direct": None, "page": None, "source": "error"}
