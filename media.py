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

    total_frames = int(round(float(video_stream.get('nb_frames') or 0) or duration * fps_val))
    is_hdr       = 'bt2020' in video_stream.get('color_primaries', 'bt709')
    return duration, width, height, is_hdr, total_frames, channels, fps_val


async def async_generate_thumbnail(duration, target_file):
    loop = asyncio.get_event_loop()
    def sync_thumbnail():
        ts  = duration * 0.25
        cmd = [
            "ffmpeg", "-ss", str(ts), "-i", target_file,
            "-vf", "scale=480:-1",
            "-frames:v", "1", "-q:v", "3", config.SCREENSHOT, "-y"
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    await loop.run_in_executor(None, sync_thumbnail)


def get_crop_params(duration):
    if duration < 10: return None
    test_points    = [duration * 0.05, duration * 0.20, duration * 0.40, duration * 0.60, duration * 0.80, duration * 0.90]
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
    if count >= 4:
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






# ---------------------------------------------------------------------------
# MINI ENCODE — VMAF-GUIDED CRF OPTIMISER
# ---------------------------------------------------------------------------

async def find_optimal_crf(
    source:        str,
    duration:      float,
    target_vmaf:   float,
    max_size_mb:   float,
    preset:        str,
    svtav1_params: str,
    vf_filters:    list,   # list of individual filter strings (not yet joined)
    audio_cmd:     list,   # full ffmpeg audio arg list
    sample_dur:    float = 30.0,
) -> str:
    """
    Probes 4 CRF values on a 30-second sample from the middle of the video,
    then returns the highest CRF that still meets target_vmaf.
    Falls back to a size-based pick if no probe clears the VMAF bar.

    Returns the recommended CRF as a string.
    """
    # Draw the sample from the 40–50% mark to avoid cold opens / credits.
    sample_start = max(0.0, duration * 0.40)
    sample_dur   = min(sample_dur, duration - sample_start)
    if sample_dur < 10:
        print("[mini] Source too short for CRF probe — using CRF 55.")
        return "55"

    # CRF ladder: low→high (high = smaller file, lower quality)
    crf_ladder  = [48, 54, 60, 66]
    # Use one preset step faster for probing to cut overhead
    probe_preset = str(min(int(preset) + 2, 12))

    print(
        f"[mini] CRF probe | sample {sample_start:.0f}s+{sample_dur:.0f}s | "
        f"VMAF target {target_vmaf} | size cap {max_size_mb}MB"
    )

    results: list[tuple[int, float, float]] = []   # (crf, vmaf, projected_mb)

    for crf in crf_ladder:
        vmaf, bitrate_kbps = await _probe_crf(
            source, sample_start, sample_dur,
            crf, probe_preset, svtav1_params, vf_filters, audio_cmd,
        )
        if vmaf is None:
            print(f"[mini]   CRF {crf}: probe failed — skipping")
            continue

        projected_mb = (bitrate_kbps * duration) / (8 * 1024)
        vmaf_ok = vmaf  >= target_vmaf
        size_ok = projected_mb <= max_size_mb
        print(
            f"[mini]   CRF {crf}: VMAF {vmaf:.1f} {'✓' if vmaf_ok else '✗'}  "
            f"~{projected_mb:.1f}MB {'✓' if size_ok else '✗'}"
        )
        results.append((crf, vmaf, projected_mb))

    if not results:
        print("[mini] All probes failed — using CRF 55.")
        return "55"

    # Priority 1: highest CRF where BOTH vmaf≥target AND size≤cap
    both_ok = [(c, v, m) for c, v, m in results if v >= target_vmaf and m <= max_size_mb]
    if both_ok:
        best = max(both_ok, key=lambda x: x[0])
        print(f"[mini] ✅ Optimal CRF {best[0]}  VMAF {best[1]:.1f}  ~{best[2]:.1f}MB")
        return str(best[0])

    # Priority 2: highest CRF where vmaf≥target (size over cap, but quality is right)
    vmaf_ok = [(c, v, m) for c, v, m in results if v >= target_vmaf]
    if vmaf_ok:
        best = max(vmaf_ok, key=lambda x: x[0])
        print(
            f"[mini] ⚠️  CRF {best[0]} meets VMAF {target_vmaf} "
            f"but projected size {best[2]:.1f}MB > cap {max_size_mb}MB"
        )
        return str(best[0])

    # Priority 3: nothing hits VMAF — pick highest VMAF we measured
    best = max(results, key=lambda x: x[1])
    print(
        f"[mini] ⚠️  VMAF target unreachable — "
        f"best: CRF {best[0]}  VMAF {best[1]:.1f}  ~{best[2]:.1f}MB"
    )
    return str(best[0])


async def _probe_crf(
    source:        str,
    start:         float,
    clip_dur:      float,
    crf:           int,
    preset:        str,
    svtav1_params: str,
    vf_filters:    list,
    audio_cmd:     list,
) -> tuple:
    """
    Encode a short clip at the given CRF.
    Returns (vmaf_score, bitrate_kbps) or (None, None) on failure.
    """
    probe_file = f"_probe_crf{crf}.mkv"
    vf_arg     = ["-vf", ",".join(vf_filters)] if vf_filters else []

    cmd = [
        "ffmpeg", "-y",
        "-ss", str(start), "-t", str(clip_dur),
        "-i", source,
        *vf_arg,
        "-c:v", "libsvtav1",
        "-pix_fmt", "yuv420p10le",
        "-crf", str(crf),
        "-preset", preset,
        "-svtav1-params", svtav1_params,
        "-threads", "0",
        *audio_cmd,
        "-an" if "-c:a" not in audio_cmd else "",   # handled below
        probe_file,
    ]
    # Remove the spurious "-an" if audio_cmd is already present
    cmd = [x for x in cmd if x]

    # Rebuild cleanly: audio is handled by audio_cmd, so don't add -an
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(start), "-t", str(clip_dur),
        "-i", source,
        *vf_arg,
        "-c:v", "libsvtav1",
        "-pix_fmt", "yuv420p10le",
        "-crf", str(crf),
        "-preset", preset,
        "-svtav1-params", svtav1_params,
        "-threads", "0",
        *audio_cmd,
        probe_file,
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        await asyncio.wait_for(proc.communicate(), timeout=300)

        if not os.path.exists(probe_file) or os.path.getsize(probe_file) == 0:
            return None, None

        size_bytes   = os.path.getsize(probe_file)
        bitrate_kbps = (size_bytes * 8) / (clip_dur * 1000)
        vmaf_score   = await _quick_vmaf(probe_file, source, start, clip_dur)
        return vmaf_score, bitrate_kbps

    except Exception as exc:
        print(f"[mini] _probe_crf error at CRF {crf}: {exc}")
        return None, None
    finally:
        if os.path.exists(probe_file):
            try:
                os.remove(probe_file)
            except OSError:
                pass


async def _quick_vmaf(
    encoded:   str,
    reference: str,
    ref_start: float,
    clip_dur:  float,
) -> float | None:
    """
    Run a fast single-pass VMAF on a short clip.
    encoded   — the probe output (already starts at t=0)
    reference — the original source file (must seek to ref_start)
    """
    cmd = [
        "ffmpeg", "-threads", "0",
        "-i", encoded,
        "-ss", str(ref_start), "-t", str(clip_dur), "-i", reference,
        "-filter_complex", "[0:v][1:v]libvmaf=n_threads=4:log_fmt=xml",
        "-f", "null", "-"
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=180)
        output = stderr.decode("utf-8", errors="ignore")
        for line in output.splitlines():
            if "VMAF score:" in line:
                return float(line.split("VMAF score:")[1].strip())
    except Exception as exc:
        print(f"[mini] _quick_vmaf error: {exc}")
    return None


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

        page_url = upload_data["data"]["downloadPage"]

        # Use downloadPage only — direct URL is tied to the upload server node
        # which may rotate or differ from the CDN edge serving downloads.
        return {
            "direct": page_url,
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
