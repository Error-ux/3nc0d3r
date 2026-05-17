import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))  # repo root

import time
from datetime import timedelta, datetime, timezone
import os
from pyrogram import enums

last_up_update = 0
last_up_pct    = -1
up_start_time  = 0

# Spinner frames cycled by wall-clock seconds for in-progress chunks
_SPINNERS = ["⚙️ ", "🔄", "⚙️ ", "⏳"]

def _spinner(now_utc: datetime | None) -> str:
    if now_utc is None:
        return "⚙️ "
    idx = int(now_utc.second / 2) % len(_SPINNERS)
    return _SPINNERS[idx]

def generate_progress_bar(percentage):
    total_segments = 15
    completed = int((max(0, min(100, percentage)) / 100) * total_segments)
    return "[" + "▰" * completed + "▱" * (total_segments - completed) + "]"

def format_time(seconds):
    return str(timedelta(seconds=int(seconds))).zfill(8)

def _chunk_elapsed(job: dict, now_utc: datetime) -> str:
    """Return MM:SS elapsed for an in-progress job, or '' if unavailable."""
    started = job.get("started_at")
    if not started:
        return ""
    try:
        s = datetime.strptime(started, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        secs = int((now_utc - s).total_seconds())
        if secs < 0:
            return ""
        m, s2 = divmod(secs, 60)
        return f"{m}:{s2:02d}"
    except Exception:
        return ""

def get_vmaf_ui(percent, speed, eta):
    bar = generate_progress_bar(percent)
    return (
        f"<code>┌─── 🧠 [ SYSTEM.ANALYSIS ] ───┐\n"
        f"│                                    \n"
        f"│ 🔬 METRICS: VMAF + SSIM (30s)\n"
        f"│ 📊 PROG: {bar} {percent:.1f}%\n"
        f"│ ⚡ SPEED: {speed:.1f} FPS\n"
        f"│ ⏳ ETA: {format_time(eta)}\n"
        f"│                                    \n"
        f"└────────────────────────────────────┘</code>"
    )

def get_failure_ui(file_name, error_snippet, phase="ENCODE"):
    phase_icons = {"DOWNLOAD": "📥", "ENCODE": "⚙️", "UPLOAD": "☁️"}
    icon = phase_icons.get(phase.upper(), "❌")
    return (
        f"<code>┌─── ⚠️ [ MISSION.CRITICAL.FAILURE ] ───┐\n"
        f"│                                    \n"
        f"│ 📂 FILE: {file_name}\n"
        f"│ {icon} PHASE: {phase.upper()} FAILED\n"
        f"│ ❌ ERROR DETECTED:\n"
        f"│ {error_snippet[:200]}\n"
        f"│                                    \n"
        f"│ 🛠️ STATUS: Core dumped. \n"
        f"│ 📑 Check the attached log for details.\n"
        f"└────────────────────────────────────┘</code>"
    )


def get_download_ui(percent, speed, size_mb, elapsed, eta):
    bar = generate_progress_bar(percent)
    return (
        f"<code>┌─── 🛰️ [ SYSTEM.DOWNLOAD.ACTIVE ] ───┐\n"
        f"│                                    \n"
        f"│ 📥 STATUS: Fetching from Telegram  \n"
        f"│ 📊 PROG: {bar} {percent:.1f}%\n"
        f"│ ⚡ SPEED: {speed:.2f} MB/s\n"
        f"│ 📦 SIZE: {size_mb:.2f} MB\n"
        f"│ ⏳ TIME: {format_time(elapsed)} / {format_time(eta)}\n"
        f"│                                    \n"
        f"└────────────────────────────────────┘</code>"
    )

def get_encode_ui(file_name, speed, fps, elapsed, eta, curr_sec, duration, percent, final_crf, final_preset, res_label, crop_label, hdr_label, grain_label, u_audio, u_bitrate, size, cpu=None, ram=None, demo_label=""):
    bar = generate_progress_bar(percent)
    sys_line = f"\u2502 \U0001f5a5\ufe0f SYSTEM: CPU {cpu:.1f}% | RAM {ram:.1f}%\n" if cpu is not None and ram is not None else ""
    demo_line = f"\u2502 \u26a1 DEMO MODE:{demo_label}\n" if demo_label else ""
    est_final = (size / percent * 100) if percent > 1 else 0
    if percent > 1:
        size_line = f"\u2502 \U0001f4e6 SIZE: {size:.2f} MB \u2192 ~{est_final:.1f} MB est\n"
    else:
        size_line = f"\u2502 \U0001f4e6 SIZE: {size:.2f} MB\n"
    return (
        "<code>"
        + "\u250c\u2500\u2500\u2500 \U0001f6f0\ufe0f [ SYSTEM.ENCODE.PROCESS ] \u2500\u2500\u2500\u2510\n"
        + "\u2502                                    \n"
        + f"\u2502 \U0001f4c2 FILE: {file_name}\n"
        + f"\u2502 \u26a1 SPEED: {speed:.1f}x ({int(fps)} FPS)\n"
        + f"\u2502 \u23f3 TIME: {format_time(elapsed)} / ETA: {format_time(eta)}\n"
        + f"\u2502 \U0001f552 DONE: {format_time(curr_sec)} / {format_time(duration)}\n"
        + "\u2502                                    \n"
        + f"\u2502 \U0001f4ca PROG: {bar} {percent:.1f}% \n"
        + "\u2502                                    \n"
        + f"\u2502 \U0001f6e0\ufe0f SETTINGS: CRF {final_crf} | Preset {final_preset}\n"
        + f"\u2502 \U0001f39e\ufe0f VIDEO: {res_label}{crop_label} | 10-bit | {hdr_label}{grain_label}\n"
        + f"\u2502 \U0001f50a AUDIO: {u_audio.upper()} @ {u_bitrate}\n"
        + size_line
        + "\u2502                                    \n"
        + demo_line
        + sys_line
        + "\u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518</code>"
    )

async def upload_progress(current, total, app, chat_id, status_msg, file_name):
    global last_up_update
    now = time.time()

    if now - last_up_update < 8:
        return

    percent = (current / total) * 100
    bar = generate_progress_bar(percent)
    cur_mb = current / (1024 * 1024)
    tot_mb = total / (1024 * 1024)

    scifi_up_ui = (
        f"<code>┌─── 🛰️ [ SYSTEM.UPLINK.ACTIVE ] ───┐\n"
        f"│                                    \n"
        f"│ 📂 FILE: {file_name}\n"
        f"│ 📊 PROG: {bar} {percent:.1f}%\n"
        f"│ 📦 SIZE: {cur_mb:.2f} / {tot_mb:.2f} MB\n"
        f"│ 📡 STATUS: Transmitting to Orbit... \n"
        f"│                                    \n"
        f"└────────────────────────────────────┘</code>"
    )

    try:
        await app.edit_message_text(chat_id, status_msg.id, scifi_up_ui, parse_mode=enums.ParseMode.HTML)
    except Exception:
        pass
    last_up_update = now

def get_parallel_ui(
    jobs, total, n_done, elapsed, eta, file_name, crf, preset, psy_rd, res_label,
    now_utc: datetime | None = None,
):
    """Parallel encode progress UI for coordinator TG messages."""

    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    overall_pct = (n_done / total * 100) if total else 0
    bar = generate_progress_bar(overall_pct)
    spin = _spinner(now_utc)

    # Build chunk status grid — 5 per row
    # In-progress: show live elapsed time so the message visibly changes every update
    lines = []
    row = []
    for i in range(1, total + 1):
        tag = str(i).zfill(2)
        job = next((j for j in jobs if f"chunk-{str(i).zfill(3)}" in j["name"]), None)

        if job is None:
            cell = f"{tag}⏳"
        elif job["status"] == "completed" and job["conclusion"] == "success":
            cell = f"{tag}✅"
        elif job["status"] == "completed":
            cell = f"{tag}❌"
        elif job["status"] == "in_progress":
            t = _chunk_elapsed(job, now_utc)
            cell = f"{tag}{spin}{t}" if t else f"{tag}{spin}"
        else:
            cell = f"{tag}🕐"

        row.append(cell)
        if len(row) == 5:
            lines.append("  ".join(row))
            row = []
    if row:
        lines.append("  ".join(row))

    chunk_grid = "\n│ ".join(lines)

    n_running = sum(1 for j in jobs if j["status"] == "in_progress")
    n_queued  = total - n_done - n_running

    eta_str     = format_time(eta) if eta > 0 else "--:--:--"
    elapsed_str = format_time(elapsed)

    return (
        f"<code>┌─── 🛸 [ SYSTEM.PARALLEL.ENCODE ] ──┐\n"
        f"│\n"
        f"│ 📂 FILE: {file_name}\n"
        f"│ 🛠️  CRF {crf} | Preset {preset} | PSY-RD {psy_rd}\n"
        f"│ 🎞️  {res_label} | 10-bit | PSYEX\n"
        f"│\n"
        f"│ 📊 OVERALL: {bar} {overall_pct:.0f}%\n"
        f"│ ⚡ WALL: {elapsed_str}  ETA: {eta_str}\n"
        f"│ ✅ {n_done}/{total} done  🔄 {n_running} running  🕐 {n_queued} queued\n"
        f"│\n"
        f"│ {chunk_grid}\n"
        f"│\n"
        f"└────────────────────────────────────┘</code>"
    )
