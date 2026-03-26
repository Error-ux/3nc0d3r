import time
from datetime import timedelta
import os
from pyrogram import enums

last_up_update = 0

def generate_progress_bar(percentage):
    total_segments = 15
    completed = int((max(0, min(100, percentage)) / 100) * total_segments)
    return "[" + "▰" * completed + "▱" * (total_segments - completed) + "]"

def format_time(seconds):
    return str(timedelta(seconds=int(seconds))).zfill(8)

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

def get_download_fail_ui(error_msg):
    return (
        f"<code>┌─── ❌ [ DOWNLOAD.MISSION.FAILED ] ───┐\n"
        f"│                                    \n"
        f"│ ❌ ERROR: {error_msg}\n"
        f"│ 🛠️ STATUS: Downlink Terminated.    \n"
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


def get_cancelled_ui(file_name, elapsed_str):
    return (
        f"<code>┌─── 🛑 [ MISSION.CANCELLED ] ───────┐\n"
        f"│                                    \n"
        f"│ 📂 FILE: {file_name}\n"
        f"│ ⏱ ELAPSED: {elapsed_str}\n"
        f"│                                    \n"
        f"│ 🚫 STATUS: Encode aborted by user. \n"
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
    sys_line = f"│ 🖥️ SYSTEM: CPU {cpu:.1f}% | RAM {ram:.1f}%\n" if cpu is not None and ram is not None else ""
    demo_line = f"│ ⚡ DEMO MODE:{demo_label}\n" if demo_label else ""
    est_final = (size / percent * 100) if percent > 1 else 0
    size_line = (
        f"│ 📦 SIZE: {size:.2f} MB → ~{est_final:.1f} MB est\n"
        if percent > 1 else
        f"│ 📦 SIZE: {size:.2f} MB\n"
    )
    return (
        f"<code>┌─── 🛰️ [ SYSTEM.ENCODE.PROCESS ] ───┐\n"
        f"│                                    \n"
        f"│ 📂 FILE: {file_name}\n"
        f"│ ⚡ SPEED: {speed:.1f}x ({int(fps)} FPS)\n"
        f"│ ⏳ TIME: {format_time(elapsed)} / ETA: {format_time(eta)}\n"
        f"│ 🕒 DONE: {format_time(curr_sec)} / {format_time(duration)}\n"
        f"│                                    \n"
        f"│ 📊 PROG: {bar} {percent:.1f}% \n"
        f"│                                    \n"
        f"│ 🛠️ SETTINGS: CRF {final_crf} | Preset {final_preset}\n"
        f"│ 🎞️ VIDEO: {res_label}{crop_label} | 10-bit | {hdr_label}{grain_label}\n"
        f"│ 🔊 AUDIO: {u_audio.upper()} @ {u_bitrate}\n"
        f"{{size_line}}"
        f"│                                    \n"
        f"{demo_line}"
        f"{sys_line}"
        f"└────────────────────────────────────┘</code>"
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