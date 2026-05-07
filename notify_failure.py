"""
notify_failure.py

Sends a failure notification to Telegram when the pipeline fails.
Reads from env: BOT_TOKEN, CHAT_ID, DOWNLOAD_OUTCOME, ENCODE_OUTCOME,
                GITHUB_RUN_NUMBER, UI_TITLE
Reads logs from: download.log, encode.log
"""

import os
import json
import subprocess
from pathlib import Path

BOT_TOKEN        = os.environ.get("BOT_TOKEN", "")
CHAT_ID          = os.environ.get("CHAT_ID", "")
DOWNLOAD_OUTCOME = os.environ.get("DOWNLOAD_OUTCOME", "")
ENCODE_OUTCOME   = os.environ.get("ENCODE_OUTCOME", "")
RUN_NUMBER       = os.environ.get("GITHUB_RUN_NUMBER", "?")
UI_TITLE         = os.environ.get("UI_TITLE", "Unknown")

# Resolve file name from tg_fname.txt or fallback to UI_TITLE
file_name = Path("tg_fname.txt").read_text().strip() \
    if Path("tg_fname.txt").exists() else UI_TITLE

# Determine which step failed
if DOWNLOAD_OUTCOME == "failure":
    phase, log_file, icon = "DOWNLOAD", "download.log", "📥"
elif ENCODE_OUTCOME == "failure":
    phase, log_file, icon = "ENCODE", "encode.log", "⚙️"
else:
    phase, log_file, icon = "UNKNOWN", None, "❌"

# Get last 5 lines of the log as the error snippet
log_path = Path(log_file) if log_file else None
if log_path and log_path.exists():
    lines   = log_path.read_text().splitlines()
    snippet = " ".join(lines[-5:])
else:
    snippet = "No log available."

message = (
    f"<code>"
    f"┌─── ⚠️ [ MISSION.CRITICAL.FAILURE ] ───┐\n"
    f"│\n"
    f"│ 📂 FILE: {file_name}\n"
    f"│ {icon} PHASE: {phase} FAILED\n"
    f"│ ❌ ERROR DETECTED:\n"
    f"│ {snippet}\n"
    f"│\n"
    f"│ 🛠️ STATUS: Core dumped.\n"
    f"│ 📑 Full log attached below.\n"
    f"└────────────────────────────────────┘"
    f"</code>"
)

def tg_send_message(text):
    payload = json.dumps({
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    })
    subprocess.run([
        "curl", "-s", "-X", "POST",
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        "-H", "Content-Type: application/json",
        "-d", payload,
    ], check=False)


def tg_send_document(filepath, caption):
    subprocess.run([
        "curl", "-s", "-X", "POST",
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
        "-F", f"chat_id={CHAT_ID}",
        "-F", f"document=@{filepath}",
        "-F", f"caption={caption}",
    ], check=False)


tg_send_message(message)

if log_path and log_path.exists():
    tg_send_document(str(log_path), f"📋 {phase} phase log — Run #{RUN_NUMBER}")

print(f"✅ Failure notification sent for phase: {phase}")
