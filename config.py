import os

# ---------- FILE PATHS & CONSTANTS ----------
SOURCE = "source.mkv"
SCREENSHOT = "grid_preview.jpg"
LOG_FILE = "encode_log.txt"

# ---------- TELEGRAM CREDENTIALS ----------
API_ID = int(os.getenv("API_ID", os.getenv("TG_API_ID", "0")).strip() or "0")
API_HASH = os.getenv("API_HASH", os.getenv("TG_API_HASH", "")).strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", os.getenv("TG_BOT_TOKEN", "")).strip()

# Dynamic 2-Channel Routing (Anime vs Other content types)
_PRIMARY_CHAT = os.getenv("CHAT_ID", os.getenv("TG_CHAT_ID", "0")).strip()
_OTHER_CHAT   = os.getenv("CHAT_ID_OTHER", os.getenv("TG_CHAT_ID_OTHER", "0")).strip()

# Check Content Type (Anime vs AMV/Donghua/Hentai/HMV/custom)
CONTENT_TYPE_VAL = os.getenv("CONTENT_TYPE", "Anime").strip()

if CONTENT_TYPE_VAL.lower() != "anime" and _OTHER_CHAT != "0" and _OTHER_CHAT != "":
    _target_chat_str = _OTHER_CHAT
else:
    _target_chat_str = _PRIMARY_CHAT

CHAT_ID = int(_target_chat_str) if _target_chat_str else 0

# Channels to forward/copy the successfully uploaded file to
# Channels to forward/copy the successfully uploaded file to
FORWARD_CHATS = []
_raw_forward = os.getenv("FORWARD_CHATS", os.getenv("TG_FORWARD_CHATS", "")).strip()
if _raw_forward:
    for part in _raw_forward.split(","):
        part = part.strip()
        if not part:
            continue
        if part.startswith("-") or part.isdigit():
            try:
                FORWARD_CHATS.append(int(part))
            except ValueError:
                FORWARD_CHATS.append(part)
        else:
            FORWARD_CHATS.append(part)

# Private notification chats/users (e.g. your personal ID 6253389182)
NOTIFY_CHATS = []
_raw_notify = os.getenv("NOTIFY_CHATS", os.getenv("TG_NOTIFY_CHATS", "6253389182")).strip()
if _raw_notify:
    for part in _raw_notify.split(","):
        part = part.strip()
        if not part:
            continue
        if part.startswith("-") or part.isdigit():
            try:
                NOTIFY_CHATS.append(int(part))
            except ValueError:
                NOTIFY_CHATS.append(part)
        else:
            NOTIFY_CHATS.append(part)



# Set environment variables for all concurrent modules and subprocesses
os.environ["CHAT_ID"] = str(CHAT_ID)
os.environ["TG_CHAT_ID"] = str(CHAT_ID)

# Dynamic Telegram Progress Update Throttling (defaulting to 120 seconds to prevent FloodWait)
TG_PROGRESS_INTERVAL = int(os.getenv("TG_PROGRESS_INTERVAL", "120").strip() or "120")
os.environ["TG_PROGRESS_INTERVAL"] = str(TG_PROGRESS_INTERVAL)

# Mute intermediate Telegram progress edits (set to True to use Web Dashboard exclusively for live tracking)
TG_MUTE_PROGRESS = os.getenv("TG_MUTE_PROGRESS", "false").lower() == "true"
os.environ["TG_MUTE_PROGRESS"] = str(TG_MUTE_PROGRESS).lower()

FILE_NAME = os.getenv("FILE_NAME", "output.mkv")
SESSION_NAME = os.getenv("SESSION_NAME", "enc_session")

# ---------- USER SETTINGS ----------
USER_RES = os.getenv("USER_RES")
USER_CRF = os.getenv("USER_CRF")
USER_PRESET = os.getenv("USER_PRESET")
USER_GRAIN = os.getenv("USER_GRAIN", "10")
AUDIO_MODE = os.getenv("AUDIO_MODE", "opus")
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "48k")
RUN_VMAF      = os.getenv("RUN_VMAF",      "true").lower() == "true"
RUN_UPLOAD    = os.getenv("RUN_UPLOAD",    "true").lower() == "true"



# Unique key per run so parallel encodes don't collide.
# GitHub Actions always sets GITHUB_RUN_ID automatically.
GITHUB_RUN_ID = os.getenv("GITHUB_RUN_ID", "local")

# ---------- ENCODER BRANDING ----------
# Sets the MKV container Title tag on every output file.
# Leave blank to inherit the title from the source file.
ENCODER_TITLE = os.getenv("ENCODER_TITLE", "zub'sEncodes")

# ---------- ANIME RENAME SETTINGS ----------
# Set by the bridge when launching a mission.
# If ANIME_NAME is blank, the raw FILE_NAME is kept as-is.
ANIME_NAME   = os.getenv("ANIME_NAME",   "")       # e.g. "Medalist"
SEASON       = os.getenv("SEASON",       "1")      # e.g. "2"
EPISODE      = os.getenv("EPISODE",      "1")      # e.g. "7"
AUDIO_TYPE   = os.getenv("AUDIO_TYPE",    "Auto")   # Sub | Dual | Tri | Multi | Auto
CONTENT_TYPE = os.getenv("CONTENT_TYPE", "Anime")  # Anime | Donghua | Hentai | HMV | AMV | custom
SUB_TRACKS   = os.getenv("SUB_TRACKS",   "")       # e.g. "English, Arabic"
AUDIO_TRACKS = os.getenv("AUDIO_TRACKS", "")       # e.g. "Japanese, English (Dub)"

# ---------- DEMO / PARTIAL ENCODING ----------
# Set DEMO_DURATION to a non-empty value (e.g. "120") to encode only that
# many seconds of the source.  DEMO_START is the seek position (default "0").
# Leave DEMO_DURATION blank (or unset) to encode the full file as normal.
DEMO_START    = os.getenv("DEMO_START",    "0")   # seconds or HH:MM:SS
DEMO_DURATION = os.getenv("DEMO_DURATION", "")    # seconds; blank = full encode


