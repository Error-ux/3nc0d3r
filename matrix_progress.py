"""
matrix_progress.py
──────────────────
Handles Telegram progress relay for the matrix (parallel chunk) encode path.

TWO MODES:

1. INIT  (called from the `split` job via CLI)
   ─ Connects to TG, sends one status message per chunk + one master message.
   ─ Prints all message IDs as JSON to stdout so the workflow can capture them.

2. POLL  (called as an asyncio task from main.py in the `merge-and-finish` job)
   ─ Reads each chunk's dedicated TG message, parses its progress %.
   ─ Aggregates into a single combined progress and edits the master message.
   ─ Runs until stop_event is set (by main.py when merge is done).

CHUNK RUNNERS (encode-parts jobs — pure bash, no Python):
   ─ Each runner uses `curl` to call editMessageText on its own dedicated
     message_id.  No race conditions — each runner owns exactly one message.

Message format written by chunk runners (must stay parseable):
   CHUNK:02/12 PROG:47.3% SPEED:0.8x ETA:00:04:22 SIZE:142MB

Master message is a full scifi-style UI (matches your existing encode UI).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time

import httpx
from pyrogram import Client, enums
from pyrogram.errors import FloodWait

import config
from ui import generate_progress_bar, format_time


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

TG_API_BASE = "https://api.telegram.org/bot"
POLL_INTERVAL = 12       # seconds between aggregate updates
STALL_TIMEOUT = 600      # seconds — if a chunk doesn't update, mark it stalled


# ─────────────────────────────────────────────────────────────────────────────
# TG BOT API HELPERS  (no pyrogram needed — plain HTTPS)
# ─────────────────────────────────────────────────────────────────────────────

def _bot_url(method: str) -> str:
    return f"{TG_API_BASE}{config.BOT_TOKEN}/{method}"


def _send_message(chat_id: int, text: str) -> dict:
    """Send a new message, return the full response dict."""
    import urllib.request, urllib.parse
    payload = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }).encode()
    req = urllib.request.Request(
        _bot_url("sendMessage"),
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def _edit_message(chat_id: int, message_id: int, text: str) -> dict:
    import urllib.request
    payload = json.dumps({
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
    }).encode()
    req = urllib.request.Request(
        _bot_url("editMessageText"),
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"[matrix_progress] edit failed: {e}", file=sys.stderr)
        return {}


def _get_message(chat_id: int, message_id: int) -> str | None:
    """
    Fetch message text via getMessages (only works for bots in groups/channels).
    Falls back to forwardMessage trick if needed.
    Uses the Bot API's getMessage endpoint (Bot API 7.0+).
    """
    import urllib.request
    payload = json.dumps({
        "chat_id": chat_id,
        "message_id": message_id,
    }).encode()
    req = urllib.request.Request(
        _bot_url("getMessage"),
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            if data.get("ok"):
                return data["result"].get("text", "")
    except Exception as e:
        print(f"[matrix_progress] getMessage failed: {e}", file=sys.stderr)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# INIT MODE  — run from the split job
# ─────────────────────────────────────────────────────────────────────────────

def init_chunk_messages(n_chunks: int, file_name: str) -> dict:
    """
    Send one placeholder TG message per chunk + one master message.
    Returns a dict:
        {
          "master_id": <int>,
          "chunk_ids": [<int>, <int>, ...]   # index = chunk number
        }
    """
    chat_id = config.CHAT_ID
    chunk_ids = []

    print(f"[matrix_progress] Sending {n_chunks} chunk placeholder messages...")

    for i in range(n_chunks):
        text = _chunk_placeholder(i, n_chunks, file_name)
        resp = _send_message(chat_id, text)
        if not resp.get("ok"):
            raise RuntimeError(f"Failed to send chunk message {i}: {resp}")
        msg_id = resp["result"]["message_id"]
        chunk_ids.append(msg_id)
        print(f"  Chunk {i:02d} → message_id={msg_id}")
        time.sleep(0.35)   # avoid hitting Telegram rate limit (30 msg/s)

    # Master summary message (sent last so it appears below chunk messages)
    master_text = _master_placeholder(n_chunks, file_name)
    resp = _send_message(chat_id, master_text)
    if not resp.get("ok"):
        raise RuntimeError(f"Failed to send master message: {resp}")
    master_id = resp["result"]["message_id"]
    print(f"  Master → message_id={master_id}")

    result = {"master_id": master_id, "chunk_ids": chunk_ids}
    print(json.dumps(result))   # captured by workflow via $(...) subshell
    return result


def _chunk_placeholder(idx: int, total: int, file_name: str) -> str:
    return (
        f"<code>┌─── 🧩 CHUNK {idx:02d}/{total} ───────────────────┐\n"
        f"│ 📂 {file_name[:30]}\n"
        f"│ ⏳ STATUS: Waiting for runner...\n"
        f"└────────────────────────────────────┘</code>"
    )


def _master_placeholder(total: int, file_name: str) -> str:
    return (
        f"<code>┌─── 🛰️ [ MATRIX.ENCODE.ACTIVE ] ───┐\n"
        f"│                                    \n"
        f"│ 📂 FILE: {file_name[:28]}\n"
        f"│ 🧩 CHUNKS: {total} parallel runners\n"
        f"│ ⏳ STATUS: Initialising...\n"
        f"│                                    \n"
        f"└────────────────────────────────────┘</code>"
    )


# ─────────────────────────────────────────────────────────────────────────────
# CHUNK RUNNER PROGRESS FORMAT
# Written by encode-parts bash runners via curl.
# Must be parseable by parse_chunk_text() below.
# ─────────────────────────────────────────────────────────────────────────────

CHUNK_PROGRESS_TEMPLATE = (
    "<code>┌─── 🧩 CHUNK {idx:02d}/{total} ───────────────────┐\n"
    "│ 📂 {file_name}\n"
    "│ 📊 PROG: {bar} {percent:.1f}%\n"
    "│ ⚡ SPEED: {speed}\n"
    "│ ⏳ ETA:   {eta}\n"
    "│ 📦 SIZE:  {size}\n"
    "└────────────────────────────────────┘</code>"
)

# Shell template (used in the workflow YAML, rendered by bash)
# Kept here for reference — actual bash code is in the workflow.
CHUNK_CURL_TEMPLATE = r"""
OUT_TIME_MS={out_time_ms}
DURATION={duration}
CHUNK_IDX={chunk_idx}
N_CHUNKS={n_chunks}
MSG_ID={message_id}
FILE_NAME="{file_name}"

CURR_SEC=$(echo "scale=2; $OUT_TIME_MS / 1000000" | bc)
PERCENT=$(echo "scale=1; $CURR_SEC * 100 / $DURATION" | bc)
ELAPSED=$(( $(date +%s) - START_TIME ))
SPEED=$(echo "scale=2; $CURR_SEC / $ELAPSED" | bc 2>/dev/null || echo "0")
REMAINING=$(echo "scale=0; ($DURATION - $CURR_SEC) / ($SPEED + 0.001)" | bc 2>/dev/null || echo "0")
ETA=$(printf '%02d:%02d:%02d' $(($REMAINING/3600)) $(($REMAINING%3600/60)) $(($REMAINING%60)))

TEXT=$(printf '<code>CHUNK:%02d/%d PROG:%.1f%% SPEED:%.1fx ETA:%s SIZE:?</code>' \
  "$CHUNK_IDX" "$N_CHUNKS" "$PERCENT" "$SPEED" "$ETA")

curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/editMessageText" \
  -H "Content-Type: application/json" \
  -d "{\"chat_id\":\"${CHAT_ID}\",\"message_id\":${MSG_ID},\"text\":\"${TEXT}\",\"parse_mode\":\"HTML\"}" \
  > /dev/null
"""


# ─────────────────────────────────────────────────────────────────────────────
# PARSE CHUNK TEXT
# ─────────────────────────────────────────────────────────────────────────────

def parse_chunk_text(text: str) -> dict | None:
    """
    Parse progress data out of a chunk message text.
    Expected format (written by bash runner):
        CHUNK:02/12 PROG:47.3% SPEED:0.8x ETA:00:04:22 SIZE:142MB
    Returns dict with keys: idx, total, percent, speed, eta, size
    Returns None if text is unparseable (placeholder / not yet started).
    """
    if not text:
        return None

    m_pct   = re.search(r"PROG:([\d.]+)%",   text)
    m_speed = re.search(r"SPEED:([\d.]+)x",  text)
    m_eta   = re.search(r"ETA:([\d:]+)",      text)
    m_chunk = re.search(r"CHUNK:(\d+)/(\d+)", text)
    m_size  = re.search(r"SIZE:([\d.]+\w+)",  text)

    if not m_pct or not m_chunk:
        return None

    return {
        "idx":     int(m_chunk.group(1)),
        "total":   int(m_chunk.group(2)),
        "percent": float(m_pct.group(1)),
        "speed":   float(m_speed.group(1)) if m_speed else 0.0,
        "eta_str": m_eta.group(1) if m_eta else "??:??:??",
        "size":    m_size.group(1) if m_size else "?",
    }


# ─────────────────────────────────────────────────────────────────────────────
# MASTER UI BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_master_ui(
    file_name: str,
    chunk_data: list[dict | None],
    final_crf: str,
    final_preset: str,
    elapsed: float,
    start_time: float,
) -> str:
    """
    Build the combined master progress UI from all chunk data.
    chunk_data: list of parse_chunk_text() results (None = not started yet).
    """
    n       = len(chunk_data)
    valid   = [c for c in chunk_data if c is not None]
    done    = [c for c in valid if c["percent"] >= 99.9]

    if not valid:
        agg_pct = 0.0
    else:
        # Weight each chunk equally (they're equal length slices)
        agg_pct = sum(c["percent"] for c in chunk_data if c is not None) / n

    bar     = generate_progress_bar(agg_pct)
    elapsed_str = format_time(elapsed)

    # Best-guess ETA: max remaining ETA among active chunks
    active_etas = []
    for c in valid:
        if c["percent"] < 99.9:
            m = re.match(r"(\d+):(\d+):(\d+)", c["eta_str"])
            if m:
                secs = int(m.group(1))*3600 + int(m.group(2))*60 + int(m.group(3))
                active_etas.append(secs)
    eta_secs    = max(active_etas) if active_etas else 0
    eta_str     = format_time(eta_secs)

    avg_speed   = (sum(c["speed"] for c in valid) / len(valid)) if valid else 0.0

    # Per-chunk mini status line (compact)
    chunk_lines = ""
    for i, c in enumerate(chunk_data):
        if c is None:
            chunk_lines += f"│  [{i:02d}] ⏳ waiting\n"
        elif c["percent"] >= 99.9:
            chunk_lines += f"│  [{i:02d}] ✅ done\n"
        else:
            mini_bar = "▰" * int(c["percent"] / 10) + "▱" * (10 - int(c["percent"] / 10))
            chunk_lines += f"│  [{i:02d}] {mini_bar} {c['percent']:5.1f}%\n"

    return (
        f"<code>┌─── 🛰️ [ MATRIX.ENCODE.ACTIVE ] ───┐\n"
        f"│                                    \n"
        f"│ 📂 FILE: {file_name[:28]}\n"
        f"│ 🧩 CHUNKS: {len(done)}/{n} done\n"
        f"│                                    \n"
        f"│ 📊 TOTAL: {bar} {agg_pct:.1f}%\n"
        f"│ ⚡ AVG SPEED: {avg_speed:.2f}x\n"
        f"│ ⏱ ELAPSED: {elapsed_str}\n"
        f"│ ⏳ ETA: {eta_str}\n"
        f"│ 🛠️ CRF {final_crf} | Preset {final_preset}\n"
        f"│                                    \n"
        f"{chunk_lines}"
        f"│                                    \n"
        f"└────────────────────────────────────┘</code>"
    )


# ─────────────────────────────────────────────────────────────────────────────
# POLL TASK  — run as asyncio.Task inside main.py
# ─────────────────────────────────────────────────────────────────────────────

async def matrix_progress_poller(
    master_id: int,
    chunk_ids: list[int],
    file_name: str,
    final_crf: str,
    final_preset: str,
    stop_event: asyncio.Event,
):
    """
    Polls each chunk's TG message, aggregates progress, edits master message.
    Call as:
        asyncio.create_task(matrix_progress_poller(...))
    Set stop_event when merge is complete.
    """
    chat_id    = config.CHAT_ID
    n          = len(chunk_ids)
    start_time = time.time()

    print(f"[matrix_progress] Poller started — tracking {n} chunks, master_id={master_id}")

    while not stop_event.is_set():
        chunk_data: list[dict | None] = []

        for msg_id in chunk_ids:
            text = _get_message(chat_id, msg_id)
            chunk_data.append(parse_chunk_text(text))

        elapsed = time.time() - start_time
        ui = build_master_ui(file_name, chunk_data, final_crf, final_preset, elapsed, start_time)
        _edit_message(chat_id, master_id, ui)

        await asyncio.sleep(POLL_INTERVAL)

    print("[matrix_progress] Poller stopped.")


# ─────────────────────────────────────────────────────────────────────────────
# CLI ENTRY POINT  — used by the split job
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    """
    Usage:
        python3 matrix_progress.py init <n_chunks> <file_name>

    Prints JSON to stdout:
        {"master_id": 123, "chunk_ids": [100, 101, ...]}
    """
    if len(sys.argv) < 4 or sys.argv[1] != "init":
        print("Usage: matrix_progress.py init <n_chunks> <file_name>", file=sys.stderr)
        sys.exit(1)

    n_chunks  = int(sys.argv[2])
    file_name = sys.argv[3]

    result = init_chunk_messages(n_chunks, file_name)
    # result already printed as JSON inside init_chunk_messages()
