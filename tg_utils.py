"""
tg_utils.py
Shared Telegram helpers for main.py and upload.py.

Extracted to avoid copy-paste drift between the two phases.
Exports:
    ALL_LANES, _resolve_lane, _resolve_session_names
    connect_telegram(tg_state, tg_ready, label)
    tg_edit(tg_state, tg_ready, text, reply_markup=None)
    tg_notify_failure(tg_state, tg_ready, file_name, reason)
"""

import asyncio
import os

from pyrogram import Client, enums
from pyrogram.errors import FloodWait

import config
from ui import get_failure_ui


# ---------------------------------------------------------------------------
# LANE RESOLUTION
# ---------------------------------------------------------------------------
ALL_LANES = [chr(ord("A") + i) for i in range(20)]  # ["A", "B", ..., "T"]


def _resolve_lane(run_number: int) -> str:
    return ALL_LANES[run_number % 20]


def _resolve_session_names() -> list[str]:
    """
    Return an ordered list of session names to try, most-preferred first.
    Own lane tried first, then cross-lane fallbacks, then legacy bare session.
    """
    run_number = int(os.environ.get("GITHUB_RUN_NUMBER", "0"))
    lane = _resolve_lane(run_number)
    print(f"TG session lane: {lane} (run #{run_number})")

    other_lanes = [l for l in ALL_LANES if l != lane]
    sessions = []
    sessions.append(f"tg_session_dir/enc_session_{lane}")
    sessions.append(f"tg_session_dir/tg_dl_session_{lane}")
    for other in other_lanes:
        sessions.append(f"tg_session_dir/enc_session_{other}")
        sessions.append(f"tg_session_dir/tg_dl_session_{other}")
    sessions.append(config.SESSION_NAME)
    return sessions


# ---------------------------------------------------------------------------
# TELEGRAM AUTH
# ---------------------------------------------------------------------------
async def connect_telegram(tg_state: dict, tg_ready: asyncio.Event, label: str):
    """
    Connect to Telegram trying each session in priority order.
    FloodWait on a session → skip to next. If all flooded → sleep shortest
    wait then retry. Sets tg_state['app'] and tg_state['status'] on success.
    """
    session_names = _resolve_session_names()
    flood_waits: dict[str, int] = {}

    app = None
    for session_name in session_names:
        try:
            candidate = Client(
                session_name,
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                bot_token=config.BOT_TOKEN,
            )
            await candidate.start()
            app = candidate
            print(f"TG auth OK with session: {session_name}")
            break
        except FloodWait as e:
            flood_waits[session_name] = e.value
            print(f"FloodWait {e.value}s on '{session_name}' — trying next...")
            continue
        except Exception as e:
            print(f"TG auth error on '{session_name}': {e} — trying next...")
            continue

    if app is None and flood_waits:
        best_session = min(flood_waits, key=flood_waits.get)
        wait_secs = flood_waits[best_session]
        attempt = 0
        while True:
            attempt += 1
            print(f"All sessions flooded. Sleeping {wait_secs}s for '{best_session}' (attempt {attempt})...")
            await asyncio.sleep(wait_secs + 5)
            try:
                candidate = Client(
                    best_session,
                    api_id=config.API_ID,
                    api_hash=config.API_HASH,
                    bot_token=config.BOT_TOKEN,
                )
                await candidate.start()
                app = candidate
                print(f"TG auth OK (post-flood attempt {attempt}): {best_session}")
                break
            except FloodWait as e:
                wait_secs = e.value
                print(f"Another FloodWait: {wait_secs}s — retrying...")
                continue
            except Exception as e:
                print(f"TG auth failed on post-flood attempt {attempt}: {e}")
                return

    if app is None:
        print("TG auth failed: no usable session found.")
        return

    try:
        status = await app.send_message(
            config.CHAT_ID,
            f"<b>[ SYSTEM ONLINE ] {label}</b>",
            parse_mode=enums.ParseMode.HTML,
        )
    except FloodWait as e:
        await asyncio.sleep(e.value)
        status = await app.send_message(
            config.CHAT_ID,
            f"<b>[ SYSTEM ONLINE ] {label}</b>",
            parse_mode=enums.ParseMode.HTML,
        )

    tg_state["app"] = app
    tg_state["status"] = status
    tg_ready.set()
    print("Telegram connected.")


# ---------------------------------------------------------------------------
# SAFE TG EDIT
# ---------------------------------------------------------------------------
async def tg_edit(tg_state: dict, tg_ready: asyncio.Event, text: str, reply_markup=None):
    if not tg_ready.is_set():
        return
    app    = tg_state.get("app")
    status = tg_state.get("status")
    if not app or not status:
        return
    try:
        kwargs = dict(parse_mode=enums.ParseMode.HTML)
        if reply_markup:
            kwargs["reply_markup"] = reply_markup
        await app.edit_message_text(config.CHAT_ID, status.id, text, **kwargs)
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# FAILURE NOTIFIER
# ---------------------------------------------------------------------------
async def tg_notify_failure(tg_state: dict, tg_ready: asyncio.Event,
                            file_name: str, reason: str):
    """
    Edit status message to failure UI and attach log file if present.
    Safe to call even if TG never connected.
    """
    app    = tg_state.get("app")
    status = tg_state.get("status")
    if not app or not status:
        print(f"[TG-FAIL] TG unavailable — reason: {reason}")
        return
    try:
        await app.edit_message_text(
            config.CHAT_ID, status.id,
            get_failure_ui(file_name, reason),
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as e:
        print(f"[TG-FAIL] Could not edit status message: {e}")
    if config.LOG_FILE and __import__("os").path.exists(config.LOG_FILE):
        try:
            await app.send_document(
                config.CHAT_ID, config.LOG_FILE,
                caption="<b>FULL MISSION LOG</b>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception as e:
            print(f"[TG-FAIL] Could not send log: {e}")
