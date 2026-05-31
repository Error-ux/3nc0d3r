import sys
import os
import asyncio
import math
import time
from pathlib import Path

# repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyrogram import Client, enums, raw, types, utils
from pyrogram.errors import FloodWait

# Patch for modern large Telegram IDs
utils.MIN_CHANNEL_ID = -1009999999999999

import config
from utils.ui import get_failure_ui

_active_tg_state = None

# Monkeypatch Pyrogram Client.save_file to support pre-uploaded InputFile/InputFileBig objects
original_save_file = Client.save_file

async def patched_save_file(self, path, progress=None, progress_args=()):
    if isinstance(path, (raw.types.InputFile, raw.types.InputFileBig)):
        return path
    return await original_save_file(self, path, progress=progress, progress_args=progress_args)

Client.save_file = patched_save_file

# ---------------------------------------------------------------------------
# FAST MEDIA HELPERS
# ---------------------------------------------------------------------------

async def fast_upload(client: Client, file_path: str, progress_callback=None, progress_args=None):
    """
    Uploads a file in parallel chunks for maximum speed.
    Returns an InputFile object ready for send_document.
    """
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    chunk_size = 512 * 1024
    total_parts = math.ceil(file_size / chunk_size)
    is_big = file_size > 10 * 1024 * 1024
    
    # Generate a random 8-byte file ID
    file_id = int.from_bytes(os.urandom(8), "little", signed=True)
    
    semaphore = asyncio.Semaphore(16)
    uploaded_parts = 0
    
    async def upload_worker(part_index, offset):
        nonlocal uploaded_parts
        async with semaphore:
            with open(file_path, 'rb') as f:
                f.seek(offset)
                chunk = f.read(chunk_size)
            
            if is_big:
                await client.invoke(
                    raw.functions.upload.SaveBigFilePart(
                        file_id=file_id,
                        file_part=part_index,
                        file_total_parts=total_parts,
                        bytes=chunk
                    )
                )
            else:
                await client.invoke(
                    raw.functions.upload.SaveFilePart(
                        file_id=file_id,
                        file_part=part_index,
                        bytes=chunk
                    )
                )
            
            uploaded_parts += 1
            if progress_callback:
                # throttled UI update handled by the callback itself usually
                await progress_callback(min(uploaded_parts * chunk_size, file_size), file_size, *progress_args)

    tasks = [upload_worker(i, i * chunk_size) for i in range(total_parts)]
    await asyncio.gather(*tasks)

    if is_big:
        return raw.types.InputFileBig(id=file_id, parts=total_parts, name=file_name)
    else:
        return raw.types.InputFile(id=file_id, parts=total_parts, name=file_name, md5_checksum="")


# ---------------------------------------------------------------------------
# LANE RESOLUTION
# ---------------------------------------------------------------------------
ALL_LANES = [chr(ord("A") + i) for i in range(20)]  # ["A", "B", ..., "T"]


def _resolve_lane(run_number: int) -> str:
    return ALL_LANES[run_number % 20]


def _resolve_session_names() -> list[str]:
    """
    Return an ordered list of session names to try, most-preferred first.
    """
    run_number = int(os.environ.get("GITHUB_RUN_NUMBER", "0"))
    lane = _resolve_lane(run_number)
    print(f"TG session lane: {lane} (run #{run_number})")

    session_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "tg_session_dir"))
    sessions = []
    sessions.append(os.path.join(session_dir, f"enc_session_{lane}"))
    sessions.append(os.path.join(session_dir, f"tg_dl_session_{lane}"))
    sessions.append(config.SESSION_NAME)
    return sessions


def _get_all_session_names() -> list[str]:
    """
    Return all available lane session names ordered by priority for session lane rotation.
    """
    run_number = int(os.environ.get("GITHUB_RUN_NUMBER", "0"))
    preferred_lane = _resolve_lane(run_number)
    
    # Put preferred lane first, then others
    lanes = [preferred_lane] + [l for l in ALL_LANES if l != preferred_lane]
    
    session_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "tg_session_dir"))
    sessions = []
    for l in lanes:
        sessions.append(os.path.join(session_dir, f"enc_session_{l}"))
        sessions.append(os.path.join(session_dir, f"tg_dl_session_{l}"))
    sessions.append(config.SESSION_NAME)
    return sessions


# ---------------------------------------------------------------------------
# TELEGRAM AUTH
# ---------------------------------------------------------------------------
async def connect_telegram(tg_state: dict, tg_ready: asyncio.Event, label: str):
    """
    Connect to Telegram trying each session in priority order.
    """
    session_names = _resolve_session_names()
    flood_waits: dict[str, int] = {}

    app = None
    chosen_session = None
    for session_name in session_names:
        try:
            candidate = Client(
                session_name,
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                bot_token=config.BOT_TOKEN,
                max_concurrent_transmissions=16
            )
            await candidate.start()
            app = candidate
            chosen_session = session_name
            print(f"TG auth OK with session: {session_name}")
            break
        except Exception as e:
            if "FloodWait" in type(e).__name__ or getattr(e, "value", None) is not None:
                wait_val = getattr(e, "value", 10)
                flood_waits[session_name] = wait_val
                print(f"FloodWait {wait_val}s on '{session_name}' — trying next...")
                continue
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
                    max_concurrent_transmissions=16
                )
                await candidate.start()
                app = candidate
                chosen_session = best_session
                print(f"TG auth OK (post-flood attempt {attempt}): {best_session}")
                break
            except Exception as e:
                if "FloodWait" in type(e).__name__ or getattr(e, "value", None) is not None:
                    wait_secs = getattr(e, "value", 10)
                    print(f"Another FloodWait: {wait_secs}s — retrying...")
                    continue
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
    except Exception as e:
        if "FloodWait" in type(e).__name__ or getattr(e, "value", None) is not None:
            wait_val = getattr(e, "value", 10)
            await asyncio.sleep(wait_val)
            status = await app.send_message(
                config.CHAT_ID,
                f"<b>[ SYSTEM ONLINE ] {label}</b>",
                parse_mode=enums.ParseMode.HTML,
            )
        else:
            raise e

    tg_state["app"] = app
    tg_state["status"] = status
    tg_state["session_name"] = chosen_session
    tg_state["label"] = label
    
    global _active_tg_state
    _active_tg_state = tg_state
    
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
    except Exception as e:
        if "FloodWait" in type(e).__name__ or getattr(e, "value", None) is not None:
            wait_val = getattr(e, "value", 10)
            await asyncio.sleep(wait_val + 1)
        else:
            pass


# ---------------------------------------------------------------------------
# Telegram Session Lane Rotator
# ---------------------------------------------------------------------------
async def rotate_session_lane(tg_state: dict) -> bool:
    """
    Rotates the active Pyrogram client connection to a NEW dynamic session lane database
    under the SAME primary bot token. Appends an incrementing swap suffix to ensure there
    is an infinite supply of fresh session databases, preventing database locks and rate limits.
    """
    current_name = tg_state.get("session_name")
    if not current_name:
        print("[SESSION ROTATE ERROR] No active session_name in tg_state.")
        return False
        
    import re
    # Strip any existing swap suffix (e.g. _swap2) to resolve back to the base lane name
    base_name = re.sub(r"_swap\d+$", "", current_name)
    
    counter = tg_state.get("rotation_counter", 0) + 1
    tg_state["rotation_counter"] = counter
    
    next_session = f"{base_name}_swap{counter}"
    print(f"[SESSION ROTATE] Swapping connection to a fresh dynamic lane: {os.path.basename(next_session)}...")

    # Close old app safely
    old_app = tg_state.get("app")
    if old_app:
        try:
            await old_app.stop()
        except Exception:
            pass

    # Try starting the next session candidate
    new_app = None
    try:
        candidate = Client(
            next_session,
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            bot_token=config.BOT_TOKEN,
            max_concurrent_transmissions=16
        )
        await candidate.start()
        new_app = candidate
        print(f"[SESSION ROTATE] Successfully connected to dynamic lane session: {next_session}")
    except Exception as e:
        print(f"[SESSION ROTATE ERROR] Failed to connect to dynamic lane {next_session}: {e}")
        return False

    # Update tg_state
    tg_state["app"] = new_app
    tg_state["session_name"] = next_session

    print(f"✅ [SESSION ROTATE] Swapped active lane to: {os.path.basename(next_session)}.")
    return True


# ---------------------------------------------------------------------------
# FLOODWAIT RETRY PROTECTION HELPER (WITH SESSION ROTATION INTEGRATION)
# ---------------------------------------------------------------------------
async def run_with_flood_retry(func, *args, **kwargs):
    """
    Executes a Pyrogram method with automatic FloodWait retry protection.
    Features Session Lane Rotation to dynamically swap lane sessions under the same bot,
    bypassing rate limits immediately if fallbacks exist.
    """
    retries = 0
    while True:
        try:
            # Dynamically fetch the current bound method in case bot rotation swapped the client
            global _active_tg_state
            if _active_tg_state and hasattr(func, "__self__"):
                active_app = _active_tg_state.get("app")
                if active_app and func.__self__ is not active_app:
                    method_name = func.__name__
                    # Re-bind the function call to the new active client instance!
                    func = getattr(active_app, method_name)
            
            return await func(*args, **kwargs)
        except Exception as e:
            # 100% drift-proof FloodWait catching
            if "FloodWait" in type(e).__name__ or getattr(e, "value", None) is not None:
                wait_time = getattr(e, "value", 10)
                
                # Attempt bot session lane rotation to bypass the wait completely!
                if _active_tg_state:
                    rotated = await rotate_session_lane(_active_tg_state)
                    if rotated:
                        print("[FLOODWAIT] Successfully rotated to next session lane. Retrying immediately...", flush=True)
                        continue
                
                # Fallback to sleeping if rotation was not possible/available
                wait_time = wait_time + 3
                print(f"[FLOODWAIT] Rate limit hit. Sleeping for {wait_time}s before retrying...", flush=True)
                await asyncio.sleep(wait_time)
                continue
                
            err_str = str(e).lower()
            if any(term in err_str for term in ["connection", "timeout", "network", "reset", "broken pipe"]):
                retries += 1
                wait_time = min(5 * retries, 60)
                print(f"[RETRY] Network/transient error: {e}. Retrying in {wait_time}s...", flush=True)
                await asyncio.sleep(wait_time)
            else:
                raise e


# ---------------------------------------------------------------------------
# FAILURE NOTIFIER
# ---------------------------------------------------------------------------
async def tg_notify_failure(tg_state: dict, tg_ready: asyncio.Event,
                            file_name: str, reason: str):
    app    = tg_state.get("app")
    status = tg_state.get("status")
    if not app or not status:
        print(f"[TG-FAIL] TG unavailable — reason: {reason}")
        return
    try:
        await run_with_flood_retry(
            app.edit_message_text,
            config.CHAT_ID, status.id,
            get_failure_ui(file_name, reason),
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as e:
        print(f"[TG-FAIL] Could not edit status message: {e}")
    if config.LOG_FILE and os.path.exists(config.LOG_FILE):
        try:
            await run_with_flood_retry(
                app.send_document,
                config.CHAT_ID, config.LOG_FILE,
                caption="<b>FULL MISSION LOG</b>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception as e:
            print(f"[TG-FAIL] Could not send log: {e}")
