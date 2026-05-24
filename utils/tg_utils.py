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
    other_lanes = [l for l in ALL_LANES if l != lane]
    sessions = []
    sessions.append(os.path.join(session_dir, f"enc_session_{lane}"))
    sessions.append(os.path.join(session_dir, f"tg_dl_session_{lane}"))
    for other in other_lanes:
        sessions.append(os.path.join(session_dir, f"enc_session_{other}"))
        sessions.append(os.path.join(session_dir, f"tg_dl_session_{other}"))
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
                    max_concurrent_transmissions=16
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
    if config.LOG_FILE and os.path.exists(config.LOG_FILE):
        try:
            await app.send_document(
                config.CHAT_ID, config.LOG_FILE,
                caption="<b>FULL MISSION LOG</b>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception as e:
            print(f"[TG-FAIL] Could not send log: {e}")
