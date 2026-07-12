"""
utils/telethon_upload.py — Telethon-based parallel uploader for Telegram.

Replaces Pyrogram's fast_upload with Telethon's more resilient connection
handling, eliminating broken-pipe crashes under parallel chunk uploads.

Usage:
    from utils.telethon_upload import telethon_upload_file
    msg_id = await telethon_upload_file(
        file_path, chat_id, caption, thumb, buttons_data,
        progress_callback, progress_args
    )
"""
import asyncio
import math
import os
import time

from telethon import TelegramClient, helpers
from telethon.sessions import StringSession
from telethon.tl import types, functions

import config

# ─── Persistent client (created once per process) ────────────────────────────
_client: TelegramClient | None = None
_client_lock = asyncio.Lock()


async def _get_client() -> TelegramClient:
    """Lazily create and cache a single Telethon bot client.
    
    Uses StringSession from TG_TELETHON_SESSION env var to avoid
    re-authenticating the bot on every GHA run (which triggers FloodWait).
    """
    global _client
    async with _client_lock:
        if _client is None or not _client.is_connected():
            api_id = int(os.getenv("API_ID", os.getenv("TG_API_ID", "0")))
            api_hash = os.getenv("API_HASH", os.getenv("TG_API_HASH", ""))
            bot_token = os.getenv("BOT_TOKEN", os.getenv("TG_BOT_TOKEN", ""))
            session_str = os.getenv("TG_TELETHON_SESSION", "")

            if session_str:
                # Reuse existing session — no re-auth, no FloodWait
                _client = TelegramClient(StringSession(session_str), api_id, api_hash)
                await _client.connect()
                if not await _client.is_user_authorized():
                    await _client.sign_in(bot_token=bot_token)
                print("[telethon] Bot client connected (session reused).", flush=True)
            else:
                # First-time auth — print session string for user to save
                _client = TelegramClient(StringSession(), api_id, api_hash)
                await _client.start(bot_token=bot_token)
                new_session = _client.session.save()
                print("[telethon] Bot client connected (fresh auth).", flush=True)
                print(f"[telethon] ⚠️  Save this as GHA secret TG_TELETHON_SESSION to avoid FloodWait:", flush=True)
                print(f"[telethon] SESSION={new_session}", flush=True)
    return _client


async def disconnect_client():
    """Disconnect the cached Telethon client (call at pipeline end)."""
    global _client
    if _client and _client.is_connected():
        await _client.disconnect()
        _client = None


# ─── Parallel chunk uploader ─────────────────────────────────────────────────
async def _fast_upload(
    client: TelegramClient,
    file_path: str,
    progress_callback=None,
    progress_args=None,
    workers: int = 8,
) -> types.InputFileBig | types.InputFile:
    """
    Upload a file to Telegram in parallel chunks using Telethon.
    Returns an InputFile / InputFileBig ready for send_file.
    """
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    part_size = 512 * 1024  # 512 KB per chunk
    part_count = math.ceil(file_size / part_size)
    is_big = file_size > 10 * 1024 * 1024
    file_id = helpers.generate_random_long()

    semaphore = asyncio.Semaphore(workers)
    uploaded_parts = 0

    async def _worker(part_index: int, offset: int):
        nonlocal uploaded_parts
        async with semaphore:
            with open(file_path, "rb") as f:
                f.seek(offset)
                chunk = f.read(part_size)

            for attempt in range(5):
                try:
                    if is_big:
                        await client(
                            functions.upload.SaveBigFilePartRequest(
                                file_id=file_id,
                                file_part=part_index,
                                file_total_parts=part_count,
                                bytes=chunk,
                            )
                        )
                    else:
                        await client(
                            functions.upload.SaveFilePartRequest(
                                file_id=file_id,
                                file_part=part_index,
                                bytes=chunk,
                            )
                        )
                    break  # success
                except Exception as e:
                    if attempt < 4:
                        wait = 2 ** attempt
                        print(
                            f"[telethon_upload] chunk {part_index} attempt {attempt+1} "
                            f"failed: {e} — retrying in {wait}s",
                            flush=True,
                        )
                        await asyncio.sleep(wait)
                    else:
                        raise

            uploaded_parts += 1
            if progress_callback:
                current = min(uploaded_parts * part_size, file_size)
                if progress_args:
                    await progress_callback(current, file_size, *progress_args)
                else:
                    await progress_callback(current, file_size)

    tasks = [_worker(i, i * part_size) for i in range(part_count)]
    await asyncio.gather(*tasks)

    if is_big:
        return types.InputFileBig(file_id, part_count, file_name)
    return types.InputFile(file_id, part_count, file_name, "")


# ─── High-level upload + send ────────────────────────────────────────────────
async def telethon_upload_file(
    file_path: str,
    chat_id: int,
    caption: str,
    thumb: str | None = None,
    buttons_data: list | None = None,
    progress_callback=None,
    progress_args=None,
    workers: int = 8,
) -> int | None:
    """
    Upload a file via Telethon and send it as a document.
    Returns the Telegram message ID of the sent document, or None on failure.

    Parameters
    ----------
    file_path : str         Path to the file to upload.
    chat_id : int           Target chat/channel ID.
    caption : str           HTML caption for the document.
    thumb : str | None      Path to thumbnail image.
    buttons_data : list     List of (text, url) tuples for inline buttons.
    progress_callback       async callback(current, total, *progress_args)
    progress_args           Extra args forwarded to the callback.
    workers : int           Number of parallel upload workers (default 8).
    """
    client = await _get_client()

    print(f"[telethon_upload] Uploading {os.path.basename(file_path)}...", flush=True)
    start = time.time()

    try:
        # 1. Parallel upload
        input_file = await _fast_upload(
            client, file_path,
            progress_callback=progress_callback,
            progress_args=progress_args,
            workers=workers,
        )

        # 2. Build inline buttons (if any)
        from telethon.tl.types import (
            ReplyInlineMarkup,
            KeyboardButtonUrl,
            KeyboardButtonRow,
        )
        reply_markup = None
        if buttons_data:
            btn_row = [KeyboardButtonUrl(text=t, url=u) for t, u in buttons_data if u]
            if btn_row:
                reply_markup = ReplyInlineMarkup(rows=[KeyboardButtonRow(buttons=btn_row)])

        # 3. Build attributes
        attributes = [types.DocumentAttributeFilename(os.path.basename(file_path))]

        # 4. Send the document
        try:
            entity = await client.get_entity(chat_id)
        except Exception as e:
            print(f"[telethon_upload] Warning: get_entity failed for {chat_id}: {e}. Retrying with direct ID.", flush=True)
            entity = chat_id

        result = await client.send_file(
            entity,
            input_file,
            caption=caption,
            parse_mode="html",
            thumb=thumb,
            attributes=attributes,
            force_document=True,
            buttons=reply_markup,
        )

        elapsed = time.time() - start
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        speed = size_mb / elapsed if elapsed > 0 else 0
        print(
            f"[telethon_upload] ✅ Sent in {elapsed:.1f}s "
            f"({size_mb:.1f} MB @ {speed:.2f} MB/s)",
            flush=True,
        )
        return result.id

    except Exception as e:
        import traceback
        print(f"[telethon_upload] ❌ Upload failed: {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        return None
