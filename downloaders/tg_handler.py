import asyncio
import os
import sys
import time
import traceback
import math
import logging
import builtins
from pathlib import Path

# Prevent any interactive input prompts in non-interactive environments
def no_interactive_input(prompt=""):
    raise RuntimeError(f"Interactive terminal input requested: '{prompt}'. Failing fast.")
builtins.input = no_interactive_input

# Disable debug logging for Pyrogram to prevent dumping MTProto packet hex/binary data
logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logging.getLogger("pyrogram").setLevel(logging.WARNING)

# Add repo root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyrogram import Client, enums, raw, utils
from pyrogram.errors import FloodWait

# Patch for modern large Telegram IDs
utils.MIN_CHANNEL_ID = -1009999999999999

from utils.ui import get_download_ui, format_time

async def progress(current, total, app, chat_id, message, start_time):
    if not hasattr(progress, "last_update"):
        progress.last_update = 0

    now = time.time()
    # Throttle log + UI updates to 30 seconds unless it's the very first or final callback
    if now - progress.last_update < 30 and current != total:
        return

    progress.last_update = now
    pct = (current / total * 100) if total > 0 else 0
    print(f"📥 [PROGRESS] Downloaded {current}/{total} bytes ({pct:.1f}%)", flush=True)

    if total <= 0:
        # Download with unknown size — show beautiful active scrolling progress bar
        downloaded_mb = current / (1024 * 1024)
        elapsed = now - start_time
        speed_bytes = current / elapsed if elapsed > 0 else 0
        speed_mb = speed_bytes / (1024 * 1024)
        
        # Dynamic active scrolling loader bar
        animation_chars = [
            "[▰▱▱▱▱▱▱▱▱▱▱▱▱▱▱]",
            "[▱▰▱▱▱▱▱▱▱▱▱▱▱▱▱]",
            "[▱▱▰▱▱▱▱▱▱▱▱▱▱▱▱]",
            "[▱▱▱▰▱▱▱▱▱▱▱▱▱▱▱]",
            "[▱▱▱▱▰▱▱▱▱▱▱▱▱▱▱]",
            "[▱▱▱▱▱▰▱▱▱▱▱▱▱▱▱]",
            "[▱▱▱▱▱▱▰▱▱▱▱▱▱▱▱]",
            "[▱▱▱▱▱▱▱▰▱▱▱▱▱▱▱]",
            "[▱▱▱▱▱▱▱▱▰▱▱▱▱▱▱]",
            "[▱▱▱▱▱▱▱▱▱▰▱▱▱▱▱]",
            "[▱▱▱▱▱▱▱▱▱▱▰▱▱▱▱]",
            "[▱▱▱▱▱▱▱▱▱▱▱▰▱▱▱]",
            "[▱▱▱▱▱▱▱▱▱▱▱▱▰▱▱]",
            "[▱▱▱▱▱▱▱▱▱▱▱▱▱▰▱]",
            "[▱▱▱▱▱▱▱▱▱▱▱▱▱▱▰]",
        ]
        frame = int(elapsed * 2) % len(animation_chars)
        bar = animation_chars[frame]
        
        ui_text = (
            f"<code>┌─── 🛰️ [ SYSTEM.DOWNLOAD.ACTIVE ] ───┐\n"
            f"│                                    \n"
            f"│ 📥 STATUS: Fetching from Telegram  \n"
            f"│ 📊 PROG: {bar} (Size Unknown)\n"
            f"│ ⚡ SPEED: {speed_mb:.2f} MB/s\n"
            f"│ 📦 SIZE: {downloaded_mb:.2f} MB\n"
            f"│ ⏳ TIME: {format_time(elapsed)}\n"
            f"│                                    \n"
            f"└────────────────────────────────────┘</code>"
        )
        try:
            await app.edit_message_text(chat_id, message.id, ui_text, parse_mode=enums.ParseMode.HTML)
        except Exception as e:
            print(f"❌ [PROGRESS] Telegram UI update failed: {e}", flush=True)
        return

    percent = pct
    elapsed = now - start_time
    speed_bytes = current / elapsed if elapsed > 0 else 0
    speed_mb    = speed_bytes / (1024 * 1024)
    size_mb     = total / (1024 * 1024)
    eta         = (total - current) / speed_bytes if speed_bytes > 0 else 0

    ui_text = get_download_ui(percent, speed_mb, size_mb, elapsed, eta)
    try:
        await app.edit_message_text(chat_id, message.id, ui_text, parse_mode=enums.ParseMode.HTML)
    except Exception as e:
        print(f"❌ [PROGRESS] Telegram UI update failed: {e}", flush=True)

async def fast_download(client, message, file_name, progress_callback, progress_args):
    """
    Downloads a file in parallel chunks for maximum speed.
    """
    media = message.document or message.video or message.audio or message.animation
    if not media:
        return None

    file_size = media.file_size
    # Constant chunk size for MTProto alignment
    chunk_size = 1024 * 1024 
    part_count = math.ceil(file_size / chunk_size)
    
    from pyrogram.file_id import FileId, FileType
    decoded = FileId.decode(media.file_id)
    
    if decoded.file_type in [FileType.PHOTO, FileType.THUMBNAIL]:
        location = raw.types.InputPhotoFileLocation(
            id=decoded.media_id,
            access_hash=decoded.access_hash,
            file_reference=decoded.file_reference,
            thumb_size=decoded.thumbnail_size
        )
    else:
        location = raw.types.InputDocumentFileLocation(
            id=decoded.media_id,
            access_hash=decoded.access_hash,
            file_reference=decoded.file_reference,
            thumb_size=decoded.thumbnail_size or ""
        )

    # Pre-allocate file space
    with open(file_name, "wb") as f:
        f.truncate(file_size)

    semaphore = asyncio.Semaphore(16)
    downloaded_chunks = 0
    start_time = progress_args[-1]

    async def download_chunk(offset):
        nonlocal downloaded_chunks
        async with semaphore:
            for _ in range(3): 
                try:
                    r = await client.invoke(
                        raw.functions.upload.GetFile(
                            location=location,
                            offset=offset,
                            limit=chunk_size # Keep limit constant for all chunks
                        )
                    )
                    if isinstance(r, raw.types.upload.File):
                        fd = os.open(file_name, os.O_RDWR)
                        try:
                            os.pwrite(fd, r.bytes, offset)
                        finally:
                            os.close(fd)
                    break
                except FloodWait as e:
                    await asyncio.sleep(e.value + 2)
                except Exception as e:
                    print(f"Error downloading chunk at {offset}: {e}")
                    await asyncio.sleep(2)
            
            downloaded_chunks += 1
            await progress_callback(
                min(downloaded_chunks * chunk_size, file_size), 
                file_size, 
                *progress_args
            )

    # Local Progress
    await progress_callback(0, file_size, *progress_args)

    tasks = [download_chunk(i * chunk_size) for i in range(part_count)]
    await asyncio.gather(*tasks)
    return file_name

async def fast_download_file_id(client, file_id, file_name, progress_callback, progress_args):
    """
    Downloads a file by raw file_id string in parallel chunks (up to 16 concurrent connections)
    without needing a Message object. Dynamically discovers total_size during download.
    """
    from pyrogram.file_id import FileId, FileType
    decoded = FileId.decode(file_id)
    
    if decoded.file_type in [FileType.PHOTO, FileType.THUMBNAIL]:
        location = raw.types.InputPhotoFileLocation(
            id=decoded.media_id,
            access_hash=decoded.access_hash,
            file_reference=decoded.file_reference,
            thumb_size=decoded.thumbnail_size
        )
    else:
        location = raw.types.InputDocumentFileLocation(
            id=decoded.media_id,
            access_hash=decoded.access_hash,
            file_reference=decoded.file_reference,
            thumb_size=decoded.thumbnail_size or ""
        )

    chunk_size = 1024 * 1024  # 1MB chunks
    downloaded_chunks = 0
    total_size = None
    finished_event = asyncio.Event()
    
    # Open file for writing offsets concurrently
    fd = os.open(file_name, os.O_CREAT | os.O_RDWR)
    
    semaphore = asyncio.Semaphore(16)
    next_offset = 0
    downloaded_bytes = 0
    active_tasks = set()
    start_time = progress_args[-1]
    
    async def download_chunk(offset):
        nonlocal total_size, downloaded_bytes, downloaded_chunks
        async with semaphore:
            if finished_event.is_set() and (total_size is not None and offset >= total_size):
                return
            
            for attempt in range(3):
                try:
                    r = await client.invoke(
                        raw.functions.upload.GetFile(
                            location=location,
                            offset=offset,
                            limit=chunk_size
                        )
                    )
                    if isinstance(r, raw.types.upload.File):
                        chunk_bytes = r.bytes
                        chunk_len = len(chunk_bytes)
                        
                        if chunk_len > 0:
                            os.pwrite(fd, chunk_bytes, offset)
                            downloaded_bytes += chunk_len
                            
                        # If returned chunk is smaller than limit, we reached the end!
                        if chunk_len < chunk_size:
                            total_size = offset + chunk_len
                            finished_event.set()
                            
                    break
                except FloodWait as e:
                    await asyncio.sleep(e.value + 2)
                except Exception as e:
                    print(f"Error downloading chunk at {offset}: {e}")
                    await asyncio.sleep(2)
            
            downloaded_chunks += 1
            await progress_callback(
                downloaded_bytes, 
                total_size or 0, 
                *progress_args
            )

    # Coordinated loop to run up to 16 tasks concurrently
    while not finished_event.is_set():
        while len(active_tasks) < 16 and not finished_event.is_set():
            task = asyncio.create_task(download_chunk(next_offset))
            active_tasks.add(task)
            next_offset += chunk_size
            
        if active_tasks:
            done, pending = await asyncio.wait(active_tasks, return_when=asyncio.FIRST_COMPLETED)
            active_tasks = pending
        else:
            break

    # Wait for remaining active tasks below total_size to finish
    if active_tasks:
        valid_tasks = []
        for t in active_tasks:
            # Task offsets beyond total_size can be ignored
            valid_tasks.append(t)
        if valid_tasks:
            await asyncio.gather(*valid_tasks, return_exceptions=True)
        
    os.close(fd)
    
    # Truncate to exact final size
    if total_size is not None:
        with open(file_name, "ab") as f:
            f.truncate(total_size)
            
    return file_name

async def main():
    try:
        api_id_str = os.environ.get("TG_API_ID", "").strip()
        api_hash = os.environ.get("TG_API_HASH", "").strip()
        bot_token = os.environ.get("TG_BOT_TOKEN", "").strip()
        chat_id_str = os.environ.get("TG_CHAT_ID", "").strip()
        url = os.environ.get("VIDEO_URL", "").strip()

        print(f"DEBUG ENV: TG_API_ID present={bool(api_id_str)} (len={len(api_id_str)})", flush=True)
        print(f"DEBUG ENV: TG_API_HASH present={bool(api_hash)} (len={len(api_hash)})", flush=True)
        print(f"DEBUG ENV: TG_BOT_TOKEN present={bool(bot_token)} (len={len(bot_token)})", flush=True)
        print(f"DEBUG ENV: TG_CHAT_ID present={bool(chat_id_str)} (len={len(chat_id_str)})", flush=True)
        print(f"DEBUG ENV: VIDEO_URL present={bool(url)} (len={len(url)})", flush=True)

        api_id = int(api_id_str) if api_id_str else 0
        chat_id = int(chat_id_str) if chat_id_str else 0
    except ValueError as e:
        print(f"CRITICAL: Invalid Environment Variables. {e}", flush=True)
        sys.exit(1)
    
    try:
        from utils.tg_utils import _resolve_session_names
        session_names = _resolve_session_names()
        print(f"DEBUG: Prioritized session names to try: {session_names}", flush=True)

        app = None
        flood_waits = {}

        for session_name in session_names:
            print(f"DEBUG: Trying session: {session_name}...", flush=True)
            try:
                candidate = Client(
                    session_name,
                    api_id=api_id,
                    api_hash=api_hash,
                    bot_token=bot_token or None,
                    max_concurrent_transmissions=16
                )
                await candidate.start()
                app = candidate
                print(f"DEBUG: TG auth OK with session: {session_name}", flush=True)
                break
            except FloodWait as e:
                flood_waits[session_name] = e.value
                print(f"DEBUG: FloodWait {e.value}s on '{session_name}' — trying next...", flush=True)
                continue
            except Exception as e:
                print(f"DEBUG: TG auth error on '{session_name}': {e} — trying next...", flush=True)
                continue

        if app is None and flood_waits:
            best_session = min(flood_waits, key=flood_waits.get)
            wait_secs = flood_waits[best_session]
            attempt = 0
            while True:
                attempt += 1
                print(f"All sessions flooded. Sleeping {wait_secs}s for '{best_session}' (attempt {attempt})...", flush=True)
                await asyncio.sleep(wait_secs + 5)
                try:
                    candidate = Client(
                        best_session,
                        api_id=api_id,
                        api_hash=api_hash,
                        bot_token=bot_token or None,
                        max_concurrent_transmissions=16
                    )
                    await candidate.start()
                    app = candidate
                    print(f"DEBUG: TG auth OK (post-flood attempt {attempt}): {best_session}", flush=True)
                    break
                except FloodWait as e:
                    wait_secs = e.value
                    print(f"DEBUG: Another FloodWait: {wait_secs}s — retrying...", flush=True)
                    continue
                except Exception as e:
                    print(f"DEBUG: TG auth failed on post-flood attempt {attempt}: {e}", flush=True)
                    break

        if app is None:
            print("❌ Could not authorize with Telegram. No usable session found.", flush=True)
            sys.exit(1)

        try:
            status = await app.send_message(
                chat_id, 
                "📡 <b>[ SYSTEM.INIT ] Establishing Downlink...</b>", 
                parse_mode=enums.ParseMode.HTML
            )
            
            try:
                with open("dl_msg_id.txt", "w") as f:
                    f.write(str(status.id))
            except Exception:
                pass
            
            start_time = time.time()
            final_name = "video.mkv"

            if "t.me/" in url:
                link = url.rstrip("/")
                parts = link.split("/")
                
                try:
                    msg_id = int(parts[-1].split("?")[0])
                except (ValueError, IndexError):
                    print("❌ Could not parse Message ID from link.")
                    sys.exit(1)
                
                if len(parts) >= 4 and parts[-3] == "c":
                    target_chat = int(f"-100{parts[-2]}")
                else:
                    target_chat = parts[-2]
                
                try: 
                    await app.get_chat(target_chat)
                except Exception: 
                    pass
                
                msg = await app.get_messages(target_chat, msg_id)
                
                if not msg or not msg.media:
                    await app.edit_message_text(chat_id, status.id, "❌ <b>ERROR: No media found in link.</b>", parse_mode=enums.ParseMode.HTML)
                    sys.exit(1)
                
                media = msg.video or msg.document or msg.audio
                final_name = getattr(media, "file_name", "video.mkv")
                
                try:
                    from utils.tg_simple import notify_private
                    notify_private(f"📥 <b>[ DOWNLOAD STARTED ]</b>\n📄 <b>FILE:</b> <code>{final_name}</code>\n⚙️ <b>VIA:</b> <code>Telegram Message Link</code>")
                except Exception:
                    pass

                try:
                    # Use Fast Download
                    dl_result = await fast_download(
                        app,
                        msg,
                        file_name="./source.mkv",
                        progress_callback=progress,
                        progress_args=(app, chat_id, status, start_time)
                    )
                except Exception as dl_err:
                    err_msg = f"fast_download raised an exception: {dl_err}"
                    print(f"❌ {err_msg}")
                    traceback.print_exc()
                    await app.edit_message_text(
                        chat_id, status.id,
                        f"❌ <b>[ DOWNLOAD.FAILED ]</b>\n<code>{err_msg}</code>",
                        parse_mode=enums.ParseMode.HTML
                    )
                    sys.exit(1)

            elif "tg_file:" in url:
                raw_data = url.replace("tg_file:", "")
                
                if "|" in raw_data:
                    file_id, final_name = raw_data.split("|", 1)
                else:
                    file_id = raw_data
                
                try:
                    from utils.tg_simple import notify_private
                    notify_private(f"📥 <b>[ DOWNLOAD STARTED ]</b>\n📄 <b>FILE:</b> <code>{final_name}</code>\n⚙️ <b>VIA:</b> <code>Telegram File ID</code>")
                except Exception:
                    pass

                try:
                    dl_result = await fast_download_file_id(
                        app,
                        file_id=file_id.strip(),
                        file_name="./source.mkv",
                        progress_callback=progress,
                        progress_args=(app, chat_id, status, start_time)
                    )
                except Exception as dl_err:
                    err_msg = f"download_media raised an exception: {dl_err}"
                    print(f"❌ {err_msg}")
                    await app.edit_message_text(
                        chat_id, status.id,
                        f"❌ <b>[ DOWNLOAD.FAILED ]</b>\n<code>{err_msg}</code>",
                        parse_mode=enums.ParseMode.HTML
                    )
                    sys.exit(1)
            else:
                await app.edit_message_text(chat_id, status.id, "❌ <b>ERROR: Unsupported URL format.</b>", parse_mode=enums.ParseMode.HTML)
                sys.exit(1)

            # Pyrogram's download_media often modifies the file extension automatically
            # based on MIME type. Rename the returned path to our expected "./source.mkv".
            if dl_result and dl_result != "./source.mkv" and os.path.exists(dl_result):
                try:
                    if os.path.exists("./source.mkv"):
                        os.remove("./source.mkv")
                    os.rename(dl_result, "./source.mkv")
                    print(f"DEBUG: Successfully renamed {dl_result} to ./source.mkv", flush=True)
                except Exception as rename_err:
                    print(f"WARNING: Failed to rename {dl_result} to ./source.mkv: {rename_err}", flush=True)

            source_path = "./source.mkv"
            if not os.path.exists(source_path) or os.path.getsize(source_path) == 0:
                err_msg = "source.mkv invalid after download. Check file_id access or bot permissions."
                print(f"❌ {err_msg}")
                await app.edit_message_text(
                    chat_id, status.id,
                    f"❌ <b>[ DOWNLOAD.FAILED ]</b>\n<code>{err_msg}</code>",
                    parse_mode=enums.ParseMode.HTML
                )
                sys.exit(1)

            await app.edit_message_text(
                chat_id, 
                status.id, 
                "✅ <b>[ DOWNLOAD.COMPLETE ] Transferring to Encoder...</b>", 
                parse_mode=enums.ParseMode.HTML
            )
            
            with open("tg_fname.txt", "w", encoding="utf-8") as f:
                f.write(final_name)

            try:
                from utils.tg_simple import notify_private
                notify_private(f"✅ <b>[ DOWNLOAD COMPLETED ]</b>\n📄 <b>FILE:</b> <code>{final_name}</code>")
            except Exception:
                pass

        finally:
            await app.stop()

    except Exception as e:
        print(f"FATAL ERROR during download: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
