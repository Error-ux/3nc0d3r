import asyncio
import os
import sys
import time
import traceback
import math
from pathlib import Path

# Add repo root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyrogram import Client, enums, raw, utils
from pyrogram.errors import FloodWait

# Patch for modern large Telegram IDs
utils.MIN_CHANNEL_ID = -1009999999999999

from utils.ui import get_download_ui

async def progress(current, total, app, chat_id, message, start_time):
    if not hasattr(progress, "last_update"):
        progress.last_update = 0

    if total <= 0:
        return

    now = time.time()
    # Throttled to 5 seconds for more responsiveness
    if now - progress.last_update < 5 and current < total:
        return

    progress.last_update = now
    percent = (current / total) * 100
    elapsed = now - start_time
    speed_bytes = current / elapsed if elapsed > 0 else 0
    speed_mb    = speed_bytes / (1024 * 1024)
    size_mb     = total / (1024 * 1024)
    eta         = (total - current) / speed_bytes if speed_bytes > 0 else 0

    ui_text = get_download_ui(percent, speed_mb, size_mb, elapsed, eta)
    try:
        await app.edit_message_text(chat_id, message.id, ui_text, parse_mode=enums.ParseMode.HTML)
    except Exception:
        pass

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

    tasks = [download_chunk(i * chunk_size) for i in range(part_count)]
    await asyncio.gather(*tasks)
    return file_name

async def main():
    try:
        api_id = int(os.environ.get("TG_API_ID", "0").strip())
        api_hash = os.environ.get("TG_API_HASH", "").strip()
        bot_token = os.environ.get("TG_BOT_TOKEN", "").strip()
        chat_id = int(os.environ.get("TG_CHAT_ID", "0").strip())
        url = os.environ.get("VIDEO_URL", "").strip()
    except ValueError as e:
        print(f"CRITICAL: Invalid Environment Variables. {e}")
        sys.exit(1)
    
    session_dir = "/tmp/tg_sessions"
    os.makedirs(session_dir, exist_ok=True)

    _ALL_LANES = [chr(ord("A") + i) for i in range(20)]
    run_number = int(os.environ.get("GITHUB_RUN_NUMBER", "0"))
    lane = _ALL_LANES[run_number % 20]
    print(f"Session lane: {lane} (run #{run_number})")
    session_path = os.path.join(session_dir, f"tg_dl_session_{lane}")

    try:
        app = Client(
            session_path, 
            api_id=api_id, 
            api_hash=api_hash, 
            bot_token=bot_token,
            max_concurrent_transmissions=16
        )
        
        for _attempt in range(5):
            try:
                await app.start()
                break
            except FloodWait as e:
                wait_secs = e.value + 5
                print(f"⏳ FloodWait on auth: waiting {wait_secs}s (attempt {_attempt + 1}/5)")
                await asyncio.sleep(wait_secs)
        else:
            print("❌ Could not authorize with Telegram after 5 attempts.")
            sys.exit(1)

        try:
            status = await app.send_message(
                chat_id, 
                "📡 <b>[ SYSTEM.INIT ] Establishing Downlink...</b>", 
                parse_mode=enums.ParseMode.HTML
            )
            
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
                    dl_result = await app.download_media(
                        message=file_id.strip(),
                        file_name="./source.mkv",
                        progress=progress,
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

        finally:
            await app.stop()

    except Exception as e:
        print(f"FATAL ERROR during download: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
