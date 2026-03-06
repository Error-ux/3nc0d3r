import asyncio
import os
import sys
import time
import traceback
from pyrogram import Client, enums
from pyrogram.errors import FloodWait
from ui import get_download_ui

async def progress(current, total, app, chat_id, message, start_time):
    if not hasattr(progress, "last_pct"):
        progress.last_pct = -1

    if total <= 0:
        return

    percent  = (current / total) * 100
    milestone = int(percent // 5) * 5

    if milestone <= progress.last_pct:
        return

    progress.last_pct = milestone
    elapsed = time.time() - start_time
    speed_bytes = current / elapsed if elapsed > 0 else 0
    speed_mb    = speed_bytes / (1024 * 1024)
    size_mb     = total / (1024 * 1024)
    eta         = (total - current) / speed_bytes if speed_bytes > 0 else 0

    ui_text = get_download_ui(percent, speed_mb, size_mb, elapsed, eta)
    try:
        await app.edit_message_text(chat_id, message.id, ui_text, parse_mode=enums.ParseMode.HTML)
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
    except Exception:
        pass

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
    
    session_dir = "tg_session_dir"
    os.makedirs(session_dir, exist_ok=True)
    session_path = os.path.join(session_dir, "tg_dl_session")

    try:
        app = Client(session_path, api_id=api_id, api_hash=api_hash, bot_token=bot_token)
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
                
                await app.download_media(
                    msg, 
                    file_name="./source.mkv",
                    progress=progress, 
                    progress_args=(app, chat_id, status, start_time)
                )

            elif "tg_file:" in url:
                raw_data = url.replace("tg_file:", "")
                
                if "|" in raw_data:
                    file_id, final_name = raw_data.split("|", 1)
                else:
                    file_id = raw_data
                
                await app.download_media(
                    message=file_id.strip(), 
                    file_name="./source.mkv",
                    progress=progress, 
                    progress_args=(app, chat_id, status, start_time)
                )
            
            else:
                await app.edit_message_text(chat_id, status.id, "❌ <b>ERROR: Unsupported URL format.</b>", parse_mode=enums.ParseMode.HTML)
                sys.exit(1)

            # Keep phase changes directly in Telegram so you know when it moves to encode
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