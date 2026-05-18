"""
merge.py — Merge encoded chunks + post-processing for the batch matrix pipeline.

Steps:
  1. Validate all expected chunks present
  2. ffmpeg concat  → final_raw.mkv
  3. mkvmerge remux → {resolved_filename}.mkv  (chapters, encoder title tag)
  4. Thumbnail generation
  5. VMAF + SSIM    (optional, --run-vmaf)
  6. Gofile upload  (optional, --run-upload)
  7. Edit TG channel message with final report

Called by the finalize job in batch.yml.
Reads message_id from message_ids.json artifact (posted by batch_monitor.py).
All TG updates go to TG_CHANNEL_ID via bot token — no user session needed.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import config
from utils.media import (
    fmt_size, fmt_duration, FFMPEG,
    async_generate_thumbnail, get_vmaf, upload_to_cloud,
    verify_mkv_magic,
)
from utils.ui import generate_progress_bar, format_time, get_vmaf_ui


# ─── TG channel helper ────────────────────────────────────────────────────────

async def _bot_edit(bot_token: str, channel_id: int, message_id: int, text: str) -> None:
    """Edit a channel message using the bot token via Pyrogram."""
    try:
        from pyrogram import Client, enums
        async with Client("merge_bot", bot_token=bot_token,
                          api_id=config.API_ID, api_hash=config.API_HASH) as app:
            await app.edit_message_text(
                channel_id, message_id, text,
                parse_mode=enums.ParseMode.HTML,
            )
    except Exception as e:
        print(f"[TG] edit failed: {e}")


async def _bot_send(bot_token: str, channel_id: int, text: str) -> int:
    """Send a new message to the channel. Returns message_id."""
    try:
        from pyrogram import Client, enums
        async with Client("merge_bot", bot_token=bot_token,
                          api_id=config.API_ID, api_hash=config.API_HASH) as app:
            msg = await app.send_message(
                channel_id, text,
                parse_mode=enums.ParseMode.HTML,
            )
            return msg.id
    except Exception as e:
        print(f"[TG] send failed: {e}")
        return 0


# ─── Chunk validation ─────────────────────────────────────────────────────────

def validate_chunks(enc_dir: Path, expected: int) -> list[Path]:
    """
    Verify all expected part_*-encoded.mkv chunks are present and valid MKV.
    Returns sorted list on success, exits 1 on failure.
    """
    chunks = sorted(enc_dir.glob("*-encoded.mkv"))
    actual = len(chunks)

    if actual != expected:
        print(f"[ERROR] Chunk count mismatch: expected {expected}, found {actual}.")
        missing = expected - actual
        print(f"        {missing} chunk(s) missing — aborting merge.")
        sys.exit(1)

    bad = [c for c in chunks if not verify_mkv_magic(c)]
    if bad:
        print(f"[ERROR] Corrupt chunk(s) detected: {[c.name for c in bad]}")
        sys.exit(1)

    print(f"✅  All {actual} chunks validated.")
    return chunks


# ─── ffmpeg concat ────────────────────────────────────────────────────────────

def merge(encoded_chunks: list[Path], output: Path) -> None:
    """Concatenate sorted encoded chunks into a single output MKV."""
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)

    print(f"\n━━━ Merge ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", flush=True)
    print(f"  Chunks  : {len(encoded_chunks)}", flush=True)
    print(f"  Output  : {output}", flush=True)

    list_file = output.parent / ".concat_list.txt"
    with open(list_file, "w") as f:
        for c in sorted(encoded_chunks):
            f.write(f"file '{c.resolve()}'\n")

    result = subprocess.run(
        [FFMPEG, "-f", "concat", "-safe", "0",
         "-i", str(list_file),
         "-map", "0", "-c", "copy",
         str(output), "-y"],
        capture_output=True, text=True,
    )
    list_file.unlink(missing_ok=True)

    if result.returncode != 0:
        print(f"[ERROR] ffmpeg merge failed:\n{result.stderr}")
        sys.exit(1)

    size_mb = output.stat().st_size / 1_048_576
    print(f"✅  Merge complete → {output}  ({fmt_size(size_mb)})", flush=True)


# ─── mkvmerge remux ───────────────────────────────────────────────────────────

def remux_with_source(output: Path, source: Path, encoder_title: str = "") -> None:
    """
    Copy chapters and attachments from source into output.
    Stamp MKV Title tag with encoder_title.
    """
    if not source.exists():
        print(f"  source.mkv not found — skipping remux", flush=True)
        return

    fixed      = output.parent / f"FIXED_{output.name}"
    title_args = ["--title", encoder_title] if encoder_title.strip() else []

    result = subprocess.run(
        ["mkvmerge", "-o", str(fixed),
         *title_args,
         str(output),
         "--no-video", "--no-audio", "--no-subtitles", "--no-attachments",
         str(source)],
        capture_output=True,
    )

    if result.returncode == 0 and fixed.exists():
        output.unlink(missing_ok=True)
        fixed.rename(output)
        print("  ✅  mkvmerge remux complete", flush=True)
    else:
        print(f"  mkvmerge failed (rc={result.returncode}) — skipping", flush=True)
        fixed.unlink(missing_ok=True)


# ─── Async post-processing ────────────────────────────────────────────────────

async def post_process(
    output:        Path,
    source:        Path,
    crf:           int,
    preset:        int,
    chunk_count:   int,
    episode:       str,
    message_id:    int,
    bot_token:     str,
    channel_id:    int,
    alert_chat_id: int,
    run_vmaf:      bool = False,
    run_upload:    bool = False,
    encoder_title: str  = "",
    anime_name:    str  = "",
    season:        str  = "1",
    audio_type:    str  = "Auto",
    content_type:  str  = "Anime",
    sub_tracks_lbl: str = "",
    audio_tracks_lbl: str = "",
    duration:      float = 0.0,
    width:         int   = 0,
    height:        int   = 0,
    fps_val:       float = 24.0,
    crop_val:      str | None = None,
    is_hdr:        bool  = False,
    grain:         int   = 0,
    audio_bitrate: str   = "64k",
) -> None:
    """Full post-encode pipeline: remux → thumbnail → VMAF → upload → TG report."""

    async def _edit(text: str) -> None:
        if message_id:
            await _bot_edit(bot_token, channel_id, message_id, text)

    # ── 1. mkvmerge remux ─────────────────────────────────────────────────
    await _edit(
        f"<code>┌─── 🔀 [ MERGE.REMUX ] ───────────────────┐\n"
        f"│ Episode: {episode}\n"
        f"│ Remuxing chapters + metadata…\n"
        f"└────────────────────────────────────────────┘</code>"
    )
    remux_with_source(output, source, encoder_title)
    final_size = output.stat().st_size / 1_048_576

    # ── 2. Rename ──────────────────────────────────────────────────────────
    file_name    = output.name
    audio_tracks = []
    sub_tracks   = []

    if anime_name:
        try:
            from utils.rename import resolve_output_name, get_track_info
            resolved, audio_type_label, audio_tracks, sub_tracks = resolve_output_name(
                source              = str(output),
                anime_name          = anime_name,
                season              = season,
                episode             = episode,
                height              = height,
                audio_type_override = audio_type,
                content_type        = content_type,
            )
            new_path = output.parent / resolved
            output.rename(new_path)
            output    = new_path
            file_name = resolved
            print(f"  ✅  Renamed → {file_name}", flush=True)
        except Exception as exc:
            print(f"  Rename failed: {exc}", flush=True)
    else:
        try:
            from utils.rename import get_track_info
            audio_tracks, sub_tracks = get_track_info(str(output))
        except Exception:
            pass

    # ── 3. Thumbnail ───────────────────────────────────────────────────────
    grid_task = asyncio.create_task(
        async_generate_thumbnail(duration or 1.0, str(output))
    )

    # ── 4. VMAF ────────────────────────────────────────────────────────────
    vmaf_val = ssim_val = "N/A"
    if run_vmaf and source.exists():
        await _edit(
            f"<code>┌─── 🧠 [ VMAF.ANALYSIS ] ─────────────────┐\n"
            f"│ Episode: {episode}\n"
            f"│ Running VMAF + SSIM…\n"
            f"└────────────────────────────────────────────┘</code>"
        )

        async def _vmaf_progress(payload):
            ui = get_vmaf_ui(payload["vmaf_percent"], payload["fps"], payload["eta"])
            await _edit(ui)

        vmaf_val, ssim_val = await get_vmaf(
            str(output), crop_val, width, height,
            duration or 1.0, fps_val, kv_writer=_vmaf_progress,
        )
        print(f"  VMAF: {vmaf_val}  SSIM: {ssim_val}", flush=True)

    # ── 5. Upload ──────────────────────────────────────────────────────────
    cloud = {"direct": None, "page": None, "source": "disabled"}
    if run_upload:
        await _edit(
            f"<code>┌─── ☁️ [ GOFILE.UPLINK ] ─────────────────┐\n"
            f"│ Episode: {episode}\n"
            f"│ Uploading to Gofile…\n"
            f"└────────────────────────────────────────────┘</code>"
        )
        cloud = await upload_to_cloud(str(output))
        print(f"  Upload: {cloud['source']} — {cloud.get('page', 'N/A')}", flush=True)

    await grid_task

    # ── 6. Build inline buttons ────────────────────────────────────────────
    try:
        from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        btn_row = []
        gh_repo = os.environ.get("GITHUB_REPOSITORY", "")
        run_id  = os.environ.get("GITHUB_RUN_ID",     "")
        if gh_repo and run_id:
            log_url = f"https://github.com/{gh_repo}/actions/runs/{run_id}"
            btn_row.append(InlineKeyboardButton("📋 Actions Log", url=log_url))
        if cloud["source"] == "gofile" and cloud.get("page"):
            btn_row.append(InlineKeyboardButton("Gofile", url=cloud["page"]))
        elif cloud["source"] == "litterbox" and cloud.get("direct"):
            btn_row.append(InlineKeyboardButton("Litterbox", url=cloud["direct"]))
        buttons = InlineKeyboardMarkup([btn_row]) if btn_row else None
    except Exception:
        buttons = None

    # ── 7. Track report ────────────────────────────────────────────────────
    track_report = ""
    try:
        from utils.rename import format_track_report
        track_report = format_track_report(audio_tracks, sub_tracks)
    except Exception:
        pass

    user_notes = ""
    if sub_tracks_lbl:
        user_notes += f"\n🔤 <b>SUB LABELS:</b> <code>{sub_tracks_lbl}</code>"
    if audio_tracks_lbl:
        user_notes += f"\n🔊 <b>AUDIO LABELS:</b> <code>{audio_tracks_lbl}</code>"

    hdr_label   = "HDR10" if is_hdr else "SDR"
    grain_label = f" | Grain: {grain}" if grain else ""
    crop_label  = " | Cropped" if crop_val else ""

    cloud_line = ""
    if cloud["source"] == "gofile" and cloud.get("page"):
        cloud_line = f"\n🔗 <b>GOFILE:</b> {cloud['page']}"
    elif cloud["source"] == "litterbox" and cloud.get("direct"):
        cloud_line = f"\n🔗 <b>LITTERBOX:</b> {cloud['direct']}"

    final_report = (
        f"✅ <b>EPISODE COMPLETE</b>\n\n"
        f"📄 <b>FILE:</b> <code>{file_name}</code>\n"
        f"📦 <b>SIZE:</b> <code>{final_size:.2f} MB</code>\n"
        f"⏱ <b>DURATION:</b> <code>{fmt_duration(duration)}</code>\n\n"
        f"📊 <b>QUALITY:</b> VMAF: <code>{vmaf_val}</code> | SSIM: <code>{ssim_val}</code>\n\n"
        f"🛠 <b>SPECS:</b>\n"
        f"└ Preset: {preset} | CRF: {crf} | Chunks: {chunk_count}\n"
        f"└ Video: {hdr_label}{crop_label}{grain_label}\n"
        f"└ Audio: opus @ {audio_bitrate}\n"
        f"└ Type: {content_type}\n"
        f"\n{track_report}"
        f"{user_notes}"
        f"{cloud_line}"
    )

    # Edit the channel message to the final report
    await _edit(final_report)

    # Also send inline buttons as a separate edit if buttons exist
    if buttons and message_id:
        try:
            from pyrogram import Client, enums
            async with Client("merge_bot_btn", bot_token=bot_token,
                              api_id=config.API_ID, api_hash=config.API_HASH) as app:
                await app.edit_message_text(
                    channel_id, message_id, final_report,
                    parse_mode=enums.ParseMode.HTML,
                    reply_markup=buttons,
                )
        except Exception as e:
            print(f"[TG] button edit failed: {e}")

    print(f"\n━━━ ✅  Episode {episode} complete ━━━━━━━━━━━━━━━━━━━━━━━━", flush=True)


# ─── CLI entry point ──────────────────────────────────────────────────────────

def _parse_args():
    p = argparse.ArgumentParser(description="Merge + post-process encoded chunks")
    p.add_argument("--enc-dir",       default="encoded-parts",  help="Directory containing *-encoded.mkv")
    p.add_argument("--output",        default="encoded",         help="Output name without extension")
    p.add_argument("--expected",      type=int, default=None,    help="Expected chunk count (validates)")
    p.add_argument("--episode",       default="1",               help="Episode number (for TG display)")
    p.add_argument("--message-ids",   default="message_ids.json")
    p.add_argument("--crf",           type=int, default=50)
    p.add_argument("--preset",        type=int, default=4)
    p.add_argument("--grain",         type=int, default=0)
    p.add_argument("--crop",          default=None)
    p.add_argument("--hdr",           action="store_true")
    p.add_argument("--audio-bitrate", default="64k")
    p.add_argument("--run-vmaf",      action="store_true")
    p.add_argument("--run-upload",    action="store_true")
    p.add_argument("--encoder-title", default="")
    p.add_argument("--anime-name",    default="")
    p.add_argument("--season",        default="1")
    p.add_argument("--audio-type",    default="Auto")
    p.add_argument("--content-type",  default="Anime")
    p.add_argument("--sub-tracks",    default="")
    p.add_argument("--audio-tracks",  default="")
    p.add_argument("--params-file",   default=None,             help="Path to encode_params.json")
    return p.parse_args()


if __name__ == "__main__":
    args    = _parse_args()
    enc_dir = Path(args.enc_dir)

    # Load encode_params.json if available
    params: dict = {}
    if args.params_file and Path(args.params_file).exists():
        with open(args.params_file) as f:
            params = json.load(f)

    # Resolve TG credentials
    bot_token    = os.environ.get("TG_BOT_TOKEN", config.BOT_TOKEN)
    channel_id   = int(os.environ.get("TG_CHANNEL_ID", "0"))
    alert_chat   = int(os.environ.get("TG_CHAT_ID", str(config.CHAT_ID)))

    # Read message_id for this episode from the monitor artifact
    message_id = 0
    msg_ids_path = Path(args.message_ids)
    if msg_ids_path.exists():
        try:
            msg_ids = json.loads(msg_ids_path.read_text())
            message_id = int(msg_ids.get(str(args.episode), 0))
        except Exception as e:
            print(f"[merge] Could not read message_ids.json: {e}")

    # Validate and merge
    expected = args.expected or params.get("chunk_count")
    chunks   = validate_chunks(enc_dir, expected) if expected else sorted(enc_dir.glob("*-encoded.mkv"))

    output   = Path(f"{args.output}.mkv")
    merge(chunks, output)

    # Post-process
    source = Path("source.mkv")
    run_vmaf   = args.run_vmaf   or config.RUN_VMAF
    run_upload = args.run_upload or config.RUN_UPLOAD

    asyncio.run(post_process(
        output          = output,
        source          = source,
        crf             = params.get("crf",           args.crf),
        preset          = params.get("preset",         args.preset),
        chunk_count     = len(chunks),
        episode         = args.episode,
        message_id      = message_id,
        bot_token       = bot_token,
        channel_id      = channel_id,
        alert_chat_id   = alert_chat,
        run_vmaf        = run_vmaf,
        run_upload      = run_upload,
        encoder_title   = args.encoder_title or config.ENCODER_TITLE,
        anime_name      = args.anime_name    or config.ANIME_NAME,
        season          = args.season        or config.SEASON,
        audio_type      = args.audio_type    or config.AUDIO_TYPE,
        content_type    = args.content_type  or config.CONTENT_TYPE,
        sub_tracks_lbl  = args.sub_tracks    or config.SUB_TRACKS,
        audio_tracks_lbl= args.audio_tracks  or config.AUDIO_TRACKS,
        duration        = params.get("duration",      0.0),
        width           = params.get("width",         0),
        height          = params.get("height",        0),
        fps_val         = params.get("fps_val",       24.0),
        crop_val        = params.get("crop_val",      args.crop),
        is_hdr          = params.get("is_hdr",        args.hdr),
        grain           = params.get("grain",         args.grain),
        audio_bitrate   = params.get("audio_bitrate", args.audio_bitrate),
    ))
