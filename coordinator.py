import asyncio
import json
import os
import subprocess
import time
import glob
from datetime import datetime, timezone

import requests
from pyrogram import enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

import config
from utils.tg_utils import connect_telegram, tg_edit, tg_notify_failure
from utils.ui import get_parallel_ui, format_time, upload_progress
from utils.rename import resolve_output_name, parse_from_filename, format_track_report

# ── GitHub API helpers ────────────────────────────────────────────────────────

GH_TOKEN = os.environ["GITHUB_TOKEN"]
GH_REPO  = os.environ["GITHUB_REPOSITORY"]
RUN_ID   = os.environ["GITHUB_RUN_ID"]
TOTAL_CHUNKS = int(os.getenv("CHUNKS", "10"))

_GH_HEADERS = {
    "Authorization": f"Bearer {GH_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

_DT_FMT = "%Y-%m-%dT%H:%M:%SZ"

def _parse_dt(s: str) -> datetime:
    return datetime.strptime(s, _DT_FMT).replace(tzinfo=timezone.utc)


def get_encode_jobs() -> list[dict]:
    """Return all jobs for this run whose name starts with 'Encode chunk-'."""
    url = f"https://api.github.com/repos/{GH_REPO}/actions/runs/{RUN_ID}/jobs"
    resp = requests.get(url, headers=_GH_HEADERS, params={"per_page": 100}, timeout=15)
    resp.raise_for_status()
    return [j for j in resp.json().get("jobs", []) if "Encode chunk-" in j["name"]]


def all_encode_jobs_done(jobs: list[dict]) -> bool:
    return (
        len(jobs) == TOTAL_CHUNKS
        and all(j["status"] == "completed" for j in jobs)
    )


def any_encode_job_failed(jobs: list[dict]) -> bool:
    return any(
        j["status"] == "completed" and j["conclusion"] != "success"
        for j in jobs
    )


def compute_eta(jobs: list[dict], n_done: int) -> float:
    """
    Live ETA: uses actual elapsed time for in-progress chunks.

    Completed chunks give us avg_duration.
    In-progress chunks have already consumed (now - started_at) seconds.
    Remaining per chunk = max(0, avg_duration - elapsed).
    Since all chunks run in parallel, ETA = slowest remaining chunk.
    Queued chunks haven't started yet — treat them as avg_duration remaining.
    """
    if n_done == TOTAL_CHUNKS:
        return 0.0

    now_utc = datetime.now(timezone.utc)

    # Collect completed durations
    completed_secs: list[float] = []
    for j in jobs:
        if j["status"] == "completed" and j.get("started_at") and j.get("completed_at"):
            s = _parse_dt(j["started_at"])
            e = _parse_dt(j["completed_at"])
            completed_secs.append((e - s).total_seconds())

    avg_dur = sum(completed_secs) / len(completed_secs) if completed_secs else None

    remaining: list[float] = []

    for j in jobs:
        if j["status"] == "completed":
            continue
        if j["status"] == "in_progress" and j.get("started_at"):
            elapsed_chunk = (now_utc - _parse_dt(j["started_at"])).total_seconds()
            if avg_dur is not None:
                remaining.append(max(0.0, avg_dur - elapsed_chunk))
            else:
                # No completed chunks yet — use elapsed as lower bound proxy
                remaining.append(elapsed_chunk * 0.5)
        else:
            # queued / waiting — full avg_duration expected
            if avg_dur is not None:
                remaining.append(avg_dur)

    return max(remaining) if remaining else 0.0


# ── Artifact download ─────────────────────────────────────────────────────────

def download_encoded_chunks():
    """Download all enc-NNN artifacts into ./encoded/ using gh CLI."""
    os.makedirs("encoded", exist_ok=True)
    for i in range(1, TOTAL_CHUNKS + 1):
        name = f"enc-{str(i).zfill(3)}"
        subprocess.run(
            ["gh", "run", "download", RUN_ID, "--name", name, "--dir", f"encoded/{name}"],
            check=True,
            env={**os.environ, "GH_TOKEN": GH_TOKEN},
        )
    print(f"[coordinator] All {TOTAL_CHUNKS} encoded chunks downloaded.")


def merge_chunks(output_path: str):
    """Concat encoded chunks into final output with ffmpeg (stream copy)."""
    chunks = sorted(glob.glob("encoded/enc-*/encoded_*.mkv"))
    if len(chunks) != TOTAL_CHUNKS:
        raise RuntimeError(f"Expected {TOTAL_CHUNKS} chunks, found {len(chunks)}")

    with open("concat.txt", "w") as f:
        for c in chunks:
            f.write(f"file '{os.path.abspath(c)}'\n")

    print(f"[coordinator] Merging {len(chunks)} chunks → {output_path}")
    subprocess.run(
        [
            "ffmpeg", "-hide_banner", "-loglevel", "warning",
            "-f", "concat", "-safe", "0", "-i", "concat.txt",
            "-c", "copy", output_path, "-y",
        ],
        check=True,
    )
    size_mb = os.path.getsize(output_path) / 1024 ** 2
    print(f"[coordinator] Merge complete — {size_mb:.1f} MB")
    return size_mb


# ── Main coordinator coroutine ────────────────────────────────────────────────

async def run():
    # Load source info written by prepare job
    with open("src/source_info.json") as f:
        src_info = json.load(f)

    file_name = open("src/tg_fname.txt").read().strip()

    final_crf    = config.USER_CRF    or "48"
    final_preset = config.USER_PRESET or "6"
    psy_rd       = config.PSY_RD      or "1.5"
    res_label    = config.USER_RES    or src_info.get("resolution", "Original")

    _gh_repo = GH_REPO
    _run_id  = RUN_ID
    _log_url = f"https://github.com/{_gh_repo}/actions/runs/{_run_id}"

    buttons = InlineKeyboardMarkup([[
        InlineKeyboardButton("📋 Open Log",    url=_log_url),
        InlineKeyboardButton("🛑 Terminate All", callback_data=f"kill_{_run_id}"),
    ]])

    # Connect TG
    tg_state = {}
    tg_ready = asyncio.Event()
    tg_task  = asyncio.create_task(
        connect_telegram(tg_state, tg_ready, file_name)
    )

    start_time = time.time()

    # ── Phase 1: Poll encode job statuses until all done ─────────────────────
    print(f"[coordinator] Monitoring {TOTAL_CHUNKS} encode jobs...")

    # Wait up to 3 min for the matrix jobs to appear (runner queue delay)
    deadline = time.time() + 180
    while time.time() < deadline:
        jobs = get_encode_jobs()
        if jobs:
            break
        await asyncio.sleep(5)

    last_update = 0

    while True:
        jobs = get_encode_jobs()

        elapsed = time.time() - start_time
        n_done  = sum(1 for j in jobs if j["status"] == "completed")
        now_ts  = time.time()

        eta = compute_eta(jobs, n_done)

        # Update TG every 5s — fast enough to feel live, safe for TG rate limits
        if now_ts - last_update >= 5:
            ui = get_parallel_ui(
                jobs=jobs,
                total=TOTAL_CHUNKS,
                n_done=n_done,
                elapsed=elapsed,
                eta=eta,
                file_name=file_name,
                crf=final_crf,
                preset=final_preset,
                psy_rd=psy_rd,
                res_label=res_label,
                now_utc=datetime.now(timezone.utc),
            )
            await tg_edit(tg_state, tg_ready, ui, reply_markup=buttons)
            last_update = now_ts

        if any_encode_job_failed(jobs):
            raise RuntimeError("One or more encode jobs failed — aborting merge.")

        if all_encode_jobs_done(jobs):
            print(f"[coordinator] All {TOTAL_CHUNKS} chunks done. Proceeding to merge.")
            break

        await asyncio.sleep(5)

    # ── Phase 2: Download + merge ─────────────────────────────────────────────
    merge_ui = (
        f"<code>┌─── 🔗 [ SYSTEM.MERGE.ACTIVE ] ───┐\n"
        f"│\n"
        f"│ 📂 FILE: {file_name}\n"
        f"│ 🔗 Merging {TOTAL_CHUNKS} chunks...\n"
        f"│\n"
        f"└────────────────────────────────────┘</code>"
    )
    await tg_edit(tg_state, tg_ready, merge_ui, reply_markup=buttons)

    download_encoded_chunks()

    parsed = parse_from_filename(file_name)
    if parsed:
        anime_name = parsed["anime_name"]
        season     = parsed["season"]
        episode    = parsed["episode"]
        is_special = parsed["is_special"]
    else:
        anime_name = file_name
        season, episode, is_special = 1, 1, False

    height = src_info.get("height", 0)

    # Use the first downloaded chunk as the probe source so ffprobe has a real file
    probe_source = sorted(glob.glob("encoded/enc-*/encoded_*.mkv"))
    probe_source = probe_source[0] if probe_source else file_name

    output_name, _, _, _ = resolve_output_name(
        source      = probe_source,
        anime_name  = anime_name,
        season      = season,
        episode     = episode,
        height      = height,
        is_special  = is_special,
    )
    size_mb = merge_chunks(output_name)

    # ── Phase 3: Send to TG ───────────────────────────────────────────────────
    total_elapsed = time.time() - start_time

    done_ui = (
        f"<code>┌─── ✅ [ MISSION.COMPLETE ] ────────┐\n"
        f"│\n"
        f"│ 📂 FILE: {file_name}\n"
        f"│ 🛠️  CRF {final_crf} | Preset {final_preset} | PSY-RD {psy_rd}\n"
        f"│ 🎞️  {res_label} | 10-bit | PSYEX\n"
        f"│ ⚡ {TOTAL_CHUNKS} chunks | Wall: {format_time(total_elapsed)}\n"
        f"│ 📦 {size_mb:.1f} MB\n"
        f"│\n"
        f"└────────────────────────────────────┘</code>"
    )
    await tg_edit(tg_state, tg_ready, done_ui)

    # Upload final file
    app = tg_state.get("app")
    chat_id = config.CHAT_ID
    if app and chat_id:
        status_msg = tg_state.get("status_msg")
        await app.send_document(
            chat_id,
            output_name,
            caption=done_ui,
            parse_mode=enums.ParseMode.HTML,
            progress=upload_progress,
            progress_args=(app, chat_id, status_msg, output_name),
        )

    tg_task.cancel()


if __name__ == "__main__":
    asyncio.run(run())
