import asyncio
import json
import os
import subprocess
import time
import glob

import requests
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

import config
from utils.tg_utils import connect_telegram, tg_edit, tg_notify_failure
from utils.ui import get_parallel_ui, format_time, upload_progress
from utils.rename import resolve_output_name, format_track_report

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
        await asyncio.sleep(15)

    last_update = 0

    while True:
        jobs = get_encode_jobs()

        elapsed = time.time() - start_time
        n_done  = sum(1 for j in jobs if j["status"] == "completed")

        # ETA: extrapolate from completed jobs' wall times
        eta = 0.0
        durations = []
        for j in jobs:
            if j["status"] == "completed" and j.get("started_at") and j.get("completed_at"):
                from datetime import datetime, timezone
                fmt = "%Y-%m-%dT%H:%M:%SZ"
                s = datetime.strptime(j["started_at"],   fmt).replace(tzinfo=timezone.utc)
                e = datetime.strptime(j["completed_at"], fmt).replace(tzinfo=timezone.utc)
                durations.append((e - s).total_seconds())
        if durations and n_done < TOTAL_CHUNKS:
            avg = sum(durations) / len(durations)
            eta = avg * (TOTAL_CHUNKS - n_done)

        # Update TG every 20s
        if time.time() - last_update >= 20:
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
            )
            await tg_edit(tg_state, tg_ready, ui, reply_markup=buttons)
            last_update = time.time()

        if any_encode_job_failed(jobs):
            raise RuntimeError("One or more encode jobs failed — aborting merge.")

        if all_encode_jobs_done(jobs):
            print(f"[coordinator] All {TOTAL_CHUNKS} chunks done. Proceeding to merge.")
            break

        await asyncio.sleep(15)

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

    output_name = resolve_output_name(file_name)
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
            parse_mode="html",
            progress=upload_progress,
            progress_args=(app, chat_id, status_msg, output_name),
        )

    tg_task.cancel()


if __name__ == "__main__":
    asyncio.run(run())
