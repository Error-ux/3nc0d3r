"""
batch_monitor.py — Live progress monitor for fast_encode=true mode.

Runs concurrently with the encode-chunks matrix job.
- Posts ONE message per episode to TG_CHANNEL_ID at startup
- Polls GitHub Actions Jobs API every 4 seconds
- Edits each episode message with per-chunk progress
- Writes message_ids.json artifact so finalize job can edit same messages
- Exits cleanly when all encode-chunk jobs are complete or failed

Environment variables (set by batch.yml):
    GITHUB_TOKEN         auto-provided by Actions
    GITHUB_REPOSITORY    e.g. user/repo
    GITHUB_RUN_ID        current run ID
    TG_BOT_TOKEN
    TG_CHANNEL_ID
    TG_API_ID
    TG_API_HASH
    EPISODE_LIST_JSON    JSON array: [{episode, chunks_for_this_episode, output_name}, ...]
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import aiohttp
import config
from utils.media import progress_bar as mk_bar, fmt_duration

# ─── Constants ────────────────────────────────────────────────────────────────

POLL_INTERVAL  = 4          # seconds between API polls
GITHUB_TOKEN   = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO    = os.environ.get("GITHUB_REPOSITORY", "")
GITHUB_RUN_ID  = os.environ.get("GITHUB_RUN_ID",     "")
BOT_TOKEN      = os.environ.get("TG_BOT_TOKEN",      config.BOT_TOKEN)
CHANNEL_ID     = int(os.environ.get("TG_CHANNEL_ID", "0"))
MSG_IDS_FILE   = Path("message_ids.json")

JOBS_URL = (
    f"https://api.github.com/repos/{GITHUB_REPO}/actions/runs/{GITHUB_RUN_ID}/jobs"
    f"?per_page=100"
)

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept":        "application/vnd.github+json",
    "X-GitHub-Api-Version": "2026-01-01",
}


# ─── TG helpers ───────────────────────────────────────────────────────────────

async def bot_send(app, text: str) -> int:
    """Send a message to the channel. Returns message_id."""
    try:
        from pyrogram import enums
        msg = await app.send_message(
            CHANNEL_ID, text,
            parse_mode=enums.ParseMode.HTML,
        )
        return msg.id
    except Exception as e:
        print(f"[TG] send failed: {e}")
        return 0


async def bot_edit(app, message_id: int, text: str) -> None:
    """Edit a channel message."""
    if not message_id:
        return
    try:
        from pyrogram import enums
        await app.edit_message_text(
            CHANNEL_ID, message_id, text,
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as e:
        # FloodWait is the most common — log and continue
        print(f"[TG] edit {message_id}: {e}")


# ─── GitHub API helpers ───────────────────────────────────────────────────────

async def fetch_jobs(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch all jobs for this run. Returns list of job dicts."""
    jobs = []
    url  = JOBS_URL
    while url:
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    print(f"[API] HTTP {resp.status} — retrying next poll")
                    return jobs
                data  = await resp.json()
                jobs += data.get("jobs", [])
                # Pagination
                link  = resp.headers.get("Link", "")
                url   = None
                for part in link.split(","):
                    if 'rel="next"' in part:
                        url = part.split(";")[0].strip().strip("<>")
                        break
        except Exception as e:
            print(f"[API] fetch error: {e}")
            break
    return jobs


def parse_chunk_jobs(jobs: list[dict], episode_list: list[dict]) -> dict:
    """
    Parse jobs list into per-episode chunk status counts.

    encode-chunks matrix jobs are named:
        encode-chunks (episode: N, chunk_index: M, ...)
    
    Returns dict keyed by episode str:
        { "1": {"total": 4, "done": 2, "running": 1, "queued": 1, "failed": 0}, ... }
    """
    # Build expected chunk counts per episode
    ep_chunks = {str(ep["episode"]): ep["chunks_for_this_episode"] for ep in episode_list}
    status_map = {str(ep["episode"]): {
        "total":   ep["chunks_for_this_episode"],
        "done":    0,
        "running": 0,
        "queued":  0,
        "failed":  0,
    } for ep in episode_list}

    for job in jobs:
        name = job.get("name", "")
        # Match: "encode-chunks (episode: 2, chunk_index: 1, ...)"
        if not name.startswith("encode-chunks"):
            continue

        # Extract episode number from job name
        ep_num = None
        try:
            # GitHub Actions matrix job names contain the matrix values in parens
            inside = name[name.index("(")+1:name.index(")")]
            for part in inside.split(","):
                part = part.strip()
                if part.startswith("episode:"):
                    ep_num = part.split(":")[1].strip()
                    break
        except Exception:
            continue

        if ep_num not in status_map:
            continue

        conclusion = job.get("conclusion")  # null, success, failure, cancelled
        status     = job.get("status")      # queued, in_progress, completed

        if status == "completed":
            if conclusion == "success":
                status_map[ep_num]["done"] += 1
            else:
                status_map[ep_num]["failed"] += 1
        elif status == "in_progress":
            status_map[ep_num]["running"] += 1
        else:
            status_map[ep_num]["queued"] += 1

    return status_map


def build_episode_card(ep: dict, stats: dict, start_time: float) -> str:
    """Build the progress card text for one episode."""
    episode    = str(ep["episode"])
    total      = stats["total"]
    done       = stats["done"]
    running    = stats["running"]
    queued     = stats["queued"]
    failed     = stats["failed"]
    name       = ep.get("output_name", f"Episode {episode}")

    pct  = (done / total * 100) if total else 0
    bar  = mk_bar(pct, width=15)
    elapsed = int(time.time() - start_time)

    if failed > 0 and done + running + queued == 0:
        status_icon = "❌"
        status_line = f"│ FAILED — {failed}/{total} chunks failed\n"
    elif done == total:
        status_icon = "✅"
        status_line = f"│ All {total} chunks complete\n"
    else:
        status_icon = "⚙️"
        status_line = (
            f"│ ▶ {running} encoding  ⏳ {queued} queued  ✅ {done} done"
            + (f"  ❌ {failed} failed" if failed else "")
            + "\n"
        )

    return (
        f"<code>┌─── {status_icon} [ EP {episode.zfill(2)} — CHUNK ENCODE ] ────────┐\n"
        f"│ {name[:36]}\n"
        f"│                                        \n"
        f"│ {bar} {pct:.0f}%  ({done}/{total} chunks)\n"
        f"│                                        \n"
        f"{status_line}"
        f"│ ⏱ Elapsed: {fmt_duration(elapsed)}\n"
        f"└────────────────────────────────────────┘</code>"
    )


def all_complete(status_map: dict) -> bool:
    """True when every chunk is either done or failed (no queued or running)."""
    for stats in status_map.values():
        if stats["queued"] > 0 or stats["running"] > 0:
            return False
    return True


# ─── Main monitor loop ────────────────────────────────────────────────────────

async def monitor() -> None:
    episode_list_raw = os.environ.get("EPISODE_LIST_JSON", "[]")
    try:
        episode_list = json.loads(episode_list_raw)
    except Exception:
        print("[monitor] ERROR: EPISODE_LIST_JSON is invalid JSON.")
        sys.exit(1)

    if not episode_list:
        print("[monitor] No episodes — nothing to monitor.")
        return

    if not CHANNEL_ID or not BOT_TOKEN:
        print("[monitor] ERROR: TG_CHANNEL_ID or TG_BOT_TOKEN not set.")
        sys.exit(1)

    print(f"[monitor] Starting — {len(episode_list)} episodes, polling every {POLL_INTERVAL}s", flush=True)

    from pyrogram import Client
    app = Client(
        "monitor_bot",
        bot_token=BOT_TOKEN,
        api_id=config.API_ID,
        api_hash=config.API_HASH,
    )
    await app.start()
    print("[monitor] TG connected.", flush=True)

    # ── Post one opening message per episode ──────────────────────────────
    message_ids: dict[str, int] = {}
    start_time = time.time()

    for ep in episode_list:
        episode = str(ep["episode"])
        total   = ep["chunks_for_this_episode"]
        name    = ep.get("output_name", f"Episode {episode}")

        opening = (
            f"<code>┌─── ⚙️ [ EP {episode.zfill(2)} — CHUNK ENCODE ] ────────┐\n"
            f"│ {name[:36]}\n"
            f"│                                        \n"
            f"│ ░░░░░░░░░░░░░░░  0%  (0/{total} chunks)\n"
            f"│                                        \n"
            f"│ ⏳ Waiting for chunks to start…\n"
            f"└────────────────────────────────────────┘</code>"
        )
        mid = await bot_send(app, opening)
        if mid:
            message_ids[episode] = mid
            print(f"[monitor] Episode {episode} → message_id={mid}", flush=True)
        await asyncio.sleep(0.5)   # avoid hitting TG rate limit on post burst

    # ── Save message_ids.json for the finalize job ────────────────────────
    MSG_IDS_FILE.write_text(json.dumps(message_ids, indent=2))
    print(f"[monitor] message_ids.json written: {message_ids}", flush=True)

    # ── Poll loop ─────────────────────────────────────────────────────────
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(POLL_INTERVAL)

            jobs       = await fetch_jobs(session)
            status_map = parse_chunk_jobs(jobs, episode_list)

            # Edit each episode message
            for ep in episode_list:
                episode = str(ep["episode"])
                mid     = message_ids.get(episode, 0)
                if not mid:
                    continue
                stats = status_map.get(episode, {
                    "total": ep["chunks_for_this_episode"],
                    "done": 0, "running": 0, "queued": 0, "failed": 0,
                })
                card = build_episode_card(ep, stats, start_time)
                await bot_edit(app, mid, card)

            # Log summary
            for ep in episode_list:
                s = status_map.get(str(ep["episode"]), {})
                print(
                    f"[monitor] ep={ep['episode']}  "
                    f"done={s.get('done',0)}/{s.get('total',0)}  "
                    f"run={s.get('running',0)}  q={s.get('queued',0)}  "
                    f"fail={s.get('failed',0)}",
                    flush=True,
                )

            if all_complete(status_map):
                print("[monitor] All chunks complete — editing final status and exiting.", flush=True)
                # Final edit: "Merging..."
                for ep in episode_list:
                    episode = str(ep["episode"])
                    mid     = message_ids.get(episode, 0)
                    if not mid:
                        continue
                    stats = status_map.get(episode, {})
                    has_failures = stats.get("failed", 0) > 0

                    if has_failures:
                        final_text = (
                            f"<code>┌─── ⚠️ [ EP {episode.zfill(2)} — PARTIAL FAILURE ] ───┐\n"
                            f"│ {ep.get('output_name', '')[:36]}\n"
                            f"│ {stats.get('done',0)}/{stats.get('total',0)} chunks succeeded\n"
                            f"│ {stats.get('failed',0)} chunk(s) failed — merge aborted\n"
                            f"└────────────────────────────────────────────┘</code>"
                        )
                    else:
                        final_text = (
                            f"<code>┌─── 🔀 [ EP {episode.zfill(2)} — MERGING ] ─────────┐\n"
                            f"│ {ep.get('output_name', '')[:36]}\n"
                            f"│ All {stats.get('total',0)} chunks done — merging…\n"
                            f"└────────────────────────────────────────────┘</code>"
                        )
                    await bot_edit(app, mid, final_text)

                break

    await app.stop()
    print("[monitor] Done.", flush=True)


if __name__ == "__main__":
    asyncio.run(monitor())
