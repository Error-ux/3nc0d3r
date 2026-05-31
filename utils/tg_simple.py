"""
utils/tg_simple.py
Lightweight Telegram notifications via raw HTTP (no Pyrogram).

Used by downloaders that run before the main Telegram session is established.
These helpers read TG_BOT_TOKEN / TG_CHAT_ID from the environment and send
messages using plain curl — zero Pyrogram dependency.

Exports:
    tg_api(endpoint, payload)  → dict | None
    tg_send(text)              → message_id | None
    tg_edit(msg_id, text)      → None
    BOT_TOKEN, CHAT_ID, RUN_NUMBER  (env-sourced constants)
"""

import json
import os
import subprocess
import config

BOT_TOKEN  = config.BOT_TOKEN
CHAT_ID    = str(config.CHAT_ID)
RUN_NUMBER = os.environ.get("GITHUB_RUN_NUMBER",  "?")


def tg_api(endpoint: str, payload: dict) -> dict | None:
    """POST to the Bot API. Returns parsed JSON or None on any error."""
    if not BOT_TOKEN or not CHAT_ID:
        return None
    try:
        data   = json.dumps(payload).encode()
        result = subprocess.run(
            [
                "curl", "-s", "-X", "POST",
                f"https://api.telegram.org/bot{BOT_TOKEN}/{endpoint}",
                "-H", "Content-Type: application/json",
                "-d", data.decode(),
            ],
            check=False, timeout=10, capture_output=True,
        )
        return json.loads(result.stdout.decode()) if result.stdout else None
    except Exception:
        return None


def tg_send(text: str) -> int | None:
    """Send a new message. Returns message_id or None."""
    resp = tg_api("sendMessage", {
        "chat_id":    CHAT_ID,
        "text":       text,
        "parse_mode": "HTML",
    })
    try:
        return resp["result"]["message_id"]
    except Exception:
        return None


def tg_edit(msg_id: int | None, text: str) -> None:
    """Edit an existing message in-place. No-op if msg_id is falsy."""
    if not msg_id:
        return
    tg_api("editMessageText", {
        "chat_id":    CHAT_ID,
        "message_id": msg_id,
        "text":       text,
        "parse_mode": "HTML",
    })


def notify_private(text: str):
    """Sends a notification to the private/notify chats configured in config.NOTIFY_CHATS."""
    import config
    if not config.BOT_TOKEN or not getattr(config, "NOTIFY_CHATS", None):
        return
    for chat in config.NOTIFY_CHATS:
        try:
            import json
            import subprocess
            payload = json.dumps({
                "chat_id": chat,
                "text": text,
                "parse_mode": "HTML"
            })
            subprocess.run(
                [
                    "curl", "-s", "-X", "POST",
                    f"https://api.telegram.org/bot{config.BOT_TOKEN}/sendMessage",
                    "-H", "Content-Type: application/json",
                    "-d", payload,
                ],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass

