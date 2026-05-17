# 3nc0d3r

GitHub Actions–based AV1 encode pipeline. Downloads a video, encodes it with SVT-AV1, scores it with VMAF, and uploads to Telegram + Gofile.

---

## Project layout

```
3nc0d3r/
├── config.py             Central config — reads all env vars (Telegram, encode settings, etc.)
├── requirements.txt
│
├── main.py               Entry point — encode phase (SVT-AV1 via ffmpeg)
├── download.py           Entry point — download router (dispatches to the right downloader)
├── upload.py             Entry point — upload phase (VMAF → Gofile → Telegram)
├── tg_rename.py          Entry point — rename-only workflow (no re-encode)
├── notify_failure.py     Entry point — sends a Telegram failure report from the workflow
│
├── downloaders/          Source acquisition modules
│   ├── anibd.py          anibd.app HLS downloader (pipeline + interactive CLI)
│   ├── iwara.py          iwara.tv/iwara.ai downloader (pipeline + CLI)
│   └── tg_handler.py     Telegram file/link downloader (via Pyrogram)
│
└── utils/                Shared helpers
    ├── media.py          ffprobe metadata, crop detection, VMAF/SSIM, cloud upload
    ├── rename.py         Structured filename builder + anitopy parser
    ├── resolve_filename.py  URL → human-readable filename (called as a subprocess)
    ├── tg_simple.py      Lightweight Telegram HTTP notifications (no Pyrogram)
    ├── tg_utils.py       Pyrogram session management + safe message editing
    └── ui.py             Terminal progress bars + Telegram UI string builders
```

---

## Pipeline flow

```
download.py  →  main.py  →  upload.py
```

1. **download.py** routes the URL to the correct downloader, writes `source.mkv` + `tg_fname.txt`.
2. **main.py** encodes `source.mkv` → `<output>.mkv` via SVT-AV1, sends live progress to Telegram.
3. **upload.py** remuxes, runs VMAF, uploads to Gofile, then sends the file to Telegram.

The rename workflow (`tg_rename.py`) is independent — it downloads, probes, renames, and re-uploads without re-encoding.

---

## Environment variables

See `config.py` for the full list. Key ones:

| Variable | Description |
|---|---|
| `API_ID` / `API_HASH` / `BOT_TOKEN` | Telegram MTProto + bot credentials |
| `CHAT_ID` | Telegram chat to post results to |
| `VIDEO_URL` | Source URL (Telegram link, HLS, direct CDN, anibd.app, iwara.tv) |
| `ANIME_NAME` | Enable structured rename (e.g. `Medalist`) |
| `SEASON` / `EPISODE` | Season/episode numbers for the rename |
| `USER_CRF` / `USER_PRESET` | SVT-AV1 quality settings |
| `USER_GRAIN` | Film grain synthesis (0–50) |
| `DEMO_DURATION` | Encode only N seconds (testing) |
