import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))  # repo root

"""
rename.py — Anime file renaming module
Generates structured filenames like:
    [S02-E07] Medalist [1080p] [Dual].mkv

Also provides rich track info for the final Telegram report.
"""

import json
import re
import subprocess


# ---------------------------------------------------------------------------
# TRACK EXTRACTION
# ---------------------------------------------------------------------------

def get_track_info(source: str) -> tuple[list[dict], list[dict]]:
    """
    Run ffprobe on *source* and return (audio_tracks, sub_tracks).

    Each audio track dict:
        index, lang, title, codec, channels, layout

    Each subtitle track dict:
        index, lang, title, codec, forced, default
    """
    cmd = [
        "ffprobe", "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        source
    ]
    try:
        raw = subprocess.check_output(cmd, stderr=subprocess.DEVNULL).decode()
        data = json.loads(raw)
    except Exception as e:
        print(f"[rename] ffprobe failed: {e}")
        return [], []

    audio_tracks: list[dict] = []
    sub_tracks:   list[dict] = []

    for stream in data.get("streams", []):
        codec_type = stream.get("codec_type", "")
        tags       = stream.get("tags", {})

        # Normalise tag keys — ffprobe can return uppercase OR lowercase
        tag_lower  = {k.lower(): v for k, v in tags.items()}
        lang       = tag_lower.get("language", "und")
        title      = tag_lower.get("title", "")

        if codec_type == "audio":
            channels = int(stream.get("channels", 0))
            layout   = stream.get("channel_layout") or f"{channels}ch"
            audio_tracks.append({
                "index":    stream.get("index", len(audio_tracks)),
                "lang":     lang,
                "title":    title,
                "codec":    stream.get("codec_name", "unknown"),
                "channels": channels,
                "layout":   layout,
            })

        elif codec_type == "subtitle":
            disposition = stream.get("disposition", {})
            sub_tracks.append({
                "index":   stream.get("index", len(sub_tracks)),
                "lang":    lang,
                "title":   title,
                "codec":   stream.get("codec_name", "unknown"),
                "forced":  bool(disposition.get("forced", 0)),
                "default": bool(disposition.get("default", 0)),
            })

    return audio_tracks, sub_tracks


# ---------------------------------------------------------------------------
# AUDIO TYPE DETECTION
# ---------------------------------------------------------------------------

def detect_audio_type(
    audio_tracks: list[dict],
    sub_tracks:   list[dict] | None = None,
) -> str:
    """
    Classify the release audio type from track lists.

    Rules
    -----
    1 audio + subs present          →  Sub
    1 audio + no subs + jpn audio   →  Raw   (original, no translation)
    1 audio + no subs + non-jpn     →  Dub   (dubbed only, no subs)
    2 audio tracks                  →  Dual
    3 audio tracks                  →  Tri
    4+ audio tracks                 →  Multi
    """
    count     = len(audio_tracks)
    has_subs  = bool(sub_tracks)

    if count <= 1:
        if has_subs:
            return "Sub"
        # No subs — check audio language
        lang = (audio_tracks[0].get("lang", "und") if audio_tracks else "und").lower()
        if lang in ("jpn", "und", ""):
            return "Raw"
        return "Dub"

    if count == 2:
        return "Dual"
    if count == 3:
        return "Tri"
    return "Multi"


# ---------------------------------------------------------------------------
# QUALITY LABEL
# ---------------------------------------------------------------------------

def detect_quality(height: int) -> str:
    """Map video pixel height to a human-readable quality tag."""
    if height >= 2100:
        return "4K"
    if height >= 1060:
        return "1080p"
    if height >= 700:
        return "720p"
    if height >= 460:
        return "480p"
    return "360p"


# ---------------------------------------------------------------------------
# FILENAME BUILDER
# ---------------------------------------------------------------------------

def build_output_name(
    anime_name:   str,
    season:       int | str,
    episode:      int | str,
    quality:      str,
    audio_type:   str,
    content_type: str = "Anime",
    ext:          str = "mkv",
    is_special:   bool = False,
) -> str:
    """
    Assemble the final filename.

    Normal:   [S02-E07] Anime Name [1080p] [Dual].mkv
    Special:  [S01-SP03] Anime Name [1080p] [Sub].mkv
    """
    safe_name   = re.sub(r'[<>:"/\\|?*\n\r\t]', "", anime_name).strip()
    season_str  = f"S{int(season):02d}"
    ep_prefix   = "SP" if is_special else "E"
    episode_str = f"{ep_prefix}{int(episode):02d}"

    return f"[{season_str}-{episode_str}] {safe_name} [{quality}] [{audio_type}].{ext}"


# ---------------------------------------------------------------------------
# ANITOPY FILENAME PARSER
# ---------------------------------------------------------------------------

def _load_rename_rules() -> list[dict]:
    """Load regex rules from rename_rules.json sitting next to this file."""
    rules_path = Path(__file__).parent / "rename_rules.json"
    if not rules_path.exists():
        return []
    try:
        with open(rules_path, "r", encoding="utf-8") as f:
            return json.load(f).get("rules", [])
    except Exception:
        return []


def _clean_title(title: str) -> str:
    """Strip release-group brackets, leftover SxxExx markers, and trailing punctuation."""
    if not title:
        return ""
    # Strip ALL leading bracket groups (e.g. "[Group][Tag] Title" → "Title")
    while re.match(r'^\[.*?\]', title):
        title = re.sub(r'^\[.*?\]\s*', '', title).strip()
    # Strip trailing bracket group
    title = re.sub(r'\s*\[.*?\]$', '', title).strip()
    # Remove episode markers anitopy may have left in the title
    title = re.sub(r'(?i)[- ]?S\d+E\d+', '', title)
    title = re.sub(r'(?i)[- ]?E\d+$', '', title)
    return title.strip(" -._")


def parse_from_filename(raw_filename: str) -> dict | None:
    """
    Parse *raw_filename* and return a structured dict:
        {
            "anime_name": str,
            "season":     int,
            "episode":    int,
            "is_special": bool,
        }
    Returns None if no title can be extracted.

    Strategy (priority high → low):
      1. Body rules  — explicit SxxExx / SP patterns found mid-filename
      2. Anitopy     — general anime filename parser
      3. Our prefix  — [SXX-EXX] / [SXX-SPXX] we emitted ourselves (fallback only)
      4. Defaults    — season=1, episode=1
    """
    rules = _load_rename_rules()

    # ── 1. Anitopy ────────────────────────────────────────────────────────────
    try:
        import anitopy
        p = anitopy.parse(raw_filename)
    except Exception as e:
        print(f"[rename] anitopy failed on {raw_filename!r}: {e}")
        return None

    anime_name = _clean_title(p.get("anime_title", "").strip())
    if not anime_name:
        return None

    # ── 2. JSON rule matching ─────────────────────────────────────────────────
    meta = {"season": None, "episode": None, "is_special": False}
    prefix_meta = None
    body_found  = False

    for rule in rules:
        m = re.search(rule["pattern"], raw_filename, re.IGNORECASE)
        if not m:
            continue
        g = m.groupdict()
        if rule["name"] == "PREFIX":
            prefix_meta = {
                "s":  int(g.get("season",  1)),
                "e":  int(g.get("episode", 1)),
                "sp": g.get("type", "").upper() == "SP",
            }
        else:
            body_found = True
            if g.get("season"):  meta["season"]     = int(g["season"])
            if g.get("episode"): meta["episode"]    = int(g["episode"])
            if "SP" in rule["name"]: meta["is_special"] = True

    # ── 3. Merge: Body > Anitopy > Prefix > Default ───────────────────────────
    # Safe int helpers — anitopy can return "2nd", "III", "23β", etc.
    def _safe(val, fallback=0):
        m = re.match(r'\d+', str(val).strip()) if val else None
        return int(m.group()) if m else fallback

    anitopy_season  = _safe(p.get("anime_season"),  0)
    anitopy_episode = _safe(p.get("episode_number"), 0)

    season = (
        meta["season"]
        or anitopy_season
        or (prefix_meta["s"] if prefix_meta else 0)
        or 1
    )
    episode = (
        meta["episode"]
        or anitopy_episode
        or (prefix_meta["e"] if prefix_meta else 0)
        or 1
    )
    is_special = (
        meta["is_special"]
        or p.get("episode_type") in ("OVA", "Special")
        or (prefix_meta["sp"] if prefix_meta else False)
    )

    # ── 4. Trailing-digit season extraction ───────────────────────────────────
    # "Hibike! Euphonium 3" → season=3, title="Hibike! Euphonium"
    # Only when anitopy didn't already detect a season.
    if season == 1:
        tm = re.search(r'^(.+?)\s+([2-9])$', anime_name)
        if tm:
            anime_name = tm.group(1).strip()
            season     = int(tm.group(2))

    print(
        f"[rename] parsed → {anime_name!r}  "
        f"S{season:02d}{'SP' if is_special else 'E'}{episode:02d}"
    )
    return {
        "anime_name": anime_name,
        "season":     season,
        "episode":    episode,
        "is_special": is_special,
    }


# ---------------------------------------------------------------------------
# RICH TRACK REPORT (for Telegram final message)
# ---------------------------------------------------------------------------

def format_track_report(audio_tracks: list[dict], sub_tracks: list[dict]) -> str:
    """
    Return an HTML-formatted block listing every audio and subtitle track.
    Designed to be appended directly to the existing Telegram report string.
    """
    lines: list[str] = []

    # ── Audio ──────────────────────────────────────────────────────────────
    lines.append("🔊 <b>AUDIO TRACKS:</b>")
    if audio_tracks:
        for i, t in enumerate(audio_tracks, 1):
            label   = t["title"] if t["title"] else t["lang"].upper()
            codec   = t["codec"].upper()
            layout  = t["layout"]
            lines.append(f"  └ [{i}] {label} | {codec} | {layout}")
    else:
        lines.append("  └ No audio tracks detected")

    lines.append("")

    # ── Subtitles ──────────────────────────────────────────────────────────
    PGS_CODECS = {"hdmv_pgs_bitmap", "pgssub"}
    lines.append("💬 <b>SUBTITLE TRACKS:</b>")
    if sub_tracks:
        for i, t in enumerate(sub_tracks, 1):
            label  = t["title"] if t["title"] else t["lang"].upper()
            codec  = t["codec"].upper()
            is_pgs = t.get("codec", "").lower() in PGS_CODECS
            flags: list[str] = []
            if t["default"]:
                flags.append("Default")
            if t["forced"]:
                flags.append("Forced")
            if is_pgs:
                flags.append("⛔ Removed")
            flag_str = f" ({', '.join(flags)})" if flags else ""
            lines.append(f"  └ [{i}] {label} | {codec}{flag_str}")
    else:
        lines.append("  └ No subtitle tracks detected")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CONVENIENCE: full rename pipeline
# ---------------------------------------------------------------------------

def resolve_output_name(
    source:               str,
    anime_name:           str,
    season:               int | str,
    episode:              int | str,
    height:               int,
    ext:                  str = "mkv",
    audio_type_override:  str = "Auto",
    content_type:         str = "Anime",
    is_special:           bool = False,
) -> tuple[str, str, list[dict], list[dict]]:
    """
    One-call helper used by main.py.

    Returns (output_filename, audio_type, audio_tracks, sub_tracks)
    """
    audio_tracks, sub_tracks = get_track_info(source)

    if audio_type_override and audio_type_override.strip().lower() != "auto":
        audio_type = audio_type_override.strip().capitalize()
    else:
        audio_type = detect_audio_type(audio_tracks, sub_tracks)

    quality  = detect_quality(height)
    filename = build_output_name(
        anime_name, season, episode, quality, audio_type,
        content_type, ext, is_special=is_special,
    )
    return filename, audio_type, audio_tracks, sub_tracks


# ---------------------------------------------------------------------------
# LANGUAGE CODE → HUMAN-READABLE NAME
# ---------------------------------------------------------------------------

# ISO 639-2/B codes (the ones ffprobe reports) mapped to English display names.
# Covers the vast majority of subtitle languages seen in anime/media releases.
_LANG_MAP: dict[str, str] = {
    "afr": "Afrikaans",  "alb": "Albanian",   "amh": "Amharic",
    "ara": "Arabic",     "arm": "Armenian",   "aze": "Azerbaijani",
    "baq": "Basque",     "bel": "Belarusian", "ben": "Bengali",
    "bos": "Bosnian",    "bul": "Bulgarian",  "bur": "Burmese",
    "cat": "Catalan",    "chi": "Chinese",    "zho": "Chinese",
    "hrv": "Croatian",   "cze": "Czech",      "ces": "Czech",
    "dan": "Danish",     "dut": "Dutch",      "nld": "Dutch",
    "eng": "English",    "est": "Estonian",   "fin": "Finnish",
    "fre": "French",     "fra": "French",     "geo": "Georgian",
    "kat": "Georgian",   "ger": "German",     "deu": "German",
    "gre": "Greek",      "ell": "Greek",      "guj": "Gujarati",
    "heb": "Hebrew",     "hin": "Hindi",      "hun": "Hungarian",
    "ice": "Icelandic",  "isl": "Icelandic",  "ind": "Indonesian",
    "ita": "Italian",    "jpn": "Japanese",   "kan": "Kannada",
    "kaz": "Kazakh",     "khm": "Khmer",      "kor": "Korean",
    "kur": "Kurdish",    "lav": "Latvian",    "lit": "Lithuanian",
    "mac": "Macedonian", "mkd": "Macedonian", "mal": "Malayalam",
    "mlt": "Maltese",    "mar": "Marathi",    "may": "Malay",
    "msa": "Malay",      "mon": "Mongolian",  "nep": "Nepali",
    "nor": "Norwegian",  "pan": "Punjabi",    "per": "Persian",
    "fas": "Persian",    "pol": "Polish",     "por": "Portuguese",
    "rum": "Romanian",   "ron": "Romanian",   "rus": "Russian",
    "srp": "Serbian",    "sin": "Sinhala",    "slo": "Slovak",
    "slk": "Slovak",     "slv": "Slovenian",  "spa": "Spanish",
    "swa": "Swahili",    "swe": "Swedish",    "tam": "Tamil",
    "tel": "Telugu",     "tha": "Thai",       "tur": "Turkish",
    "ukr": "Ukrainian",  "urd": "Urdu",       "uzb": "Uzbek",
    "vie": "Vietnamese", "wel": "Welsh",      "cym": "Welsh",
    "yid": "Yiddish",    "zul": "Zulu",
}


def lang_code_to_name(code: str) -> str:
    """
    Convert an ISO 639-2 language code (e.g. 'jpn', 'eng') to its
    English display name (e.g. 'Japanese', 'English').
    Falls back to the uppercased code if not found in the table.
    """
    if not code or code.lower() in ("und", ""):
        return "Unknown"
    return _LANG_MAP.get(code.lower(), code.upper())
