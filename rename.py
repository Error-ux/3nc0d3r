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

def parse_from_filename(raw_filename: str) -> dict | None:
    """
    Run anitopy on *raw_filename* and return a structured dict:
        {
            "anime_name": str,
            "season":     int,
            "episode":    int,
            "is_special": bool,
        }
    Returns None if anitopy can't extract a title.

    Handles:
      - Standard episodes:  [SubsPlease] Medalist - 07 (1080p).mkv
      - Seasonal:           Shingeki no Kyojin S3 - 12 [720p].mkv
      - Specials/OVA:       Oshi no Ko - 01 OVA [BDRip].mkv
      - S01E04 format:      [Ember] Dungeon Meshi - S01E04 [1080p].mkv
      - URL-decoded CDN:    Imouto Sae Ireba Ii. - 12.mkv
      - Greek suffixes:     Steins;Gate 0 - 23β.mkv  (β kept intact)
    """
    # Pre-detect [SXX-EXX] / [SXX-SPXX] prefix — our own encoder output format.
    # anitopy treats "[S01-E03]" as a release group and strips it, losing the
    # episode. Capture season/episode here so we can override anitopy below.
    _pre_m = re.match(r'^\[S(\d{1,2})[-–](SP?(\d{1,3})|E(\d{1,3}))\]\s*', raw_filename, re.IGNORECASE)
    _pre_season  = int(_pre_m.group(1))                       if _pre_m else None
    _pre_episode = int(_pre_m.group(3) or _pre_m.group(4))   if _pre_m else None
    _pre_special = _pre_m.group(2).upper().startswith("SP")   if _pre_m else False

    try:
        import anitopy
        p = anitopy.parse(raw_filename)
    except Exception as e:
        print(f"[rename] anitopy failed on {raw_filename!r}: {e}")
        return None

    anime_name = p.get("anime_title", "").strip()
    if not anime_name:
        return None

    # Season — default 1 if not present
    raw_season = p.get("anime_season", "1") or "1"
    try:
        season = int(str(raw_season).strip())
    except ValueError:
        season = 1

    # anitopy often includes a trailing season number in the title instead of
    # parsing it as anime_season. e.g. "Hibike! Euphonium 3" → season=3, title="Hibike! Euphonium"
    # Only do this when anitopy itself didn't detect a season (season == 1 from default).
    if season == 1:
        # Case 1: trailing number — "Hibike! Euphonium 3"
        m = re.match(r'^(.+?)\s+(\d{1,2})$', anime_name)
        if m:
            candidate_season = int(m.group(2))
            # Sanity check: season 2–9 is plausible, but "Evangelion 1.11" should stay as-is
            if 2 <= candidate_season <= 9:
                anime_name = m.group(1).strip()
                season     = candidate_season
        else:
            # Case 2: mid-title number before subtitle — "Hibike! Euphonium 3 - Making Episode"
            # Preserve subtitle: strip the season digit and bare "Episode" keyword.
            m = re.match(r'^(.+?)\s+(\d{1,2})\s*[-\u2013]\s*(.+)$', anime_name)
            if m:
                candidate_season = int(m.group(2))
                if 2 <= candidate_season <= 9:
                    subtitle   = re.sub(r'\bEpisode\b', '', m.group(3), flags=re.IGNORECASE).strip()
                    anime_name = f"{m.group(1).strip()} - {subtitle}".strip(" -").strip()
                    season     = candidate_season

    # Episode — default 1 if not present
    raw_ep = p.get("episode_number", "1") or "1"
    # anitopy may return "23β" or "01-12" — take leading digits
    ep_digits = re.match(r"\d+", str(raw_ep).strip())
    episode = int(ep_digits.group()) if ep_digits else 1

    # Special flag: OVA / ONA / SP / Special in episode_type or anime_type
    special_keywords = {"ova", "ona", "sp", "special", "movie"}
    ep_type    = str(p.get("episode_type",  "") or "").lower()
    anime_type = str(p.get("anime_type",    "") or "").lower()
    is_special = bool(special_keywords & {ep_type, anime_type})

    # Fallback: anitopy misses "SP03" and "[Judas]-style" "- S03" specials.
    # An explicit SP prefix always wins; "- S\d+" is a special only when a
    # season was already detected (so S03 isn't mistaken for a lone season tag).
    if not is_special:
        # Explicit SP prefix: SP03, SP3, [SP03], etc.
        sp_m = re.search(r'\bSP(\d{1,3})\b', raw_filename, re.IGNORECASE)
        if sp_m:
            is_special = True
            episode    = int(sp_m.group(1))
        # "- S03" / " S03" style when a separate season (S1/S2…) is already known
        elif season > 0:
            s_m = re.search(r'[-\s]S(\d{2,3})(?=\s|$|\[|\.)', raw_filename)
            # Only treat as special if this number doesn't match the already-parsed season
            if s_m and int(s_m.group(1)) != season:
                is_special = True
                episode    = int(s_m.group(1))

    # Override with pre-detected [SXX-EXX] values — anitopy missed them.
    if _pre_season  is not None: season     = _pre_season
    if _pre_episode is not None: episode    = _pre_episode
    if _pre_m:                   is_special = _pre_special

    print(
        f"[rename] anitopy → {anime_name!r}  "
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
