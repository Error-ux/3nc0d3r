"""
download.py

Handles all download routing:
  - Telegram file/message links  → tg_handler.py
  - Magnet links                 → disabled (exit 1)
  - M3U8 / HLS streams           → aria2c parallel + openssl decrypt + ffmpeg mux
  - Streaming platforms          → yt-dlp + aria2c
  - Direct CDN / file URLs       → aria2c

Reads from env: VIDEO_URL, CUSTOM
Writes: source.mkv, tg_fname.txt, download.log
"""

import os
import sys
import re
import subprocess
import urllib.parse
from pathlib import Path

URL    = os.environ.get("VIDEO_URL", "").strip()
CUSTOM = os.environ.get("CUSTOM", "").strip()

CDN_REFERER_MAP = {
    "uwucdn.top":        "https://kwik.cx/",
    "kwik.cx":           "https://kwik.cx/",
    "animefever":        "https://animefever.tv/",
    "cache.libria.fun":  "https://www.anilibria.tv/",
    "delivery.animepahe":"https://animepahe.ru/",
    "moon-cdn":          "https://gogoanime.cl/",
    "gogo-cdn":          "https://gogoanime.cl/",
}

STREAMING_PLATFORMS = [
    "bilibili.com", "nicovideo.jp",
    "vimeo.com", "dailymotion.com", "twitch.tv",
]


# ── Helpers ────────────────────────────────────────────────────────────────

def run(cmd, check=True, **kwargs):
    print(f"$ {' '.join(str(c) for c in cmd)}", flush=True)
    return subprocess.run(cmd, check=check, **kwargs)


def resolve_filename(url):
    """Return a .mkv filename for the given URL."""
    if CUSTOM:
        return f"{CUSTOM}.mkv"
    try:
        name = subprocess.check_output(
            ["python3", "resolve_filename.py", url],
            stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        name = urllib.parse.unquote(
            re.sub(r'\?.*', '', Path(url).name)
        )
    if not name.endswith((".mkv", ".mp4", ".webm")):
        name += ".mkv"
    return name


def detect_referer(url):
    for domain, referer in CDN_REFERER_MAP.items():
        if domain in url:
            print(f"🔗 Auto-detected referer: {referer} (matched: {domain})", flush=True)
            return referer
    return ""


def curl_fetch(url, referer="", output=None):
    # Use shell=True so curl behaves identically to bash — avoids header
    # differences that cause 403 on protected key servers
    out_arg  = f"-o {output}" if output else "-o -"
    ref_arg  = f'-H "Referer: {referer}"' if referer else ""
    cmd = f'curl -sL --fail -A "Mozilla/5.0" {ref_arg} {out_arg} "{url}"'
    result = subprocess.run(cmd, shell=True, capture_output=not output)
    if result.returncode != 0:
        raise RuntimeError(f"curl failed (HTTP {result.returncode}) for {url}")
    return result.stdout if not output else None


# ── Download strategies ────────────────────────────────────────────────────

def download_telegram():
    print("📨 Telegram link detected — delegating to tg_handler.py", flush=True)
    run(["python3", "tg_handler.py"])


def download_m3u8(url):
    print("📡 M3U8 detected — aria2c parallel download", flush=True)
    referer  = detect_referer(url)
    base_url = url.rsplit("/", 1)[0]

    # Step 1: Fetch manifest + AES key via curl
    print("📄 Fetching m3u8 manifest...", flush=True)
    manifest = curl_fetch(url, referer=referer).decode()
    Path("/tmp/hls_manifest.txt").write_text(manifest)

    key_uri_match = re.search(r'URI="([^"]+)"', manifest)
    method_match  = re.search(r'METHOD=([^,"\r\n]+)', manifest)
    method = method_match.group(1) if method_match else "NONE"
    Path("/tmp/hls_method.txt").write_text(method)

    if key_uri_match:
        key_uri = key_uri_match.group(1)
        if not key_uri.startswith("http"):
            key_uri = f"{base_url}/{key_uri}"
        print(f"🔑 Fetching key: {key_uri}", flush=True)
        curl_fetch(key_uri, referer=referer, output="/tmp/hls.key")
        key_size = Path("/tmp/hls.key").stat().st_size
        if key_size != 16:
            raise RuntimeError(
                f"AES key must be 16 bytes, got {key_size} bytes — "
                f"server likely returned an error page (check Referer/headers)"
            )
        print(f"✅ Key fetched (16 bytes)", flush=True)

    # Step 2: Parse manifest → aria2c list (no HTTP)
    run(["python3", "hls_parse.py", "/tmp/hls_manifest.txt", base_url, referer])

    seg_count = int(Path("/tmp/hls_segcount.txt").read_text().strip())
    print(f"⬇️ Downloading {seg_count} segments in parallel...", flush=True)

    # Step 3: aria2c parallel download
    run([
        "aria2c",
        "--input-file=/tmp/hls_aria2.txt",
        "--max-concurrent-downloads=16",
        "--split=1",
        "--retry-wait=3",
        "--max-tries=5",
        "--console-log-level=warn",
        "--summary-interval=10",
    ])

    # Step 4: Decrypt with openssl
    seg_dir = Path("/tmp/hls_segs")
    dec_dir = Path("/tmp/hls_dec")
    dec_dir.mkdir(exist_ok=True)

    print(f"🔓 Decrypting {method} segments with openssl...", flush=True)

    if method == "AES-128" and Path("/tmp/hls.key").exists():
        try:
            from Cryptodome.Cipher import AES as _AES
        except ImportError:
            from Crypto.Cipher import AES as _AES

        key       = Path("/tmp/hls.key").read_bytes()
        seq_start = int(Path("/tmp/hls_seq_start.txt").read_text().strip()
                        if Path("/tmp/hls_seq_start.txt").exists() else "0")
        segments  = sorted(seg_dir.glob("seg_*.ts"))

        with open("source.ts", "wb") as out:
            for i, seg in enumerate(segments):
                iv_file = Path(f"/tmp/hls_iv_{i}.hex")
                if iv_file.exists():
                    iv = bytes.fromhex(iv_file.read_text().strip())
                else:
                    iv = (seq_start + i).to_bytes(16, "big")

                data = seg.read_bytes()
                dec  = _AES.new(key, _AES.MODE_CBC, iv).decrypt(data)

                # Strip PKCS7 padding only on last segment
                if i == len(segments) - 1:
                    pad = dec[-1]
                    if 1 <= pad <= 16:
                        dec = dec[:-pad]

                out.write(dec)

        print(f"✅ Decrypted {len(segments)} segments", flush=True)
    else:
        with open("source.ts", "wb") as out:
            for f in sorted(seg_dir.glob("seg_*.ts")):
                out.write(f.read_bytes())
        print("✅ Merged (no encryption)", flush=True)

    # Step 5: Mux to MKV
    print("📦 Muxing to MKV...", flush=True)
    run(["ffmpeg", "-i", "source.ts", "-c", "copy", "source.mkv", "-y"])
    Path("source.ts").unlink(missing_ok=True)


def download_streaming(url):
    print("📡 Streaming platform detected — using yt-dlp", flush=True)
    run([
        "yt-dlp",
        "--add-header", "User-Agent:Mozilla/5.0",
        "--downloader", "aria2c",
        "--downloader-args",
        "aria2c:-x 16 -s 16 -k 1M --console-log-level=warn "
        "--summary-interval=10 --retry-wait=5 --max-tries=10",
        "--merge-output-format", "mkv",
        "-o", "source.mkv",
        url,
    ])


def download_direct(url):
    print("📥 Direct CDN download — resolving URL...", flush=True)
    result = subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-w", "%{url_effective}", "-L",
         "-A", "Mozilla/5.0", url],
        capture_output=True
    )
    final_url = result.stdout.decode().strip() or url
    print(f"✅ Resolved: {final_url}", flush=True)
    run([
        "aria2c", "-x", "16", "-s", "16", "-k", "1M",
        "--user-agent=Mozilla/5.0",
        "--console-log-level=warn",
        "--summary-interval=10",
        "--retry-wait=5",
        "--max-tries=10",
        "-o", "source.mkv",
        final_url,
    ])


# ── Main router ───────────────────────────────────────────────────────────

def main():
    if not URL:
        print("❌ VIDEO_URL is not set", file=sys.stderr)
        sys.exit(1)

    fn = resolve_filename(URL)

    if URL.startswith("tg_file:") or URL.startswith("https://t.me/"):
        download_telegram()

    elif URL.startswith("magnet:"):
        print("❌ ERROR: Magnet links are disabled.")
        sys.exit(1)

    elif "m3u8" in URL:
        download_m3u8(URL)

    elif any(p in URL for p in STREAMING_PLATFORMS):
        download_streaming(URL)

    else:
        download_direct(URL)

    Path("tg_fname.txt").write_text(fn)
    print(f"✅ Done → {fn}", flush=True)


if __name__ == "__main__":
    main()
