import sys, os, re, subprocess

m3u8_url = sys.argv[1]
referer  = sys.argv[2] if len(sys.argv) > 2 else ""
base_url = m3u8_url.rsplit("/", 1)[0]
seg_dir  = "/tmp/hls_segs"
os.makedirs(seg_dir, exist_ok=True)

def fetch(url):
    cmd = [
        "curl", "-sL", "--fail",
        "-A", "Mozilla/5.0",
        url,
    ]
    if referer:
        cmd += ["-H", f"Referer: {referer}"]
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        raise Exception(f"curl failed ({result.returncode}) for {url}: {result.stderr.decode()}")
    return result.stdout

m3u8  = fetch(m3u8_url).decode()
lines = m3u8.splitlines()
key_bytes, method, iv_default = None, "NONE", None

for line in lines:
    if line.startswith("#EXT-X-KEY"):
        m = re.search(r'METHOD=([^,]+)', line)
        method = m.group(1) if m else "NONE"
        u = re.search(r'URI="([^"]+)"', line)
        if u:
            key_url = u.group(1)
            if not key_url.startswith("http"):
                key_url = base_url + "/" + key_url
            print(f"🔑 Fetching key: {key_url}")
            key_bytes = fetch(key_url)
            print(f"✅ Key fetched ({len(key_bytes)} bytes)")
        iv_m = re.search(r'IV=0x([0-9a-fA-F]+)', line)
        iv_default = bytes.fromhex(iv_m.group(1).zfill(32)) if iv_m else None

segs = [l for l in lines if l and not l.startswith("#")]
segs = [s if s.startswith("http") else base_url + "/" + s for s in segs]
print(f"📋 {len(segs)} segments found")

with open("/tmp/hls_aria2.txt", "w") as f:
    for i, seg in enumerate(segs):
        f.write(f"{seg}\n  dir={seg_dir}\n  out=seg_{i:05d}.ts\n")
        if referer:
            f.write(f"  header=Referer: {referer}\n  header=User-Agent: Mozilla/5.0\n")

if key_bytes:
    open("/tmp/hls.key", "wb").write(key_bytes)
open("/tmp/hls_method.txt", "w").write(method)
if iv_default:
    open("/tmp/hls_iv.bin", "wb").write(iv_default)
open("/tmp/hls_segcount.txt", "w").write(str(len(segs)))
