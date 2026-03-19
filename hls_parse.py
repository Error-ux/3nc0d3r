"""
hls_parse.py <manifest_file> <base_url> [referer]

Reads an already-downloaded m3u8 manifest, builds an aria2c input file
for parallel segment downloading. All HTTP is handled by bash curl upstream.
"""
import sys, os, re

manifest_file = sys.argv[1]
base_url      = sys.argv[2]
referer       = sys.argv[3] if len(sys.argv) > 3 else ""
seg_dir       = "/tmp/hls_segs"
os.makedirs(seg_dir, exist_ok=True)

lines = open(manifest_file).read().splitlines()

# Parse IV if present in the key line
for line in lines:
    if line.startswith("#EXT-X-KEY"):
        iv_m = re.search(r'IV=0x([0-9a-fA-F]+)', line)
        if iv_m:
            open("/tmp/hls_iv.bin", "wb").write(
                bytes.fromhex(iv_m.group(1).zfill(32))
            )

# Collect segment URLs
segs = [l for l in lines if l and not l.startswith("#")]
segs = [s if s.startswith("http") else base_url + "/" + s for s in segs]
print(f"📋 {len(segs)} segments found")

# Write aria2c input file with per-segment headers
with open("/tmp/hls_aria2.txt", "w") as f:
    for i, seg in enumerate(segs):
        f.write(f"{seg}\n  dir={seg_dir}\n  out=seg_{i:05d}.ts\n")
        f.write(f"  header=User-Agent: Mozilla/5.0\n")
        if referer:
            f.write(f"  header=Referer: {referer}\n")

open("/tmp/hls_segcount.txt", "w").write(str(len(segs)))
