"""
hls_localmux.py <original_m3u8_url> [referer]

Reads the downloaded m3u8 manifest and local segments, builds a patched
local m3u8 that points to:
  - /tmp/hls.key  (already fetched by bash curl)
  - /tmp/hls_segs/seg_NNNNN.ts  (already downloaded by aria2c)

ffmpeg then decrypts + muxes using only local files — no network needed.
"""
import sys, os, re

original_url = sys.argv[1]
seg_dir      = "/tmp/hls_segs"
manifest     = open("/tmp/hls_manifest.txt").read()
lines        = manifest.splitlines()
base_url     = original_url.rsplit("/", 1)[0]

out = []
seg_index = 0

for line in lines:
    if line.startswith("#EXT-X-KEY"):
        # Rewrite URI to point to local key file
        line = re.sub(r'URI="[^"]+"', 'URI="/tmp/hls.key"', line)
        out.append(line)
    elif line and not line.startswith("#"):
        # Rewrite segment URL to local file path
        out.append(f"{seg_dir}/seg_{seg_index:05d}.ts")
        seg_index += 1
    else:
        out.append(line)

with open("/tmp/hls_local.m3u8", "w") as f:
    f.write("\n".join(out))

print(f"✅ Local m3u8 written with {seg_index} segments → /tmp/hls_local.m3u8")
