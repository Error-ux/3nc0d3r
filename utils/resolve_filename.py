import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))  # repo root

"""
resolve_filename.py
Given a URL as argv[1], print the best human-readable filename.

Priority:
  1. filename= / file= query param
  2. Content-Disposition header via curl -sL (follows redirects)
  3. URL path segment fallback
"""
import sys
import re
import subprocess
import urllib.parse

url = sys.argv[1]

# 1. Query param: ?filename= or ?file=
qs = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
fn = (qs.get("filename") or qs.get("file") or [None])[0]
if fn:
    print(urllib.parse.unquote(fn))
    sys.exit()

# 2. Content-Disposition via curl (follows redirects, same as manual test)
try:
    result = subprocess.run(
        ["curl", "-sL", "-D", "-", "-o", "/dev/null",
         "--max-time", "10", "--user-agent", "Mozilla/5.0", url],
        capture_output=True, text=True, timeout=15
    )
    headers = result.stdout

    # filename*=UTF-8''Foo%20Bar.mkv  (RFC 5987)
    m = re.search(r"filename\*=UTF-8''([^\r\n;\"]+)", headers, re.IGNORECASE)
    if m:
        print(urllib.parse.unquote(m.group(1).strip()))
        sys.exit()

    # filename="Foo%20Bar.mkv" or filename=Foo%20Bar.mkv
    m = re.search(r'filename="?([^"\r\n;]+)"?', headers, re.IGNORECASE)
    if m:
        print(urllib.parse.unquote(m.group(1).strip()))
        sys.exit()

except Exception:
    pass

# 3. URL path segment fallback
print(urllib.parse.unquote(urllib.parse.urlparse(url).path.split("/")[-1]))
