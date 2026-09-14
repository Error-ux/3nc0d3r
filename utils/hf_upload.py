import os
import sys
import json
import time
import hashlib
from datetime import datetime
from pathlib import Path
from huggingface_hub import HfApi, hf_hub_download

HTML_FRONTEND_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Video Archive - Direct Downloader</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #0b0f19;
            --card-bg: #151c2e;
            --border-color: #232d45;
            --text-main: #f1f5f9;
            --text-dim: #94a3b8;
            --accent: #38bdf8;
            --accent-glow: rgba(56, 189, 248, 0.15);
            --btn-bg: #1e293b;
            --btn-hover: #0284c7;
            --success: #10b981;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            background-color: var(--bg-color);
            color: var(--text-main);
            font-family: 'Inter', sans-serif;
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            padding: 2rem 1rem;
        }
        .container {
            max-width: 1000px;
            margin: 0 auto;
            width: 100%;
        }
        header {
            text-align: center;
            margin-bottom: 2rem;
        }
        h1 {
            font-size: 2rem;
            font-weight: 700;
            color: var(--accent);
            margin-bottom: 0.5rem;
            letter-spacing: -0.5px;
        }
        p.subtitle {
            color: var(--text-dim);
            font-size: 0.95rem;
        }
        .stats-bar {
            display: flex;
            gap: 1.5rem;
            justify-content: center;
            margin: 1.5rem 0;
            flex-wrap: wrap;
        }
        .stat-badge {
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            padding: 0.5rem 1rem;
            border-radius: 9999px;
            font-size: 0.85rem;
            color: var(--text-dim);
        }
        .stat-badge strong {
            color: var(--text-main);
            font-weight: 600;
        }
        .search-box {
            position: relative;
            margin-bottom: 1.5rem;
        }
        .search-input {
            width: 100%;
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 0.85rem 1.25rem 0.85rem 2.75rem;
            font-size: 0.95rem;
            color: var(--text-main);
            outline: none;
            transition: border-color 0.2s;
        }
        .search-input:focus {
            border-color: var(--accent);
            box-shadow: 0 0 0 3px var(--accent-glow);
        }
        .search-icon {
            position: absolute;
            left: 1rem;
            top: 50%;
            transform: translateY(-50%);
            color: var(--text-dim);
        }
        .file-list {
            display: flex;
            flex-direction: column;
            gap: 0.75rem;
        }
        .file-card {
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1.1rem 1.25rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: all 0.2s;
            gap: 1rem;
        }
        .file-card:hover {
            border-color: #3b82f6;
            transform: translateY(-1px);
        }
        .file-info {
            flex: 1;
            min-width: 0;
        }
        .file-name {
            font-weight: 600;
            font-size: 0.95rem;
            margin-bottom: 0.35rem;
            word-break: break-all;
            color: #fff;
        }
        .file-meta {
            font-size: 0.8rem;
            color: var(--text-dim);
            font-family: 'JetBrains Mono', monospace;
            display: flex;
            gap: 1rem;
        }
        .file-actions {
            display: flex;
            gap: 0.5rem;
            align-items: center;
        }
        .btn {
            background: var(--btn-bg);
            border: 1px solid var(--border-color);
            color: var(--text-main);
            padding: 0.6rem 1rem;
            border-radius: 8px;
            font-size: 0.85rem;
            font-weight: 500;
            cursor: pointer;
            text-decoration: none;
            display: inline-flex;
            align-items: center;
            gap: 0.4rem;
            transition: all 0.2s;
            white-space: nowrap;
        }
        .btn:hover {
            background: var(--btn-hover);
            border-color: var(--btn-hover);
            color: #fff;
        }
        .btn-primary {
            background: #0284c7;
            border-color: #0284c7;
            color: #fff;
        }
        .btn-primary:hover {
            background: #0369a1;
        }
        .toast {
            position: fixed;
            bottom: 2rem;
            right: 2rem;
            background: #1e293b;
            border: 1px solid var(--border-color);
            color: #fff;
            padding: 0.75rem 1.25rem;
            border-radius: 8px;
            font-size: 0.85rem;
            display: none;
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.4);
            z-index: 100;
        }
        .empty-state {
            text-align: center;
            padding: 3rem;
            color: var(--text-dim);
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>⚡ Video Archive Library</h1>
            <p class="subtitle">Disguised cloud storage with preserved original naming</p>
            <div class="stats-bar">
                <div class="stat-badge">Total Files: <strong id="total-count">0</strong></div>
                <div class="stat-badge">Total Size: <strong id="total-size">0 MB</strong></div>
            </div>
        </header>

        <div class="search-box">
            <span class="search-icon">🔍</span>
            <input type="text" id="search" class="search-input" placeholder="Search archive by original filename..." oninput="filterFiles()">
        </div>

        <div id="file-list" class="file-list">
            <div class="empty-state">Loading repository database...</div>
        </div>
    </div>

    <div id="toast" class="toast"></div>

    <script>
        let allFiles = [];

        async function loadDatabase() {
            try {
                // Fetch db.json sitting next to index.html
                const res = await fetch('./db.json?t=' + Date.now());
                if (!res.ok) throw new Error("Could not load db.json");
                allFiles = await res.json();
                renderFiles(allFiles);
                updateStats(allFiles);
            } catch (err) {
                document.getElementById('file-list').innerHTML = `
                    <div class="empty-state">
                        <p style="color: #ef4444; margin-bottom: 0.5rem;">⚠️ Failed to load database index</p>
                        <small style="color: var(--text-dim);">${err.message}</small>
                    </div>`;
            }
        }

        function updateStats(files) {
            document.getElementById('total-count').textContent = files.length;
            const totalMb = files.reduce((acc, f) => acc + (f.size_mb || 0), 0);
            document.getElementById('total-size').textContent = totalMb > 1024 
                ? (totalMb / 1024).toFixed(2) + " GB" 
                : totalMb.toFixed(1) + " MB";
        }

        function renderFiles(files) {
            const listEl = document.getElementById('file-list');
            if (!files.length) {
                listEl.innerHTML = '<div class="empty-state">No matching files found.</div>';
                return;
            }

            listEl.innerHTML = files.map(file => {
                const safeName = escapeHtml(file.original_name);
                const safeUrl = escapeHtml(file.direct_url);
                return `
                <div class="file-card">
                    <div class="file-info">
                        <div class="file-name">${safeName}</div>
                        <div class="file-meta">
                            <span>📦 ${file.size_mb ? file.size_mb + ' MB' : 'Unknown size'}</span>
                            <span>📅 ${file.uploaded_at ? file.uploaded_at.split('T')[0] : 'Recent'}</span>
                        </div>
                    </div>
                    <div class="file-actions">
                        <button class="btn btn-primary" onclick="downloadOriginal('${safeUrl}', '${safeName}', this)">
                            ⬇️ Download
                        </button>
                        <button class="btn" title="Copy aria2c command" onclick="copyAria2('${safeUrl}', '${safeName}')">
                            📋 CLI
                        </button>
                    </div>
                </div>`;
            }).join('');
        }

        function filterFiles() {
            const query = document.getElementById('search').value.toLowerCase();
            const filtered = allFiles.filter(f => f.original_name.toLowerCase().includes(query));
            renderFiles(filtered);
        }

        // Force browser download with original name
        async function downloadOriginal(url, originalName, btn) {
            const originalText = btn.innerHTML;
            btn.innerHTML = "⏳ Saving...";
            btn.disabled = true;

            try {
                // Fetch blob and trigger synthetic anchor click with original filename
                const res = await fetch(url);
                if (!res.ok) throw new Error("Download stream error");
                const blob = await res.blob();
                const blobUrl = window.URL.createObjectURL(blob);
                
                const a = document.createElement("a");
                a.style.display = "none";
                a.href = blobUrl;
                a.download = originalName;
                document.body.appendChild(a);
                a.click();
                
                setTimeout(() => {
                    window.URL.revokeObjectURL(blobUrl);
                    a.remove();
                }, 100);

                showToast(`✅ Downloading "${originalName}"`);
            } catch (err) {
                // Fallback: direct trigger with download query param
                const a = document.createElement("a");
                a.href = url + (url.includes('?') ? '&' : '?') + 'download=true';
                a.download = originalName;
                a.target = "_blank";
                document.body.appendChild(a);
                a.click();
                a.remove();
                showToast(`🚀 Started download for "${originalName}"`);
            } finally {
                btn.innerHTML = originalText;
                btn.disabled = false;
            }
        }

        function copyAria2(url, originalName) {
            const cmd = `aria2c -s 16 -x 16 -o "${originalName}" "${url}"`;
            navigator.clipboard.writeText(cmd);
            showToast("📋 aria2c command copied to clipboard!");
        }

        function showToast(msg) {
            const toast = document.getElementById('toast');
            toast.textContent = msg;
            toast.style.display = 'block';
            setTimeout(() => { toast.style.display = 'none'; }, 3000);
        }

        function escapeHtml(str) {
            return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
        }

        loadDatabase();
    </script>
</body>
</html>
"""

def generate_disguised_name(filepath: str) -> tuple[str, str]:
    """
    Generate an obfuscated identifier based on file content sample and size.
    Returns (id_hash, disguised_filename).
    """
    hasher = hashlib.sha256()
    size = os.path.getsize(filepath)
    with open(filepath, "rb") as f:
        hasher.update(f.read(1024 * 1024))  # Sample first 1 MB
    hasher.update(str(size).encode())
    
    file_id = hasher.hexdigest()[:16]
    # Disguise with neutral .dat binary extension
    disguised_filename = f"{file_id}.dat"
    return file_id, disguised_filename


def upload_to_hf(filepath: str, repo_id: str, token: str = None,
                 path_in_repo: str = None, repo_type: str = "dataset") -> dict:
    """
    Uploads a video disguised with a neutral extension and updates the remote
    db.json and index.html frontend so filenames are never mixed up.

    Returns the metadata record for the uploaded video.
    """
    token = token or os.getenv("HF_TOKEN")
    if not token:
        raise ValueError("HF_TOKEN is required for Hugging Face upload.")
    if not repo_id:
        raise ValueError("HF_REPO is required for Hugging Face upload.")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    original_filename = os.path.basename(filepath)
    file_id, disguised_filename = generate_disguised_name(filepath)

    folder = path_in_repo.strip("/") if path_in_repo else ""
    remote_video_path = f"{folder}/{disguised_filename}" if folder else disguised_filename
    db_filename = f"{folder}/db.json" if folder else "db.json"
    html_filename = f"{folder}/index.html" if folder else "index.html"

    api = HfApi(token=token)

    # ── 1. Upload Disguised File ──────────────────────────────────────────────
    print(f"[HF] Disguising '{original_filename}' → '{disguised_filename}'...", flush=True)
    print(f"[HF] Uploading to repo '{repo_id}' at '{remote_video_path}'...", flush=True)

    start_time = time.time()
    video_url = api.upload_file(
        path_or_fileobj=filepath,
        path_in_repo=remote_video_path,
        repo_id=repo_id,
        repo_type=repo_type,
    )
    elapsed = time.time() - start_time
    file_mb = round(os.path.getsize(filepath) / (1024 * 1024), 2)
    speed_mb = file_mb / elapsed if elapsed > 0 else 0
    print(f"[HF] Upload complete: {file_mb:.1f} MB in {elapsed:.1f}s ({speed_mb:.1f} MB/s)", flush=True)

    # Direct download link for the disguised file
    direct_url = f"https://huggingface.co/datasets/{repo_id}/resolve/main/{remote_video_path}"

    # ── 2. Download and Update db.json ────────────────────────────────────────
    print(f"[HF] Updating remote database '{db_filename}'...", flush=True)
    db_records = []
    tmp_db_path = "tmp_hf_db.json"

    try:
        downloaded_path = hf_hub_download(
            repo_id=repo_id,
            filename=db_filename,
            repo_type=repo_type,
            token=token
        )
        with open(downloaded_path, "r", encoding="utf-8") as f:
            db_records = json.load(f)
        if not isinstance(db_records, list):
            db_records = []
    except Exception:
        print("[HF] No existing db.json found. Creating a new database index.", flush=True)
        db_records = []

    # Construct the metadata record
    new_record = {
        "id": file_id,
        "original_name": original_filename,
        "disguised_name": disguised_filename,
        "path": remote_video_path,
        "size_mb": file_mb,
        "direct_url": direct_url,
        "uploaded_at": datetime.utcnow().isoformat() + "Z"
    }

    # Replace existing if ID matches, else prepend to show newest first
    existing_idx = next((i for i, r in enumerate(db_records) if r.get("id") == file_id), None)
    if existing_idx is not None:
        db_records[existing_idx] = new_record
    else:
        db_records.insert(0, new_record)

    with open(tmp_db_path, "w", encoding="utf-8") as f:
        json.dump(db_records, f, indent=2, ensure_ascii=False)

    api.upload_file(
        path_or_fileobj=tmp_db_path,
        path_in_repo=db_filename,
        repo_id=repo_id,
        repo_type=repo_type,
    )
    if os.path.exists(tmp_db_path):
        os.remove(tmp_db_path)
    print(f"[HF] Database updated with record for '{original_filename}'.", flush=True)

    # ── 3. Upload / Update Frontend (index.html) ──────────────────────────────
    tmp_html_path = "tmp_index.html"
    with open(tmp_html_path, "w", encoding="utf-8") as f:
        f.write(HTML_FRONTEND_TEMPLATE)

    try:
        api.upload_file(
            path_or_fileobj=tmp_html_path,
            path_in_repo=html_filename,
            repo_id=repo_id,
            repo_type=repo_type,
        )
        print(f"[HF] Frontend published at: https://huggingface.co/datasets/{repo_id}/raw/main/{html_filename}", flush=True)
    except Exception as e:
        print(f"[HF] Warning: Frontend upload skipped: {e}", flush=True)
    finally:
        if os.path.exists(tmp_html_path):
            os.remove(tmp_html_path)

    return new_record
