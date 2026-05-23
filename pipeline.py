"""
Partner Video Upload Pipeline — GitHub Actions Edition
=======================================================
- Reads pipeline_state.json from the results repo (checked out at RESULTS_DIR)
- Fetches video rows from Neon DB in batches
- Downloads each video, skips if > 100 MB
- Renames to pv_{short_id}_{repo_index}_{global_index}.mp4
- Creates GitHub repos: partner_videos6, partner_videos7, ... (100 videos each)
- Clones repo, commits & pushes each video
- Writes updated pipeline_state.json + upload_results.json back to RESULTS_DIR
  (the calling workflow then commits those files to the results repo)

Environment variables (set as GitHub Actions secrets/vars):
    GITHUB_TOKEN      - PAT with repo scope
    GITHUB_USERNAME   - GitHub username / org
    DATABASE_URL      - Neon postgres connection string
    BATCH_LIMIT       - how many videos to process this run (default: 50)
    RESULTS_DIR       - path where results repo is checked out (default: results_repo)
"""

import os
import json
import time
import shutil
import subprocess
import sys
from pathlib import Path

import psycopg2
import requests

# ─────────────────────────────────────────────
#  CONFIG  (all from env — no secrets in code)
# ─────────────────────────────────────────────
GITHUB_TOKEN    = os.environ["GITHUB_TOKEN"]
GITHUB_USERNAME = os.environ["GITHUB_USERNAME"]
DATABASE_URL    = os.environ["DATABASE_URL"]
BATCH_LIMIT     = int(os.environ.get("BATCH_LIMIT", "50"))
RESULTS_DIR     = Path(os.environ.get("RESULTS_DIR", "results_repo"))

REPO_START_INDEX   = 6
VIDEOS_PER_REPO    = 100
DB_BATCH_SIZE      = 10
MAX_FILE_MB        = 100
DOWNLOAD_TIMEOUT   = 120
VIDEO_PREFIX       = "pv_"

WORK_DIR     = Path("pipeline_work")
CLONE_DIR    = WORK_DIR / "clones"
DOWNLOAD_DIR = WORK_DIR / "downloads"

# State & results live in the checked-out results repo
STATE_FILE   = RESULTS_DIR / "pipeline_state.json"
RESULTS_FILE = RESULTS_DIR / "upload_results.json"

GITHUB_API = "https://api.github.com"
GH_HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
}

# ─────────────────────────────────────────────
#  STATE
# ─────────────────────────────────────────────
DEFAULT_STATE = {
    "last_processed_offset": 0,
    "global_index": 0,
    "current_repo_index": REPO_START_INDEX,
    "current_repo_video_count": 0,
    "created_repos": [],
    "uploaded": {},
    "skipped": [],
}

def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            state = json.load(f)
        print(f"[resume] offset={state['last_processed_offset']}  "
              f"global_index={state['global_index']}")
        return state
    print("[state] No existing state — starting fresh.")
    return dict(DEFAULT_STATE)

def save_state(state: dict):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def save_results(state: dict):
    results = [
        {
            "uuid":            uuid,
            "video_src":       info["video_src"],
            "filename":        info["filename"],
            "repo":            info["repo"],
            "github_pages_url": info["github_pages_url"],
        }
        for uuid, info in state["uploaded"].items()
    ]
    with open(RESULTS_FILE, "w") as f:
        json.dump(results, f, indent=2)
    print(f"[results] {len(results)} entries → {RESULTS_FILE}")

# ─────────────────────────────────────────────
#  DATABASE
# ─────────────────────────────────────────────
def fetch_batch(conn, offset: int, limit: int) -> list[dict]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id::text, video_src
        FROM blog_posts
        WHERE video_src LIKE 'https://best%%'
        ORDER BY id
        LIMIT %s OFFSET %s
        """,
        (limit, offset),
    )
    rows = cur.fetchall()
    cur.close()
    return [{"id": r[0], "video_src": r[1]} for r in rows]

# ─────────────────────────────────────────────
#  GITHUB HELPERS
# ─────────────────────────────────────────────
def repo_name(index: int) -> str:
    return f"partner_videos{index}"

def create_github_repo(name: str):
    print(f"[github] Creating repo: {name}")
    r = requests.post(
        f"{GITHUB_API}/user/repos",
        headers=GH_HEADERS,
        json={
            "name": name,
            "private": False,
            "auto_init": True,
            "description": f"Partner videos — {name}",
        },
        timeout=30,
    )
    if r.status_code == 422:
        print(f"[github] Repo {name} already exists.")
    elif r.status_code not in (200, 201):
        raise RuntimeError(f"Failed to create repo {name}: {r.status_code} {r.text}")

    time.sleep(2)
    pages_r = requests.post(
        f"{GITHUB_API}/repos/{GITHUB_USERNAME}/{name}/pages",
        headers=GH_HEADERS,
        json={"source": {"branch": "main", "path": "/"}},
        timeout=30,
    )
    if pages_r.status_code not in (200, 201, 204, 409):
        print(f"[github] Pages setup returned {pages_r.status_code} (non-fatal)")

def clone_url(rname: str) -> str:
    return f"https://{GITHUB_TOKEN}@github.com/{GITHUB_USERNAME}/{rname}.git"

def pages_url(repo: str, filename: str) -> str:
    return f"https://{GITHUB_USERNAME}.github.io/{repo}/{filename}"

def git(args: list[str], cwd: Path):
    result = subprocess.run(
        ["git"] + args, cwd=cwd, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed:\n{result.stderr}")
    return result

def clone_or_open(rname: str) -> Path:
    repo_dir = CLONE_DIR / rname
    if repo_dir.exists():
        git(["pull", "--rebase"], cwd=repo_dir)
    else:
        CLONE_DIR.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["git", "clone", clone_url(rname), str(repo_dir)],
            check=True, capture_output=True, text=True,
        )
    git(["config", "user.email", "pipeline@partner-videos.local"], cwd=repo_dir)
    git(["config", "user.name",  "Partner Video Pipeline"],         cwd=repo_dir)
    return repo_dir

def commit_and_push(repo_dir: Path, filename: str, db_id: str):
    git(["add", filename], cwd=repo_dir)
    git(["commit", "-m", f"Add {filename} (db_id={db_id})"], cwd=repo_dir)
    git(["push"], cwd=repo_dir)
    print(f"[git] Pushed {filename}")

# ─────────────────────────────────────────────
#  DOWNLOAD
# ─────────────────────────────────────────────
def download_video(url: str, dest: Path) -> bool:
    try:
        with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as r:
            r.raise_for_status()
            cl = r.headers.get("Content-Length")
            if cl and int(cl) > MAX_FILE_MB * 1024 * 1024:
                print(f"[skip] {int(cl)//1024//1024}MB > {MAX_FILE_MB}MB: {url}")
                return False
            downloaded = 0
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        downloaded += len(chunk)
                        if downloaded > MAX_FILE_MB * 1024 * 1024:
                            print(f"[skip] Exceeded {MAX_FILE_MB}MB mid-download")
                            dest.unlink(missing_ok=True)
                            return False
                        f.write(chunk)
        return True
    except Exception as e:
        print(f"[error] Download failed: {e}")
        dest.unlink(missing_ok=True)
        return False

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def run():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    CLONE_DIR.mkdir(parents=True, exist_ok=True)

    state = load_state()
    conn  = psycopg2.connect(DATABASE_URL)
    processed_this_run = 0

    try:
        while processed_this_run < BATCH_LIMIT:
            remaining = BATCH_LIMIT - processed_this_run
            fetch_size = min(DB_BATCH_SIZE, remaining)

            offset = state["last_processed_offset"]
            batch  = fetch_batch(conn, offset, fetch_size)

            if not batch:
                print("[done] No more rows in DB.")
                break

            print(f"\n[batch] offset={offset}  fetched={len(batch)}  "
                  f"run_progress={processed_this_run}/{BATCH_LIMIT}")

            for row in batch:
                if processed_this_run >= BATCH_LIMIT:
                    break

                uuid      = row["id"]
                video_src = row["video_src"]

                if uuid in state["uploaded"] or uuid in state["skipped"]:
                    print(f"[skip] Already processed {uuid}")
                    state["last_processed_offset"] += 1
                    continue

                # ── Repo management ──────────────────────────────────
                if state["current_repo_video_count"] >= VIDEOS_PER_REPO:
                    state["current_repo_index"]       += 1
                    state["current_repo_video_count"]  = 0

                rname = repo_name(state["current_repo_index"])

                if rname not in state["created_repos"]:
                    create_github_repo(rname)
                    state["created_repos"].append(rname)
                    save_state(state)

                repo_dir = clone_or_open(rname)

                # ── Filename ─────────────────────────────────────────
                short_id   = uuid.replace("-", "")[:8]
                repo_local = state["current_repo_video_count"]
                global_idx = state["global_index"]
                filename   = f"{VIDEO_PREFIX}{short_id}_{repo_local}_{global_idx}.mp4"
                dest_path  = DOWNLOAD_DIR / filename

                # ── Download ─────────────────────────────────────────
                print(f"[download] {filename} ← {video_src}")
                ok = download_video(video_src, dest_path)

                if not ok:
                    state["skipped"].append(uuid)
                    state["last_processed_offset"] += 1
                    state["global_index"]          += 1
                    save_state(state)
                    processed_this_run += 1
                    continue

                # ── Commit & push ─────────────────────────────────────
                shutil.copy2(dest_path, repo_dir / filename)
                try:
                    commit_and_push(repo_dir, filename, uuid)
                except RuntimeError as e:
                    print(f"[error] Git push failed: {e}")
                    (repo_dir / filename).unlink(missing_ok=True)
                    state["skipped"].append(uuid)
                    state["last_processed_offset"] += 1
                    save_state(state)
                    processed_this_run += 1
                    continue

                # ── Record ────────────────────────────────────────────
                gh_url = pages_url(rname, filename)
                state["uploaded"][uuid] = {
                    "video_src":        video_src,
                    "filename":         filename,
                    "repo":             rname,
                    "github_pages_url": gh_url,
                }
                state["current_repo_video_count"] += 1
                state["global_index"]             += 1
                state["last_processed_offset"]    += 1
                processed_this_run                += 1

                dest_path.unlink(missing_ok=True)

                save_state(state)
                save_results(state)
                print(f"[ok] {filename} → {gh_url}")

    finally:
        conn.close()

    save_results(state)
    print(f"\n[run complete] this_run={processed_this_run}  "
          f"total_uploaded={len(state['uploaded'])}  "
          f"total_skipped={len(state['skipped'])}")


if __name__ == "__main__":
    for pkg in ("psycopg2", "requests"):
        try:
            __import__(pkg)
        except ImportError:
            print(f"Missing: {pkg}  →  pip install {pkg}")
            sys.exit(1)
    run()
