"""
pipeline.py — partner video upload pipeline (crash-safe v2)

Fixes vs v1:
  1. Authenticated remote URL set before every push — fixes silent git push failures
  2. Proper logging with timestamps, repo slot, offset, and git commit status
  3. Skipped videos also committed to state immediately
  4. Download size logged so we know why files are skipped
  5. Temp file cleanup in finally block
"""

import os
import json
import time
import shutil
import subprocess
from pathlib import Path

import psycopg2
import requests

# ── config ────────────────────────────────────────────────────────────────────

GITHUB_TOKEN      = os.environ["GITHUB_TOKEN"]
GITHUB_USERNAME   = os.environ["GITHUB_USERNAME"]
DATABASE_URL      = os.environ["DATABASE_URL"]

RESULTS_REPO_NAME = "partner_videos_results"
RESULTS_DIR       = Path(os.environ.get("RESULTS_DIR", "results_repo"))

REPO_START_INDEX    = 6
VIDEOS_PER_REPO     = 100
DB_BATCH_SIZE       = 10

MAX_FILE_MB         = 100
DOWNLOAD_TIMEOUT    = 120

VIDEO_PREFIX = "pv_"

WORK_DIR     = Path("pipeline_work")
CLONE_DIR    = WORK_DIR / "clones"
DOWNLOAD_DIR = WORK_DIR / "downloads"

STATE_FILE   = RESULTS_DIR / "pipeline_state.json"
RESULTS_FILE = RESULTS_DIR / "upload_results.json"

GITHUB_API = "https://api.github.com"

GH_HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept":        "application/vnd.github+json",
}

DEFAULT_STATE = {
    "last_processed_offset":    0,
    "global_index":             0,
    "current_repo_index":       REPO_START_INDEX,
    "current_repo_video_count": 0,
    "created_repos":            [],
    "uploaded":                 {},
    "skipped":                  [],
}

# ── logging ───────────────────────────────────────────────────────────────────

def log(tag, msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [{tag}] {msg}", flush=True)

# ── state I/O ─────────────────────────────────────────────────────────────────

def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            state = json.load(f)
        log("state", f"resuming | offset={state['last_processed_offset']} "
                     f"global={state['global_index']} "
                     f"repo={state['current_repo_index']} "
                     f"repo_count={state['current_repo_video_count']} "
                     f"uploaded={len(state['uploaded'])} "
                     f"skipped={len(state['skipped'])}")
        return state
    log("state", "no previous state, starting fresh")
    return dict(DEFAULT_STATE)


def _write_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)


def save_state(state):
    _write_json(STATE_FILE, state)


def save_results(state):
    results = [
        {
            "uuid":             uuid,
            "video_src":        d["video_src"],
            "filename":         d["filename"],
            "repo":             d["repo"],
            "github_pages_url": d["github_pages_url"],
        }
        for uuid, d in state["uploaded"].items()
    ]
    _write_json(RESULTS_FILE, results)

# ── results-repo git commit ───────────────────────────────────────────────────

def commit_state_to_results_repo(label: str) -> bool:
    """
    Push pipeline_state.json + upload_results.json to the results repo.
    Returns True if committed successfully, False otherwise.
    """
    repo_dir = RESULTS_DIR

    def _git(*args):
        return subprocess.run(
            ["git", "-C", str(repo_dir)] + list(args),
            capture_output=True, text=True,
        )

    # FIX: set authenticated remote URL so push works from inside Python
    _git("remote", "set-url", "origin",
         f"https://{GITHUB_TOKEN}@github.com/{GITHUB_USERNAME}/{RESULTS_REPO_NAME}.git")

    _git("config", "user.email", "pipeline@partner-videos.local")
    _git("config", "user.name",  "Partner Video Pipeline")

    _git("add", "pipeline_state.json", "upload_results.json")

    # nothing staged — no changes
    if _git("diff", "--cached", "--quiet").returncode == 0:
        log("git", "nothing to commit")
        return True

    r = _git("commit", "-m", f"pipeline: {label}")
    if r.returncode != 0:
        log("git", f"commit FAILED: {r.stderr.strip()}")
        return False

    _git("pull", "--rebase", "--autostash")

    r = _git("push")
    if r.returncode != 0:
        log("git", f"push FAILED: {r.stderr.strip()}")
        return False

    log("git", f"pushed: {label}")
    return True

# ── GitHub API helpers ────────────────────────────────────────────────────────

def repo_name(i):
    return f"partner_videos{i}"


def create_repo(name):
    log("repo", f"creating {name}")
    r = requests.post(
        f"{GITHUB_API}/user/repos",
        headers=GH_HEADERS,
        json={"name": name, "private": False, "auto_init": True},
    )
    if r.status_code not in [200, 201, 422]:
        raise Exception(f"create repo failed: {r.text}")

    requests.post(
        f"{GITHUB_API}/repos/{GITHUB_USERNAME}/{name}/pages",
        headers=GH_HEADERS,
        json={"source": {"branch": "main", "path": "/"}},
    )
    log("repo", f"{name} created, waiting 2s for GitHub Pages")
    time.sleep(2)

# ── git helpers for video repos ───────────────────────────────────────────────

def clone_url(repo):
    return f"https://{GITHUB_TOKEN}@github.com/{GITHUB_USERNAME}/{repo}.git"


def git(args, cwd):
    x = subprocess.run(
        ["git"] + args, cwd=cwd, capture_output=True, text=True
    )
    if x.returncode:
        raise RuntimeError(x.stderr)
    return x


def clone_or_open(repo):
    repo_dir = CLONE_DIR / repo
    if repo_dir.exists():
        log("git", f"pulling {repo}")
        git(["pull", "--rebase"], repo_dir)
    else:
        log("git", f"cloning {repo}")
        CLONE_DIR.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["git", "clone", clone_url(repo), str(repo_dir)], check=True
        )
    git(["config", "user.email", "pipeline@partner-videos.local"], repo_dir)
    git(["config", "user.name",  "Partner Video Pipeline"],        repo_dir)
    return repo_dir


def commit_push(repo_dir, file):
    git(["add", file],                    repo_dir)
    git(["commit", "-m", f"add {file}"], repo_dir)
    git(["push"],                         repo_dir)

# ── download ──────────────────────────────────────────────────────────────────

def download(url, dest):
    try:
        with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as r:
            r.raise_for_status()
            downloaded = 0
            with open(dest, "wb") as f:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        downloaded += len(chunk)
                        if downloaded > MAX_FILE_MB * 1024 * 1024:
                            log("download", f"SKIPPED — exceeded {MAX_FILE_MB}MB ({url[:60]})")
                            return False
                        f.write(chunk)
        size_mb = downloaded / 1024 / 1024
        log("download", f"ok {size_mb:.1f}MB")
        return True
    except Exception as exc:
        log("download", f"FAILED: {exc} ({url[:60]})")
        return False

# ── DB helper ─────────────────────────────────────────────────────────────────

def fetch_batch(conn, offset, limit):
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

# ── main loop ─────────────────────────────────────────────────────────────────

def run():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    CLONE_DIR.mkdir(parents=True, exist_ok=True)

    state = load_state()
    conn  = psycopg2.connect(DATABASE_URL)
    log("db", "connected")

    try:
        while True:
            batch = fetch_batch(conn, state["last_processed_offset"], DB_BATCH_SIZE)
            if not batch:
                log("pipeline", "DATABASE FINISHED — all rows processed")
                break

            for row in batch:
                uuid  = row["id"]
                video = row["video_src"]

                # already uploaded — just advance offset
                if uuid in state["uploaded"]:
                    state["last_processed_offset"] += 1
                    continue

                # roll over to next repo if current is full
                if state["current_repo_video_count"] >= VIDEOS_PER_REPO:
                    state["current_repo_index"]       += 1
                    state["current_repo_video_count"]  = 0
                    log("pipeline", f"rolling over to repo {state['current_repo_index']}")

                repo = repo_name(state["current_repo_index"])

                if repo not in state["created_repos"]:
                    create_repo(repo)
                    state["created_repos"].append(repo)

                repo_dir = clone_or_open(repo)

                short = uuid.replace("-", "")[:8]
                file  = (
                    f"{VIDEO_PREFIX}"
                    f"{short}_"
                    f"{state['current_repo_video_count']}_"
                    f"{state['global_index']}"
                    f".mp4"
                )
                temp = DOWNLOAD_DIR / file

                log("pipeline",
                    f"repo={repo} slot={state['current_repo_video_count']}/{VIDEOS_PER_REPO} "
                    f"global={state['global_index']} offset={state['last_processed_offset']} "
                    f"file={file}")

                ok = download(video, temp)

                if not ok:
                    state["skipped"].append(uuid)
                    state["last_processed_offset"] += 1
                    save_state(state)
                    committed = commit_state_to_results_repo(
                        f"skip {short} offset={state['last_processed_offset']}"
                    )
                    log("pipeline",
                        f"SKIPPED {short} | offset={state['last_processed_offset']} "
                        f"| total_skipped={len(state['skipped'])} "
                        f"| state_committed={committed}")
                    temp.unlink(missing_ok=True)
                    continue

                try:
                    shutil.copy2(temp, repo_dir / file)
                    commit_push(repo_dir, file)
                finally:
                    temp.unlink(missing_ok=True)

                url = (
                    f"https://{GITHUB_USERNAME}.github.io/"
                    f"{repo}/{file}"
                )

                state["uploaded"][uuid] = {
                    "video_src":        video,
                    "filename":         file,
                    "repo":             repo,
                    "github_pages_url": url,
                }
                state["current_repo_video_count"] += 1
                state["global_index"]             += 1
                state["last_processed_offset"]    += 1

                save_state(state)
                save_results(state)

                committed = commit_state_to_results_repo(
                    f"upload {file} offset={state['last_processed_offset']}"
                )

                log("pipeline",
                    f"UPLOADED {file} "
                    f"| offset={state['last_processed_offset']} "
                    f"| uploaded={len(state['uploaded'])} "
                    f"| skipped={len(state['skipped'])} "
                    f"| state_committed={committed}")

    finally:
        conn.close()
        log("db", "connection closed")


if __name__ == "__main__":
    run()
