import os
import json
import time
import shutil
import subprocess
import sys
from pathlib import Path

import psycopg2
import requests

GITHUB_TOKEN=os.environ["GITHUB_TOKEN"]
GITHUB_USERNAME=os.environ["GITHUB_USERNAME"]
DATABASE_URL=os.environ["DATABASE_URL"]

RESULTS_DIR=Path(
    os.environ.get("RESULTS_DIR","results_repo")
)

REPO_START_INDEX=6
VIDEOS_PER_REPO=100
DB_BATCH_SIZE=10

MAX_FILE_MB=100
DOWNLOAD_TIMEOUT=120

VIDEO_PREFIX="pv_"

WORK_DIR=Path("pipeline_work")
CLONE_DIR=WORK_DIR/"clones"
DOWNLOAD_DIR=WORK_DIR/"downloads"

STATE_FILE=RESULTS_DIR/"pipeline_state.json"
RESULTS_FILE=RESULTS_DIR/"upload_results.json"

GITHUB_API="https://api.github.com"

GH_HEADERS={
    "Authorization":f"token {GITHUB_TOKEN}",
    "Accept":"application/vnd.github+json"
}


DEFAULT_STATE={

    "last_processed_offset":0,

    "global_index":0,

    "current_repo_index":REPO_START_INDEX,

    "current_repo_video_count":0,

    "created_repos":[],

    "uploaded":{},

    "skipped":[]
}


def load_state():

    if STATE_FILE.exists():

        with open(STATE_FILE) as f:
            state=json.load(f)

        print(
            f"resume offset={state['last_processed_offset']}"
        )

        return state

    return dict(DEFAULT_STATE)


def save_state(state):

    RESULTS_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(STATE_FILE,"w") as f:
        json.dump(
            state,
            f,
            indent=2
        )


def save_results(state):

    results=[]

    for uuid,data in state["uploaded"].items():

        results.append({

            "uuid":uuid,

            "video_src":data["video_src"],

            "filename":data["filename"],

            "repo":data["repo"],

            "github_pages_url":
            data["github_pages_url"]

        })

    with open(RESULTS_FILE,"w") as f:

        json.dump(
            results,
            f,
            indent=2
        )


def fetch_batch(
        conn,
        offset,
        limit
):

    cur=conn.cursor()

    cur.execute("""

        SELECT id::text,video_src
        FROM blog_posts
        WHERE video_src LIKE 'https://best%%'
        ORDER BY id
        LIMIT %s OFFSET %s

    """,(limit,offset))

    rows=cur.fetchall()

    cur.close()

    return [

        {
            "id":r[0],
            "video_src":r[1]
        }

        for r in rows
    ]


def repo_name(i):

    return f"partner_videos{i}"


def create_repo(name):

    print("create",name)

    r=requests.post(

        f"{GITHUB_API}/user/repos",

        headers=GH_HEADERS,

        json={

            "name":name,

            "private":False,

            "auto_init":True
        }
    )

    if r.status_code not in [200,201,422]:

        raise Exception(r.text)

    try:

        requests.post(

            f"{GITHUB_API}/repos/{GITHUB_USERNAME}/{name}/pages",

            headers=GH_HEADERS,

            json={
                "source":{
                    "branch":"main",
                    "path":"/"
                }
            }
        )

    except:
        pass

    time.sleep(2)


def clone_url(repo):

    return (
        f"https://{GITHUB_TOKEN}"
        f"@github.com/"
        f"{GITHUB_USERNAME}"
        f"/{repo}.git"
    )


def git(args,cwd):

    x=subprocess.run(

        ["git"]+args,

        cwd=cwd,

        capture_output=True,

        text=True
    )

    if x.returncode:

        raise RuntimeError(x.stderr)

    return x


def clone_or_open(repo):

    repo_dir=CLONE_DIR/repo

    if repo_dir.exists():

        git(
            ["pull","--rebase"],
            repo_dir
        )

    else:

        CLONE_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        subprocess.run(

            [
                "git",
                "clone",
                clone_url(repo),
                str(repo_dir)
            ],

            check=True
        )

    git(
        ["config","user.email",
        "pipeline@partner-videos.local"],
        repo_dir
    )

    git(
        ["config","user.name",
        "Partner Video Pipeline"],
        repo_dir
    )

    return repo_dir


def commit_push(
        repo_dir,
        file,
        db_id
):

    git(
        ["add",file],
        repo_dir
    )

    git(
        [
            "commit",
            "-m",
            f"add {file}"
        ],
        repo_dir
    )

    git(
        ["push"],
        repo_dir
    )


def download(
        url,
        dest
):

    try:

        with requests.get(
            url,
            stream=True,
            timeout=DOWNLOAD_TIMEOUT
        ) as r:

            r.raise_for_status()

            downloaded=0

            with open(
                    dest,
                    "wb"
            ) as f:

                for chunk in r.iter_content(
                        1024*1024
                ):

                    if chunk:

                        downloaded+=len(chunk)

                        if downloaded > (
                                MAX_FILE_MB
                                *1024
                                *1024
                        ):

                            return False

                        f.write(chunk)

        return True

    except:

        return False


def run():

    DOWNLOAD_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    CLONE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    state=load_state()

    conn=psycopg2.connect(
        DATABASE_URL
    )

    try:

        while True:

            batch=fetch_batch(

                conn,

                state[
                    "last_processed_offset"
                ],

                DB_BATCH_SIZE
            )

            if not batch:

                print(
                    "DATABASE FINISHED"
                )

                break

            for row in batch:

                uuid=row["id"]

                video=row["video_src"]

                if uuid in state["uploaded"]:

                    state[
                        "last_processed_offset"
                    ]+=1

                    continue


                if state[
                    "current_repo_video_count"
                ]>=VIDEOS_PER_REPO:

                    state[
                        "current_repo_index"
                    ]+=1

                    state[
                        "current_repo_video_count"
                    ]=0


                repo=repo_name(

                    state[
                        "current_repo_index"
                    ]
                )


                if repo not in state[
                    "created_repos"
                ]:

                    create_repo(repo)

                    state[
                        "created_repos"
                    ].append(repo)


                repo_dir=clone_or_open(
                    repo
                )


                short=uuid.replace(
                    "-",
                    ""
                )[:8]


                file=(
                    f"{VIDEO_PREFIX}"
                    f"{short}_"
                    f"{state['current_repo_video_count']}_"
                    f"{state['global_index']}"
                    f".mp4"
                )

                temp=DOWNLOAD_DIR/file


                print(
                    "download",
                    file
                )

                ok=download(
                    video,
                    temp
                )

                if not ok:

                    state[
                        "skipped"
                    ].append(uuid)

                    state[
                        "last_processed_offset"
                    ]+=1

                    save_state(state)

                    continue


                shutil.copy2(
                    temp,
                    repo_dir/file
                )

                commit_push(
                    repo_dir,
                    file,
                    uuid
                )

                url=(
                    f"https://"
                    f"{GITHUB_USERNAME}"
                    f".github.io/"
                    f"{repo}/"
                    f"{file}"
                )

                state[
                    "uploaded"
                ][uuid]={

                    "video_src":video,

                    "filename":file,

                    "repo":repo,

                    "github_pages_url":url
                }


                state[
                    "current_repo_video_count"
                ]+=1

                state[
                    "global_index"
                ]+=1

                state[
                    "last_processed_offset"
                ]+=1


                save_state(state)

                save_results(state)

                temp.unlink(
                    missing_ok=True
                )

                print(
                    "uploaded",
                    file
                )

    finally:

        conn.close()


if __name__=="__main__":
    run()
