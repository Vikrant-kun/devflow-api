from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime
from app.auth import get_current_user
from app.database import query, query_one
from app.config import settings as app_settings
from pydantic import BaseModel
from typing import Optional
import httpx, base64, secrets, json, time
import asyncio

router = APIRouter(prefix="/github", tags=["github"])


# -----------------------------
# SHARED GITHUB CLIENT
# -----------------------------
_github_client: httpx.AsyncClient | None = None

def get_github_client() -> httpx.AsyncClient:
    global _github_client
    if _github_client is None or _github_client.is_closed:
        _github_client = httpx.AsyncClient(
            timeout=httpx.Timeout(15.0),
            limits=httpx.Limits(
                max_connections=20,
                max_keepalive_connections=10
            ),
            headers={"Accept": "application/vnd.github+json"}
        )
    return _github_client


# -----------------------------
# REPO CACHE
# -----------------------------
_repo_cache: dict[str, tuple[float, list]] = {}
REPO_CACHE_TTL = 60  # seconds

async def github_request(client, method, url, headers=None, **kwargs):
    res = await client.request(method, url, headers=headers, **kwargs)

    # Handle rate limits
    if res.status_code == 403 and "rate limit" in res.text.lower():
        reset = res.headers.get("x-ratelimit-reset")

        if reset:
            wait_time = max(int(reset) - int(time.time()), 1)

            if wait_time < 10:  # only wait if short
                await asyncio.sleep(wait_time)
                res = await client.request(method, url, headers=headers, **kwargs)

    if res.status_code == 401:
        raise HTTPException(status_code=401, detail="GitHub token expired. Please reconnect.")

    return res


def get_github_token(user: dict) -> str | None:
    row = query_one(
        "SELECT github_token FROM user_settings WHERE user_id = %s",
        (user["user_id"],)
    )
    if not row or not row.get("github_token"):
        return None
    return row["github_token"]


class CreateRepoRequest(BaseModel):
    name: str
    description: Optional[str] = ""
    private: bool = False


class CommitFileRequest(BaseModel):
    repo_full_name: str
    path: str
    content: str
    message: str
    branch: Optional[str] = "main"


class BranchInfo(BaseModel):
    name: str
    commit_sha: str
    protected: bool
    is_default: bool


# -----------------------------
# SAVE SETTINGS / TOKEN
# -----------------------------
@router.post("/token/")
async def save_settings(body: dict, user: dict = Depends(get_current_user)):

    if "token" in body:
        token = body["token"]

        client = get_github_client()

        test = await github_request(
            client,
            "GET",
            "https://api.github.com/user",
            headers={"Authorization": f"Bearer {token}"}
        )

        if test.status_code != 200:
            raise HTTPException(status_code=400, detail="Invalid GitHub token")

        _repo_cache.pop(user["user_id"], None)

    allowed_keys = {
        "token": "github_token",
        "slack_webhook_url": "slack_webhook_url",
        "notion_token": "notion_token",
        "linear_token": "linear_token",
        "jira_token": "jira_token",
        "jira_domain": "jira_domain"
    }

    updates = []
    params = []

    for key, col in allowed_keys.items():
        if key in body:
            updates.append(f"{col} = %s")
            params.append(body[key])

    if not updates:
        raise HTTPException(status_code=400, detail="No valid settings provided")

    set_clause = ", ".join(updates)

    query(
        f"INSERT INTO user_settings (user_id, updated_at) VALUES (%s, NOW()) "
        f"ON CONFLICT (user_id) DO UPDATE SET {set_clause}, updated_at = NOW()",
        (user["user_id"], *params)
    )

    return {"saved": True}


# -----------------------------
# GET USER SETTINGS
# -----------------------------
@router.get("/settings")
async def get_user_settings(user: dict = Depends(get_current_user)):
    row = query_one(
        "SELECT github_token, slack_webhook_url, notion_token, linear_token, jira_token, jira_domain FROM user_settings WHERE user_id = %s",
        (user["user_id"],)
    )
    if not row:
        return {}
    return row


# -----------------------------
# LIST REPOS WITH CACHE
# -----------------------------
@router.get("/repos/")
async def list_repos(user: dict = Depends(get_current_user)):

    token = get_github_token(user)
    has_pat = bool(token)
    
    cached = _repo_cache.get(user["user_id"])
    if cached and (time.time() - cached[0]) < REPO_CACHE_TTL:
        return {"repos": cached[1], "has_pat": has_pat}

    if not token:
        return {"repos": [], "has_pat": False}
    
    client = get_github_client()

    res = await github_request(
        client,
        "GET",
        "https://api.github.com/user/repos?sort=updated&per_page=100",
        headers={"Authorization": f"Bearer {token}"}
    )

    if res.status_code != 200:
        raise HTTPException(status_code=res.status_code, detail=f"GitHub API error: {res.text}")

    repos_list = [
        {
            "id": r["id"],
            "name": r["name"],
            "full_name": r["full_name"],
            "private": r["private"],
            "url": r["html_url"],
            "updated_at": r["updated_at"]
        }
        for r in res.json()
    ]

    _repo_cache[user["user_id"]] = (time.time(), repos_list)

    return {"repos": repos_list, "has_pat": has_pat}


# -----------------------------
# CREATE REPO
# -----------------------------
@router.post("/repos/")
async def create_repo(body: CreateRepoRequest, user: dict = Depends(get_current_user)):

    token = get_github_token(user)
    if not token:
        raise HTTPException(status_code=400, detail="GitHub not connected. Please connect your GitHub account.")
    client = get_github_client()

    res = await github_request(
        client,
        "POST",
        "https://api.github.com/user/repos",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "name": body.name,
            "description": body.description,
            "private": body.private,
            "auto_init": True
        }
    )

    if res.status_code not in (200, 201):
        raise HTTPException(status_code=res.status_code, detail=res.json().get("message", "Failed to create repository"))

    repo = res.json()

    _repo_cache.pop(user["user_id"], None)

    return {
        "repo": {
            "id": repo["id"],
            "name": repo["name"],
            "full_name": repo["full_name"],
            "url": repo["html_url"]
        }
    }


# -----------------------------
# COMMIT FILE
# -----------------------------
@router.post("/commit/")
async def commit_file(body: CommitFileRequest, user: dict = Depends(get_current_user)):

    token = get_github_token(user)
    if not token:
        raise HTTPException(status_code=400, detail="GitHub not connected. Please connect your GitHub account.")
    client = get_github_client()

    encoded_content = base64.b64encode(body.content.encode("utf-8")).decode("utf-8")

    check_res = await github_request(
        client,
        "GET",
        f"https://api.github.com/repos/{body.repo_full_name}/contents/{body.path}",
        headers={"Authorization": f"Bearer {token}"},
        params={"ref": body.branch}
    )

    payload = {
        "message": body.message,
        "content": encoded_content,
        "branch": body.branch
    }

    if check_res.status_code == 200:
        payload["sha"] = check_res.json()["sha"]

    put_res = await github_request(
        client,
        "PUT",
        f"https://api.github.com/repos/{body.repo_full_name}/contents/{body.path}",
        headers={"Authorization": f"Bearer {token}"},
        json=payload
    )

    if put_res.status_code not in (200, 201):
        raise HTTPException(status_code=put_res.status_code, detail=put_res.json().get("message", "Failed to commit file"))

    return {
        "committed": True,
        "url": put_res.json().get("content", {}).get("html_url")
    }


# -----------------------------
# DELETE REPO
# -----------------------------
@router.delete("/repos/{owner}/{repo}")
async def delete_repo(owner: str, repo: str, user: dict = Depends(get_current_user)):

    token = get_github_token(user)
    if not token:
        raise HTTPException(status_code=400, detail="GitHub not connected. Please connect your GitHub account.")
    client = get_github_client()

    res = await github_request(
        client,
        "DELETE",
        f"https://api.github.com/repos/{owner}/{repo}",
        headers={"Authorization": f"Bearer {token}"}
    )

    if res.status_code != 204:
        raise HTTPException(status_code=res.status_code, detail="Failed to delete repository")

    _repo_cache.pop(user["user_id"], None)

    return {"deleted": True}


# -----------------------------
# SELECT REPO
# -----------------------------

@router.get("/selected-repo/")
async def get_selected_repo(user: dict = Depends(get_current_user)):
    row = query_one(
        "SELECT selected_repo_full_name FROM user_settings WHERE user_id = %s",
        (user["user_id"],)
    )

    if not row or not row.get("selected_repo_full_name"):
        return {"repo": None}

    return {"repo": {"full_name": row["selected_repo_full_name"]}}

@router.post("/select-repo/")
async def select_repo(body: dict, user: dict = Depends(get_current_user)):

    repo_full_name = body.get("repo_full_name")

    if not repo_full_name:
        raise HTTPException(status_code=400, detail="No repo provided")

    query(
        """INSERT INTO user_settings (user_id, selected_repo_full_name, updated_at)
           VALUES (%s, %s, NOW())
           ON CONFLICT (user_id) DO UPDATE
           SET selected_repo_full_name = EXCLUDED.selected_repo_full_name, updated_at = NOW()""",
        (user["user_id"], repo_full_name)
    )

    return {"saved": True}

@router.get("/integration-settings/")
async def get_integration_settings(user=Depends(get_current_user)):
    settings = query_one(
        "SELECT slack_webhook_url, notion_token, linear_token, jira_token, jira_domain FROM user_settings WHERE user_id = %s",
        (user["user_id"],)
    )
    return settings or {}

@router.post("/integration-settings/")
async def save_integration_settings(data: dict, user=Depends(get_current_user)):
    fields = ["slack_webhook_url", "notion_token", "linear_token", "jira_token", "jira_domain"]
    updates = {k: v for k, v in data.items() if k in fields}
    if not updates:
        return {"ok": True}
    set_clause = ", ".join(f"{k} = %s" for k in updates)
    query(f"""
        INSERT INTO user_settings (user_id, {', '.join(updates.keys())})
        VALUES (%s, {', '.join(['%s']*len(updates))})
        ON CONFLICT (user_id) DO UPDATE SET {set_clause}
    """, [user["user_id"]] + list(updates.values()))
    return {"ok": True}

@router.get("/branches/")
async def get_branches(user: dict = Depends(get_current_user)):

    token = get_github_token(user)
    if not token:
        raise HTTPException(status_code=400, detail="GitHub not connected. Please connect your GitHub account.")

    repo = query_one(
        "SELECT selected_repo_full_name FROM user_settings WHERE user_id = %s",
        (user["user_id"],)
    )

    if not repo or not repo["selected_repo_full_name"]:
        raise HTTPException(status_code=400, detail="No repository selected")

    repo_full_name = repo["selected_repo_full_name"]

    client = get_github_client()

    res = await github_request(
        client,
        "GET",
        f"https://api.github.com/repos/{repo_full_name}/branches",
        headers={"Authorization": f"Bearer {token}"}
    )

    if res.status_code != 200:
        raise HTTPException(status_code=res.status_code, detail="Failed to fetch branches")

    branches = [
    {
        "name": b["name"],
        "commit_sha": b["commit"]["sha"],
        "protected": b.get("protected", False),
        "is_default": b["name"] == "main"
    }
    for b in res.json()
]

    return {"branches": branches}

@router.get("/repo-tree/")
async def get_repo_tree(user=Depends(get_current_user)):
    settings_row = query_one(
        "SELECT github_token, selected_repo_full_name FROM user_settings WHERE user_id = %s",
        (user["user_id"],)
    )
    if not settings_row:
        raise HTTPException(status_code=400, detail="GitHub not connected")
    
    token = settings_row.get("github_token")
    repo = settings_row.get("selected_repo_full_name")
    
    if not token or not repo:
        raise HTTPException(status_code=400, detail="GitHub not connected or no repo selected")
    
    client = get_github_client()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    
    res = await github_request(
        client,
        "GET",
        f"https://api.github.com/repos/{repo}/git/trees/HEAD?recursive=1",
        headers=headers
    )
    
    if res.status_code != 200:
        raise HTTPException(status_code=res.status_code, detail="Failed to fetch repo tree")
    
    tree = res.json().get("tree", [])
    
    # Filter only blobs (files), exclude noise
    files = [
        {"path": f["path"], "type": f["type"], "size": f.get("size", 0)}
        for f in tree
        if not any(skip in f["path"] for skip in [
            "node_modules/", "dist/", "build/", ".min.",
            "__pycache__/", "venv/", ".git/"
        ])
    ]
    
    return {"repo": repo, "files": files}

@router.post("/disconnect/")
async def disconnect_github(user=Depends(get_current_user)):

    query(
        "UPDATE user_settings SET github_token = NULL WHERE user_id = %s",
        (user["user_id"],)
    )

    # clear repo cache
    _repo_cache.pop(user["user_id"], None)

    return {"success": True}

@router.get("/pulls/")
async def get_pull_requests(user=Depends(get_current_user)):
    settings_row = query_one(
        "SELECT github_token, selected_repo_full_name FROM user_settings WHERE user_id = %s",
        (user["user_id"],)
    )
    if not settings_row:
        raise HTTPException(status_code=400, detail="GitHub not connected")
    
    token = settings_row.get("github_token")
    repo = settings_row.get("selected_repo_full_name")
    
    client = get_github_client()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    
    res = await github_request(
        client,
        "GET",
        f"https://api.github.com/repos/{repo}/pulls?state=open",
        headers=headers
    )
    if res.status_code != 200:
        raise HTTPException(status_code=res.status_code, detail="Failed to fetch PRs")
    
    return {"pulls": res.json()}

@router.post("/pulls/create/")
async def create_pull_request(body: dict, user=Depends(get_current_user)):
    settings_row = query_one(
        "SELECT github_token, selected_repo_full_name FROM user_settings WHERE user_id = %s",
        (user["user_id"],)
    )
    token = settings_row.get("github_token")
    repo = settings_row.get("selected_repo_full_name")
    
    client = get_github_client()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    
    res = await github_request(
        client,
        "POST",
        f"https://api.github.com/repos/{repo}/pulls",
        headers=headers,
        json={
            "title": body.get("title"),
            "body": body.get("body", ""),
            "head": body.get("head"),
            "base": body.get("base", "main")
        }
    )
    if res.status_code != 201:
        raise HTTPException(status_code=res.status_code, detail=res.json().get("message"))
    
    return res.json()

@router.put("/pulls/{pr_number}/merge/")
async def merge_pull_request(pr_number: int, user=Depends(get_current_user)):
    settings_row = query_one(
        "SELECT github_token, selected_repo_full_name FROM user_settings WHERE user_id = %s",
        (user["user_id"],)
    )
    token = settings_row.get("github_token")
    repo = settings_row.get("selected_repo_full_name")
    
    client = get_github_client()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    
    res = await github_request(
        client,
        "PUT",
        f"https://api.github.com/repos/{repo}/pulls/{pr_number}/merge",
        headers=headers,
        json={"merge_method": "squash"}
    )
    if res.status_code != 200:
        raise HTTPException(status_code=res.status_code, detail=res.json().get("message"))
    
    return res.json()