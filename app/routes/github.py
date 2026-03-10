from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime
from app.auth import get_current_user
from app.database import query, query_one
from app.config import settings as app_settings
from pydantic import BaseModel
from typing import Optional
import httpx, base64, secrets, json

router = APIRouter(prefix="/github", tags=["github"])


def get_github_token(user: dict) -> str:
    row = query_one(
        "SELECT github_token FROM user_settings WHERE user_id = %s",
        (user["user_id"],)
    )
    if not row or not row.get("github_token"):
        raise HTTPException(status_code=401, detail="GitHub not connected. Please reconnect your GitHub account.")
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


@router.post("/token")
async def save_github_token(body: dict, user: dict = Depends(get_current_user)):
    token = body.get("token")
    if not token:
        raise HTTPException(status_code=400, detail="No token provided")
    query(
        """INSERT INTO user_settings (user_id, github_token, updated_at)
           VALUES (%s, %s, NOW())
           ON CONFLICT (user_id) DO UPDATE SET github_token = EXCLUDED.github_token, updated_at = NOW()""",
        (user["user_id"], token)
    )
    return {"saved": True}


@router.get("/repos")
async def list_repos(user: dict = Depends(get_current_user)):
    token = get_github_token(user)
    async with httpx.AsyncClient() as client:
        res = await client.get(
            "https://api.github.com/user/repos?sort=updated&per_page=100",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            timeout=15.0
        )
    if res.status_code != 200:
        raise HTTPException(status_code=res.status_code, detail=f"GitHub API error: {res.text}")
    return {"repos": [{"id": r["id"], "name": r["name"], "full_name": r["full_name"], "private": r["private"], "url": r["html_url"], "updated_at": r["updated_at"]} for r in res.json()]}


@router.post("/repos")
async def create_repo(body: CreateRepoRequest, user: dict = Depends(get_current_user)):
    token = get_github_token(user)
    async with httpx.AsyncClient() as client:
        res = await client.post(
            "https://api.github.com/user/repos",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            json={"name": body.name, "description": body.description, "private": body.private, "auto_init": True},
            timeout=10.0
        )
    if res.status_code not in (200, 201):
        raise HTTPException(status_code=res.status_code, detail=res.json().get("message", "Failed to create repository"))
    repo = res.json()
    return {"repo": {"id": repo["id"], "name": repo["name"], "full_name": repo["full_name"], "url": repo["html_url"]}}


@router.post("/commit")
async def commit_file(body: CommitFileRequest, user: dict = Depends(get_current_user)):
    token = get_github_token(user)
    encoded_content = base64.b64encode(body.content.encode("utf-8")).decode("utf-8")
    async with httpx.AsyncClient() as client:
        check_res = await client.get(
            f"https://api.github.com/repos/{body.repo_full_name}/contents/{body.path}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            params={"ref": body.branch}
        )
        payload = {"message": body.message, "content": encoded_content, "branch": body.branch}
        if check_res.status_code == 200:
            payload["sha"] = check_res.json()["sha"]
        put_res = await client.put(
            f"https://api.github.com/repos/{body.repo_full_name}/contents/{body.path}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            json=payload, timeout=12.0
        )
    if put_res.status_code not in (200, 201):
        raise HTTPException(status_code=put_res.status_code, detail=put_res.json().get("message", "Failed to commit file"))
    return {"committed": True, "url": put_res.json().get("content", {}).get("html_url")}


@router.delete("/repos/{owner}/{repo}")
async def delete_repo(owner: str, repo: str, user: dict = Depends(get_current_user)):
    token = get_github_token(user)
    async with httpx.AsyncClient() as client:
        res = await client.delete(
            f"https://api.github.com/repos/{owner}/{repo}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            timeout=10.0
        )
    if res.status_code != 204:
        raise HTTPException(status_code=res.status_code, detail="Failed to delete repository")
    return {"deleted": True}


@router.post("/webhook/setup")
async def setup_webhook(user: dict = Depends(get_current_user)):
    token = get_github_token(user)
    row = query_one(
        "SELECT selected_repo_full_name, github_webhook_secret FROM user_settings WHERE user_id = %s",
        (user["user_id"],)
    )
    if not row:
        raise HTTPException(400, "No user settings found")
    repo = row.get("selected_repo_full_name")
    if not repo:
        raise HTTPException(400, "No repository selected. Please select one first.")
    webhook_secret = row.get("github_webhook_secret") or secrets.token_hex(32)
    query(
        """INSERT INTO user_settings (user_id, github_webhook_secret, updated_at)
           VALUES (%s, %s, NOW())
           ON CONFLICT (user_id) DO UPDATE SET github_webhook_secret = EXCLUDED.github_webhook_secret, updated_at = NOW()""",
        (user["user_id"], webhook_secret)
    )
    webhook_url = f"https://devflow-api-production.up.railway.app/webhooks/github/{user['user_id']}"
    async with httpx.AsyncClient() as client:
        res = await client.post(
            f"https://api.github.com/repos/{repo}/hooks",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            json={"name": "web", "active": True, "events": ["push", "pull_request", "issues"],
                  "config": {"url": webhook_url, "content_type": "json", "secret": webhook_secret}},
            timeout=12.0
        )
    if res.status_code in (200, 201):
        return {"webhook_url": webhook_url, "status": "active", "repo": repo}
    if res.status_code == 422:
        return {"webhook_url": webhook_url, "status": "already_active", "repo": repo}
    raise HTTPException(status_code=res.status_code, detail=res.json().get("message", "Failed to register webhook"))


@router.get("/tree")
async def get_repo_tree(user: dict = Depends(get_current_user)):
    token = get_github_token(user)
    row = query_one("SELECT selected_repo_full_name FROM user_settings WHERE user_id = %s", (user["user_id"],))
    repo = row.get("selected_repo_full_name") if row else None
    if not repo:
        raise HTTPException(status_code=400, detail="No repo selected")
    async with httpx.AsyncClient() as client:
        res = await client.get(
            f"https://api.github.com/repos/{repo}/git/trees/HEAD?recursive=1",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
        )
    if res.status_code != 200:
        raise HTTPException(status_code=400, detail="Could not fetch repo tree")
    files = [f["path"] for f in res.json().get("tree", []) if f["type"] == "blob"]
    return {"files": files[:50], "repo": repo}


@router.post("/select-repo")
async def select_repo(body: dict, user: dict = Depends(get_current_user)):
    repo_full_name = body.get("repo_full_name")
    if not repo_full_name:
        raise HTTPException(status_code=400, detail="No repo provided")
    query(
        """INSERT INTO user_settings (user_id, selected_repo_full_name, updated_at)
           VALUES (%s, %s, NOW())
           ON CONFLICT (user_id) DO UPDATE SET selected_repo_full_name = EXCLUDED.selected_repo_full_name, updated_at = NOW()""",
        (user["user_id"], repo_full_name)
    )
    return {"saved": True}


@router.get("/branches")
async def list_branches(user: dict = Depends(get_current_user)):
    token = get_github_token(user)
    row = query_one("SELECT selected_repo_full_name FROM user_settings WHERE user_id = %s", (user["user_id"],))
    if not row or not row.get("selected_repo_full_name"):
        raise HTTPException(status_code=400, detail="No repository selected.")
    repo_full_name = row["selected_repo_full_name"]
    async with httpx.AsyncClient() as client:
        repo_res = await client.get(
            f"https://api.github.com/repos/{repo_full_name}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            timeout=10.0
        )
        if repo_res.status_code != 200:
            raise HTTPException(status_code=repo_res.status_code, detail=f"GitHub API error: {repo_res.text}")
        default_branch = repo_res.json().get("default_branch")
        branches_res = await client.get(
            f"https://api.github.com/repos/{repo_full_name}/branches?per_page=100",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            timeout=12.0
        )
    if branches_res.status_code != 200:
        raise HTTPException(status_code=branches_res.status_code, detail=f"Failed to fetch branches: {branches_res.text}")
    branches = [{"name": b["name"], "commit_sha": b["commit"]["sha"], "protected": b.get("protected", False), "is_default": b["name"] == default_branch} for b in branches_res.json()]
    branches.sort(key=lambda x: (not x["is_default"], x["name"]))
    return {"repo": repo_full_name, "branches": branches, "default_branch": default_branch, "total": len(branches)}