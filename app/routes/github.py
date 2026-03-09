from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from app.auth import get_current_user
from app.database import supabase
from app.config import settings as app_settings
from pydantic import BaseModel
from typing import Optional, List, Dict
import httpx
import base64
import secrets

router = APIRouter(prefix="/github", tags=["github"])


def get_github_token(user: dict) -> str:
    result = (
        supabase.table("user_settings")
        .select("github_token")
        .eq("user_id", user["user_id"])
        .execute()
    )
    if not result.data or not result.data[0].get("github_token"):
        raise HTTPException(
            status_code=401,
            detail="GitHub not connected. Please reconnect your GitHub account."
        )
    return result.data[0]["github_token"]


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


@router.post("/token")
async def save_github_token(
    body: dict,
    user: dict = Depends(get_current_user)
):
    token = body.get("token")
    if not token:
        raise HTTPException(status_code=400, detail="No token provided")

    supabase.table("user_settings").upsert({
        "user_id": user["user_id"],
        "github_token": token,
        "updated_at": "now()"
    }, on_conflict="user_id").execute()

    return {"saved": True}


@router.get("/repos")
async def list_repos(user: dict = Depends(get_current_user)):
    token = get_github_token(user)

    async with httpx.AsyncClient() as client:
        res = await client.get(
            "https://api.github.com/user/repos?sort=updated&per_page=100",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json"
            },
            timeout=15.0
        )

    if res.status_code != 200:
        raise HTTPException(
            status_code=res.status_code,
            detail=f"GitHub API error: {res.text}"
        )

    repos = res.json()
    return {
        "repos": [
            {
                "id": r["id"],
                "name": r["name"],
                "full_name": r["full_name"],
                "private": r["private"],
                "url": r["html_url"],
                "updated_at": r["updated_at"]
            }
            for r in repos
        ]
    }


@router.post("/repos")
async def create_repo(
    body: CreateRepoRequest,
    user: dict = Depends(get_current_user)
):
    token = get_github_token(user)

    async with httpx.AsyncClient() as client:
        res = await client.post(
            "https://api.github.com/user/repos",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json"
            },
            json={
                "name": body.name,
                "description": body.description,
                "private": body.private,
                "auto_init": True
            },
            timeout=10.0
        )

    if res.status_code not in (200, 201):
        raise HTTPException(
            status_code=res.status_code,
            detail=res.json().get("message", "Failed to create repository")
        )

    repo = res.json()
    return {
        "repo": {
            "id": repo["id"],
            "name": repo["name"],
            "full_name": repo["full_name"],
            "url": repo["html_url"]
        }
    }


@router.post("/commit")
async def commit_file(
    body: CommitFileRequest,
    user: dict = Depends(get_current_user)
):
    token = get_github_token(user)
    encoded_content = base64.b64encode(body.content.encode("utf-8")).decode("utf-8")

    async with httpx.AsyncClient() as client:
        # Get current file SHA if it exists
        check_res = await client.get(
            f"https://api.github.com/repos/{body.repo_full_name}/contents/{body.path}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json"
            },
            params={"ref": body.branch}
        )

        payload = {
            "message": body.message,
            "content": encoded_content,
            "branch": body.branch
        }

        if check_res.status_code == 200:
            payload["sha"] = check_res.json()["sha"]

        put_res = await client.put(
            f"https://api.github.com/repos/{body.repo_full_name}/contents/{body.path}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json"
            },
            json=payload,
            timeout=12.0
        )

    if put_res.status_code not in (200, 201):
        raise HTTPException(
            status_code=put_res.status_code,
            detail=put_res.json().get("message", "Failed to commit file")
        )

    return {
        "committed": True,
        "url": put_res.json().get("content", {}).get("html_url")
    }


@router.delete("/repos/{owner}/{repo}")
async def delete_repo(
    owner: str,
    repo: str,
    user: dict = Depends(get_current_user)
):
    token = get_github_token(user)

    async with httpx.AsyncClient() as client:
        res = await client.delete(
            f"https://api.github.com/repos/{owner}/{repo}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json"
            },
            timeout=10.0
        )

    if res.status_code != 204:
        raise HTTPException(
            status_code=res.status_code,
            detail="Failed to delete repository"
        )

    return {"deleted": True}


# ── Webhook ────────────────────────────────────────────────────────────────

@router.post("/webhook/setup")
async def setup_webhook(user: dict = Depends(get_current_user)):
    token = get_github_token(user)

    result = (
        supabase.table("user_settings")
        .select("selected_repo_full_name, github_webhook_secret")
        .eq("user_id", user["user_id"])
        .execute()
    )

    if not result.data:
        raise HTTPException(400, "No user settings found")

    settings = result.data[0]
    repo = settings.get("selected_repo_full_name")

    if not repo:
        raise HTTPException(400, "No repository selected. Please select one first.")

    webhook_secret = settings.get("github_webhook_secret") or secrets.token_hex(32)

    # Upsert secret
    supabase.table("user_settings").upsert({
        "user_id": user["user_id"],
        "github_webhook_secret": webhook_secret,
        "updated_at": "now()"
    }, on_conflict="user_id").execute()

    webhook_url = f"https://devflow-api-production.up.railway.app/webhooks/github/{user['user_id']}"

    async with httpx.AsyncClient() as client:
        res = await client.post(
            f"https://api.github.com/repos/{repo}/hooks",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json"
            },
            json={
                "name": "web",
                "active": True,
                "events": ["push", "pull_request", "issues"],
                "config": {
                    "url": webhook_url,
                    "content_type": "json",
                    "secret": webhook_secret
                }
            },
            timeout=12.0
        )

    if res.status_code in (200, 201):
        return {
            "webhook_url": webhook_url,
            "status": "active",
            "repo": repo
        }

    if res.status_code == 422:  # validation failed → hook probably already exists
        return {
            "webhook_url": webhook_url,
            "status": "already_active",
            "repo": repo
        }

    raise HTTPException(
        status_code=res.status_code,
        detail=res.json().get("message", "Failed to register webhook")
    )


# ── Repository file tree ───────────────────────────────────────────────────
@router.get("/tree")
async def get_repo_tree(
    user: dict = Depends(get_current_user)
):
    """
    Returns flat list of all files (blobs) in the default branch (usually main/master)
    Uses recursive=1 tree endpoint → good for small to medium repositories
    """
    token = get_github_token(user)

    # Get selected repo
    result = (
        supabase.table("user_settings")
        .select("selected_repo_full_name")
        .eq("user_id", user["user_id"])
        .execute()
    )

    if not result.data or not result.data[0].get("selected_repo_full_name"):
        raise HTTPException(
            status_code=400,
            detail="No repository selected. Please select a repository first."
        )

    repo_full_name = result.data[0]["selected_repo_full_name"]

    async with httpx.AsyncClient() as client:
        res = await client.get(
            f"https://api.github.com/repos/{repo_full_name}/git/trees/HEAD?recursive=1",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json"
            },
            timeout=15.0
        )

        if res.status_code != 200:
            detail = res.json().get("message", "Could not fetch repository tree")
            if "Not Found" in detail or res.status_code == 404:
                raise HTTPException(404, "Repository not found or access denied")
            raise HTTPException(status_code=400, detail=detail)

        tree = res.json().get("tree", [])
        files = [item["path"] for item in tree if item["type"] == "blob"]

    return JSONResponse({
        "files": files[:50],
        "repo": repo_full_name,
        "count": len(files),
        "truncated": len(files) > 50
    })