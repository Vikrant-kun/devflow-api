from fastapi import APIRouter, Depends, HTTPException
from app.auth import get_current_user
from app.database import supabase
from app.config import settings as app_settings
from pydantic import BaseModel
from typing import Optional
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
        raise HTTPException(status_code=401, detail="GitHub not connected. Please reconnect.")
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
            "https://api.github.com/user/repos?sort=updated&per_page=30",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
        )
    if res.status_code != 200:
        raise HTTPException(status_code=res.status_code, detail="GitHub API error")
    repos = res.json()
    return {"repos": [{"id": r["id"], "name": r["name"], "full_name": r["full_name"], "private": r["private"], "url": r["html_url"], "updated_at": r["updated_at"]} for r in repos]}

@router.post("/repos")
async def create_repo(
    body: CreateRepoRequest,
    user: dict = Depends(get_current_user)
):
    token = get_github_token(user)
    async with httpx.AsyncClient() as client:
        res = await client.post(
            "https://api.github.com/user/repos",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            json={"name": body.name, "description": body.description, "private": body.private, "auto_init": True}
        )
    if res.status_code not in [200, 201]:
        raise HTTPException(status_code=res.status_code, detail=res.json().get("message", "Failed to create repo"))
    repo = res.json()
    return {"repo": {"id": repo["id"], "name": repo["name"], "full_name": repo["full_name"], "url": repo["html_url"]}}

@router.post("/commit")
async def commit_file(
    body: CommitFileRequest,
    user: dict = Depends(get_current_user)
):
    token = get_github_token(user)
    encoded = base64.b64encode(body.content.encode()).decode()

    async with httpx.AsyncClient() as client:
        # Check if file exists to get SHA
        check = await client.get(
            f"https://api.github.com/repos/{body.repo_full_name}/contents/{body.path}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
        )
        payload = {"message": body.message, "content": encoded, "branch": body.branch}
        if check.status_code == 200:
            payload["sha"] = check.json()["sha"]

        res = await client.put(
            f"https://api.github.com/repos/{body.repo_full_name}/contents/{body.path}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            json=payload
        )
    if res.status_code not in [200, 201]:
        raise HTTPException(status_code=res.status_code, detail=res.json().get("message", "Failed to commit"))
    return {"committed": True, "url": res.json().get("content", {}).get("html_url")}

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
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
        )
    if res.status_code != 204:
        raise HTTPException(status_code=res.status_code, detail="Failed to delete repo")
    return {"deleted": True}


# ── Webhook setup ─────────────────────────────────────────────────

@router.post("/webhook/setup")
async def setup_webhook(user: dict = Depends(get_current_user)):
    token = get_github_token(user)

    result = supabase.table("user_settings") \
        .select("selected_repo_full_name, github_webhook_secret") \
        .eq("user_id", user["user_id"]) \
        .execute()
    settings_data = result.data[0] if result.data else {}
    repo = settings_data.get("selected_repo_full_name")

    if not repo:
        raise HTTPException(status_code=400, detail="No repository selected. Choose one in Integrations first.")

    # Reuse existing secret or generate a new one
    webhook_secret = settings_data.get("github_webhook_secret") or secrets.token_hex(32)

    # Save the secret regardless (idempotent upsert)
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
            timeout=10.0
        )

    if res.status_code in [200, 201]:
        return {"webhook_url": webhook_url, "status": "active", "repo": repo}

    # If hook already exists GitHub returns 422 — treat it as success
    if res.status_code == 422:
        return {"webhook_url": webhook_url, "status": "already_active", "repo": repo}

    raise HTTPException(
        status_code=res.status_code,
        detail=res.json().get("message", "Failed to register webhook")
    )

