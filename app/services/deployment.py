import base64
import httpx
from datetime import datetime, timezone
from app.config import settings # Assuming this holds your BREVO_API_KEY and POSTHOG_KEY

async def commit_to_github(repo: str, filepath: str, fixed_code: str, github_token: str, http_client: httpx.AsyncClient) -> dict:
    """
    Step 16a: Git Commit (Success).
    Pushes the sandbox-verified code straight to the user's repository.
    """
    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json"
    }
    
    # 1. Get the file's current SHA (required by GitHub API to update a file)
    file_url = f"https://api.github.com/repos/{repo}/contents/{filepath}"
    file_res = await http_client.get(file_url, headers=headers)
    
    if file_res.status_code != 200:
        return {"status": "error", "message": "Could not fetch file SHA for commit."}
        
    sha = file_res.json().get("sha")
    
    # 2. Encode the new fixed code in Base64
    encoded_content = base64.b64encode(fixed_code.encode("utf-8")).decode("utf-8")
    
    # 3. Push the commit
    commit_res = await http_client.put(
        file_url,
        headers=headers,
        json={
            "message": f"DevFlow AI: Automated fix for {filepath}",
            "content": encoded_content,
            "sha": sha
        }
    )
    
    if commit_res.status_code in (200, 201):
        commit_url = commit_res.json().get("commit", {}).get("html_url", "")
        return {"status": "success", "url": commit_url}
        
    return {"status": "error", "message": f"GitHub commit failed: {commit_res.text}"}


async def send_fallback_email(user_email: str, filepath: str, console_log: str, http_client: httpx.AsyncClient) -> str:
    """
    Step 16b: Email Fallback (Failure).
    If the Sandbox and Free Retry loop both fail, abort the commit and email the user.
    """
    html_content = f"""
    <div style="font-family:monospace;background:#080808;color:#F1F5F9;padding:28px;border-radius:16px;">
        <h2 style="color:#EF4444;">⚠️ DevFlow AI Execution Failed</h2>
        <p>The automated fix for <b>{filepath}</b> failed the sandbox test suite.</p>
        <p>The original code has been preserved. No changes were committed to your repository.</p>
        <div style="background:#111;border:1px solid #222;padding:16px;margin-top:16px;">
            <p style="color:#444;font-size:10px;">SANDBOX CONSOLE OUTPUT</p>
            <pre style="color:#F1F5F9;font-size:11px;white-space:pre-wrap;">{console_log}</pre>
        </div>
    </div>
    """
    
    res = await http_client.post(
        "https://api.brevo.com/v3/smtp/email",
        headers={
            "api-key": settings.BREVO_API_KEY,
            "Content-Type": "application/json"
        },
        json={
            "sender": {"name": "DevFlow AI", "email": settings.GMAIL_USER},
            "to": [{"email": user_email}],
            "subject": f"DevFlow Alert: Fix failed for {filepath}",
            "htmlContent": html_content
        },
        timeout=15.0
    )
    
    return "Fallback email sent."


async def log_observability_event(user_id: str, event_name: str, properties: dict, http_client: httpx.AsyncClient):
    """
    Step 18: Production Observability.
    Silently logs the run to PostHog (Free Tier) so you can track product usage.
    """
    if not hasattr(settings, 'POSTHOG_API_KEY') or not settings.POSTHOG_API_KEY:
        return # Skip if not configured
        
    await http_client.post(
        "https://us.i.posthog.com/capture/",
        json={
            "api_key": settings.POSTHOG_API_KEY,
            "event": event_name,
            "distinct_id": user_id,
            "properties": properties,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    )