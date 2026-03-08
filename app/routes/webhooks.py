from fastapi import APIRouter, Request, HTTPException, Header
from app.database import supabase
from app.services.executor import execute_workflow
import hmac, hashlib, json

router = APIRouter(prefix="/webhooks", tags=["webhooks"])

@router.post("/github/{user_id}")
async def github_webhook(
    user_id: str,
    request: Request,
    x_hub_signature_256: str = Header(None)
):
    body = await request.body()
    result = supabase.table("user_settings").select("github_webhook_secret").eq("user_id", user_id).execute()
    secret = result.data[0].get("github_webhook_secret") if result.data else None

    if secret and x_hub_signature_256:
        expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, x_hub_signature_256):
            raise HTTPException(status_code=401, detail="Invalid signature")

    payload = json.loads(body)
    event_type = request.headers.get("X-GitHub-Event", "push")

    workflows = supabase.table("workflows").select("*").eq("user_id", user_id).execute()
    triggered = []

    for workflow in (workflows.data or []):
        nodes = workflow.get("nodes", [])
        edges = workflow.get("edges", [])
        for node in nodes:
            node_data = node.get("data", {})
            label = node_data.get("label", "").lower()
            node_type = node_data.get("type", "")
            should_trigger = (
                node_type == "trigger" and (
                    (event_type == "push" and any(k in label for k in ["push", "commit", "merge"])) or
                    (event_type == "pull_request" and any(k in label for k in ["pr", "pull request"])) or
                    (event_type == "issues" and any(k in label for k in ["issue", "bug"]))
                )
            )
            if should_trigger:
                result = await execute_workflow(
                    nodes=nodes, edges=edges, user_id=user_id,
                    context={"event_type": event_type, "payload": str(payload)[:500], "triggered_by": "github_webhook"}
                )
                supabase.table("workflow_runs").insert({
                    "user_id": user_id,
                    "workflow_id": workflow["id"],
                    "workflow_name": workflow["name"],
                    "status": result["status"],
                    "started_at": result["started_at"],
                    "duration": result["duration"],
                    "triggered_by": f"github_{event_type}",
                    "snapshot": {"nodes": nodes, "edges": edges},
                    "logs": result["logs"]
                }).execute()
                triggered.append(workflow["name"])
                break

    return {"triggered": triggered, "event": event_type}
