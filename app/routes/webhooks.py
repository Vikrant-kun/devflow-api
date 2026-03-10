from fastapi import APIRouter, Request, HTTPException, Header
from app.database import query, query_one
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
    row = query_one(
        "SELECT github_webhook_secret FROM user_settings WHERE user_id = %s",
        (user_id,)
    )
    secret = row.get("github_webhook_secret") if row else None

    if secret and x_hub_signature_256:
        expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, x_hub_signature_256):
            raise HTTPException(status_code=401, detail="Invalid signature")

    payload = json.loads(body)
    event_type = request.headers.get("X-GitHub-Event", "push")
    workflows = query("SELECT * FROM workflows WHERE user_id = %s", (user_id,))
    triggered = []

    for workflow in workflows:
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
                query(
                    """INSERT INTO workflow_runs (user_id, workflow_id, workflow_name, status, started_at, duration, triggered_by, snapshot, logs)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (user_id, workflow["id"], workflow["name"], result["status"],
                     result["started_at"], result["duration"], f"github_{event_type}",
                     json.dumps({"nodes": nodes, "edges": edges}), json.dumps(result["logs"]))
                )
                triggered.append(workflow["name"])
                break

    return {"triggered": triggered, "event": event_type}