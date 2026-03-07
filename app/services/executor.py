import httpx
from datetime import datetime, timezone
from app.config import settings

async def execute_workflow(nodes: list, edges: list, user_id: str) -> dict:
    """
    Executes a workflow by walking the node graph in order.
    Each node type has a handler. Real integrations go here later.
    Returns execution result with per-node logs.
    """
    start = datetime.now(timezone.utc)
    logs = []
    status = "success"

    # Build adjacency: find execution order via topological sort
    node_map = {n["id"]: n for n in nodes}
    adj = {n["id"]: [] for n in nodes}
    for edge in edges:
        adj[edge["source"]].append(edge["target"])

    # Find root (trigger node — no incoming edges)
    has_incoming = {e["target"] for e in edges}
    roots = [n["id"] for n in nodes if n["id"] not in has_incoming]
    if not roots:
        roots = [nodes[0]["id"]] if nodes else []

    # BFS execution order
    visited = []
    queue = list(roots)
    seen = set()
    while queue:
        nid = queue.pop(0)
        if nid in seen:
            continue
        seen.add(nid)
        visited.append(nid)
        queue.extend(adj.get(nid, []))

    # Execute each node
    for nid in visited:
        node = node_map.get(nid)
        if not node:
            continue
        node_data = node.get("data", {})
        node_type = node_data.get("type", node.get("type", "action"))
        label = node_data.get("label", "Unknown Step")

        try:
            result = await _execute_node(node_type, node_data, user_id)
            logs.append({
                "node_id": nid,
                "label": label,
                "type": node_type,
                "status": "success",
                "message": result,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            status = "failed"
            logs.append({
                "node_id": nid,
                "label": label,
                "type": node_type,
                "status": "failed",
                "message": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            break  # Stop on first failure

    end = datetime.now(timezone.utc)
    duration_seconds = (end - start).total_seconds()
    duration_str = f"{int(duration_seconds // 60)}m {int(duration_seconds % 60)}s"

    return {
        "status": status,
        "duration": duration_str,
        "logs": logs,
        "started_at": start.isoformat(),
        "finished_at": end.isoformat()
    }


async def _execute_node(node_type: str, node_data: dict, user_id: str) -> str:
    """
    Individual node execution handlers.
    Stub implementations — replace with real integrations.
    """
    label = node_data.get("label", "step")
    icon = node_data.get("icon", "")

    if node_type == "trigger":
        return f"Trigger '{label}' activated"

    elif node_type == "action":
        # Real GitHub/Slack/Jira calls go here
        if "github" in label.lower() or icon in ["git-branch", "github"]:
            return await _stub_github(label)
        elif "slack" in label.lower() or icon == "bell":
            return await _stub_slack(label)
        elif "jira" in label.lower():
            return await _stub_jira(label)
        else:
            return f"Action '{label}' executed"

    elif node_type == "ai":
        return await _stub_ai(label, node_data)

    elif node_type == "notification":
        return f"Notification sent: '{label}'"

    else:
        return f"Step '{label}' completed"


# ── Stub handlers (replace with real API calls) ──────────────────

async def _stub_github(label: str) -> str:
    # TODO: use stored GitHub token from user_settings
    # async with httpx.AsyncClient() as client:
    #     res = await client.post("https://api.github.com/...", headers={"Authorization": f"Bearer {token}"})
    return f"GitHub: '{label}' — stub executed (connect token in Integrations)"

async def _stub_slack(label: str) -> str:
    # TODO: use stored Slack webhook from user_settings
    return f"Slack: '{label}' — stub executed (connect webhook in Integrations)"

async def _stub_jira(label: str) -> str:
    return f"Jira: '{label}' — stub executed (connect token in Integrations)"

async def _stub_ai(label: str, node_data: dict) -> str:
    description = node_data.get("description", "")
    async with httpx.AsyncClient() as client:
        try:
            res = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {settings.GROQ_API_KEY}"
                },
                json={
                    "model": "llama-3.3-70b-versatile",
                    "messages": [{"role": "user", "content": f"Execute this AI step briefly: {description or label}"}],
                    "max_tokens": 200
                },
                timeout=15.0
            )
            if res.status_code == 200:
                data = res.json()
                content = data["choices"][0]["message"]["content"]
                return f"AI '{label}': {content[:120]}..."
            return f"AI '{label}' — Groq returned {res.status_code}"
        except Exception as e:
            return f"AI '{label}' — failed: {str(e)}"
