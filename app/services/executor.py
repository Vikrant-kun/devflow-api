import httpx
import re
from datetime import datetime, timezone
from app.config import settings
from app.database import supabase


# ── Fetch user integrations ───────────────────────────────────────

async def get_user_integrations(user_id: str) -> dict:
    result = supabase.table("user_settings").select(
        "github_token, selected_repo_full_name, slack_webhook_url, notion_token, linear_token, jira_token, jira_domain"
    ).eq("user_id", user_id).execute()
    return result.data[0] if result.data else {}


# ── Email executor ────────────────────────────────────────────────

async def _execute_email(node_data: dict, context: dict) -> str:
    label = node_data.get("label", "")
    description = node_data.get("description", "")
    parent_output = "\n".join(context.get("parent_outputs", []))
    
    # Extract email from label or description
    text = f"{label} {description}"
    match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
    if not match:
        raise Exception("No email address found in node. Add an email like 'notify team@company.com'")
        
    to_email = match.group(0)
    body = parent_output or description or f"DevFlow pipeline step '{label}' completed."
    
    async with httpx.AsyncClient() as client:
        res = await client.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {settings.RESEND_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "from": "DevFlow <notifications@devflow.ai>",
                "to": [to_email],
                "subject": f"DevFlow Pipeline: {label}",
                "html": f"""
                <div style="font-family:monospace;background:#080808;color:#F1F5F9;padding:24px;border-radius:12px;">
                    <h2 style="color:#6EE7B7;margin-bottom:16px;">⚡ DevFlow Pipeline Notification</h2>
                    <p style="color:#64748B;font-size:12px;">Step: <strong style="color:#F1F5F9">{label}</strong></p>
                    <div style="background:#111;border:1px solid #222;border-radius:8px;padding:16px;margin-top:12px;">
                        <pre style="color:#F1F5F9;font-size:12px;white-space:pre-wrap;">{body}</pre>
                    </div>
                    <p style="color:#333;font-size:10px;margin-top:16px;">Sent by DevFlow AI · devflow.ai</p>
                </div>
                """
            },
            timeout=10.0
        )
        if res.status_code in [200, 201]:
            return f"Email sent to {to_email}"
        raise Exception(f"Email failed: {res.json().get('message', res.status_code)}")


# ── Main executor ─────────────────────────────────────────────────

async def execute_workflow(nodes: list, edges: list, user_id: str, context: dict = {}) -> dict:
    start = datetime.now(timezone.utc)
    logs = []
    status = "success"
    integrations = await get_user_integrations(user_id)

    # Topological sort
    node_map = {n["id"]: n for n in nodes}
    adj = {n["id"]: [] for n in nodes}
    for edge in edges:
        adj[edge["source"]].append(edge["target"])

    has_incoming = {e["target"] for e in edges}
    roots = [n["id"] for n in nodes if n["id"] not in has_incoming]
    if not roots:
        roots = [nodes[0]["id"]] if nodes else []

    visited, queue, seen = [], list(roots), set()
    while queue:
        nid = queue.pop(0)
        if nid in seen:
            continue
        seen.add(nid)
        visited.append(nid)
        queue.extend(adj.get(nid, []))

    # Pass output between nodes
    node_outputs = {}

    for nid in visited:
        node = node_map.get(nid)
        if not node:
            continue
        node_data = node.get("data", {})
        node_type = node_data.get("type", node.get("type", "action"))
        label = node_data.get("label", "Unknown Step")

        # Gather inputs from parent nodes
        parent_outputs = [node_outputs[e["source"]] for e in edges if e["target"] == nid and e["source"] in node_outputs]
        node_context = {**context, "parent_outputs": parent_outputs}

        t_start = datetime.now(timezone.utc)
        try:
            result = await _execute_node(node_type, node_data, user_id, integrations, node_context)
            node_outputs[nid] = result
            duration = f"{(datetime.now(timezone.utc) - t_start).total_seconds():.1f}s"
            logs.append({
                "node_id": nid, "node_label": label, "type": node_type,
                "status": "success", "message": result, "duration": duration,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            status = "failed"
            duration = f"{(datetime.now(timezone.utc) - t_start).total_seconds():.1f}s"
            logs.append({
                "node_id": nid, "node_label": label, "type": node_type,
                "status": "failed", "message": str(e), "duration": duration,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            break

    end = datetime.now(timezone.utc)
    secs = (end - start).total_seconds()
    return {
        "status": status,
        "duration": f"{int(secs // 60)}m {int(secs % 60)}s" if secs >= 60 else f"{secs:.1f}s",
        "logs": logs,
        "started_at": start.isoformat(),
        "finished_at": end.isoformat()
    }


# ── WebSocket-aware executor (streams node updates in real time) ───

async def execute_workflow_ws(
    nodes: list,
    edges: list,
    user_id: str,
    context: dict = {},
    on_node_complete=None
) -> dict:
    start = datetime.now(timezone.utc)
    logs = []
    status = "success"
    integrations = await get_user_integrations(user_id)

    # Topological sort (same as execute_workflow)
    node_map = {n["id"]: n for n in nodes}
    adj = {n["id"]: [] for n in nodes}
    for edge in edges:
        adj[edge["source"]].append(edge["target"])

    has_incoming = {e["target"] for e in edges}
    roots = [n["id"] for n in nodes if n["id"] not in has_incoming]
    if not roots:
        roots = [nodes[0]["id"]] if nodes else []

    visited, queue, seen = [], list(roots), set()
    while queue:
        nid = queue.pop(0)
        if nid in seen:
            continue
        seen.add(nid)
        visited.append(nid)
        queue.extend(adj.get(nid, []))

    node_outputs = {}

    for nid in visited:
        node = node_map.get(nid)
        if not node:
            continue
        node_data = node.get("data", {})
        node_type = node_data.get("type", node.get("type", "action"))
        label = node_data.get("label", "Unknown Step")

        # Send "running" status before execution
        if on_node_complete:
            await on_node_complete({
                "node_id": nid,
                "node_label": label,
                "type": node_type,
                "status": "running",
                "message": "Executing...",
                "duration": None,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })

        parent_outputs = [node_outputs[e["source"]] for e in edges if e["target"] == nid and e["source"] in node_outputs]
        node_context = {**context, "parent_outputs": parent_outputs}

        t_start = datetime.now(timezone.utc)
        try:
            result = await _execute_node(node_type, node_data, user_id, integrations, node_context)
            node_outputs[nid] = result
            duration = f"{(datetime.now(timezone.utc) - t_start).total_seconds():.1f}s"
            log_entry = {
                "node_id": nid, "node_label": label, "type": node_type,
                "status": "success", "message": result, "duration": duration,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            logs.append(log_entry)
            if on_node_complete:
                await on_node_complete(log_entry)
        except Exception as e:
            status = "failed"
            duration = f"{(datetime.now(timezone.utc) - t_start).total_seconds():.1f}s"
            log_entry = {
                "node_id": nid, "node_label": label, "type": node_type,
                "status": "failed", "message": str(e), "duration": duration,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            logs.append(log_entry)
            if on_node_complete:
                await on_node_complete(log_entry)
            break

    end = datetime.now(timezone.utc)
    secs = (end - start).total_seconds()
    return {
        "status": status,
        "duration": f"{int(secs // 60)}m {int(secs % 60)}s" if secs >= 60 else f"{secs:.1f}s",
        "logs": logs,
        "started_at": start.isoformat(),
        "finished_at": end.isoformat()
    }



async def _execute_node(node_type: str, node_data: dict, user_id: str, integrations: dict, context: dict) -> str:
    label = node_data.get("label", "step").lower()
    icon = node_data.get("icon", "")
    description = node_data.get("description", "")

    if node_type == "trigger":
        return f"Trigger activated: {node_data.get('label', 'Pipeline started')}"

    elif node_type == "ai":
        return await _execute_ai(node_data, context, integrations)

    elif node_type == "action" or node_type == "notification":
        # Check email specifically
        if "mail" in icon or "email" in label or "@" in label or "@" in description:
            return await _execute_email(node_data, context)
            
        elif any(k in label for k in ["github", "commit", "push", "pr", "pull request", "branch", "repo"]) or icon in ["git-branch", "github"]:
            return await _execute_github(node_data, integrations, context)
        elif any(k in label for k in ["slack", "notify", "notification", "message", "alert"]) or icon in ["bell", "slack"]:
            return await _execute_slack(node_data, integrations, context)
        elif any(k in label for k in ["notion"]):
            return await _execute_notion(node_data, integrations, context)
        elif any(k in label for k in ["linear", "issue", "ticket"]):
            return await _execute_linear(node_data, integrations, context)
        elif any(k in label for k in ["jira", "ticket", "atlassian"]):
            return await _execute_jira(node_data, integrations, context)
        else:
            return f"Action '{node_data.get('label')}' executed"

    return f"Step '{node_data.get('label')}' completed"


# ── GitHub executor ───────────────────────────────────────────────

async def _execute_github(node_data: dict, integrations: dict, context: dict) -> str:
    token = integrations.get("github_token")
    repo = integrations.get("selected_repo_full_name")
    label = node_data.get("label", "").lower()
    description = node_data.get("description", "")

    if not token:
        raise Exception("GitHub not connected. Go to Integrations → Connect GitHub.")
    if not repo:
        raise Exception("No repository selected. Go to Integrations → select a repo.")

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}

    async with httpx.AsyncClient() as client:

        # Create branch
        if any(k in label for k in ["branch", "create branch"]):
            branch_name = f"devflow/{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
            # Get default branch SHA
            r = await client.get(f"https://api.github.com/repos/{repo}", headers=headers)
            r.raise_for_status()
            default_branch = r.json()["default_branch"]
            r2 = await client.get(f"https://api.github.com/repos/{repo}/git/ref/heads/{default_branch}", headers=headers)
            r2.raise_for_status()
            sha = r2.json()["object"]["sha"]
            r3 = await client.post(f"https://api.github.com/repos/{repo}/git/refs", headers=headers,
                json={"ref": f"refs/heads/{branch_name}", "sha": sha})
            r3.raise_for_status()
            return f"Branch created: {branch_name} in {repo}"

        # Create PR
        elif any(k in label for k in ["pr", "pull request", "open pr"]):
            parent_output = context.get("parent_outputs", [""])
            body = parent_output[-1] if parent_output else description or "Automated PR via DevFlow"
            r = await client.get(f"https://api.github.com/repos/{repo}", headers=headers)
            default_branch = r.json().get("default_branch", "main")
            r2 = await client.post(f"https://api.github.com/repos/{repo}/pulls", headers=headers,
                json={"title": f"DevFlow: {node_data.get('label', 'Automated PR')}",
                      "body": body, "head": default_branch, "base": default_branch})
            if r2.status_code in [200, 201]:
                pr = r2.json()
                return f"PR #{pr['number']} created: {pr['html_url']}"
            return f"GitHub action completed on {repo}"

        # Commit a file
        elif any(k in label for k in ["commit", "push", "upload", "file"]):
            import base64
            parent_output = "\n".join(context.get("parent_outputs", []))
            content = parent_output or description or f"# Generated by DevFlow\n\nPipeline ran at {datetime.now(timezone.utc).isoformat()}"
            filename = f"devflow-output-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.md"
            encoded = base64.b64encode(content.encode()).decode()
            r = await client.put(f"https://api.github.com/repos/{repo}/contents/{filename}",
                headers=headers,
                json={"message": f"DevFlow: {node_data.get('label', 'automated commit')}",
                      "content": encoded})
            if r.status_code in [200, 201]:
                return f"Committed {filename} to {repo}"
            raise Exception(f"GitHub commit failed: {r.json().get('message', r.status_code)}")

        # Default — create an issue
        else:
            parent_output = "\n".join(context.get("parent_outputs", []))
            body = parent_output or description or "Automated issue created by DevFlow pipeline"
            r = await client.post(f"https://api.github.com/repos/{repo}/issues", headers=headers,
                json={"title": node_data.get("label", "DevFlow Issue"), "body": body,
                      "labels": ["devflow", "automated"]})
            if r.status_code == 201:
                issue = r.json()
                return f"Issue #{issue['number']} created: {issue['html_url']}"
            raise Exception(f"GitHub API error: {r.json().get('message', r.status_code)}")


# ── AI executor ───────────────────────────────────────────────────

async def _execute_ai(node_data: dict, context: dict, integrations: dict) -> str:
    description = node_data.get("description", "")
    label = node_data.get("label", "AI Step")
    parent_output = "\n".join(context.get("parent_outputs", []))
    model = node_data.get("model", "groq")  # default groq

    prompt = f"""You are an AI step in a workflow pipeline.
Previous step output: {parent_output or 'None'}
Your task: {description or label}
Respond with a concise, actionable result (2-4 sentences max)."""

    async with httpx.AsyncClient() as client:

        if model == "gpt4" and settings.OPENAI_API_KEY:
            res = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {settings.OPENAI_API_KEY}"},
                json={"model": "gpt-4o", "messages": [{"role": "user", "content": prompt}], "max_tokens": 300},
                timeout=20.0
            )
            res.raise_for_status()
            return res.json()["choices"][0]["message"]["content"].strip()

        elif model == "gemini" and settings.GEMINI_API_KEY:
            res = await client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={settings.GEMINI_API_KEY}",
                headers={"Content-Type": "application/json"},
                json={"contents": [{"parts": [{"text": prompt}]}]},
                timeout=20.0
            )
            res.raise_for_status()
            return res.json()["candidates"][0]["content"]["parts"][0]["text"].strip()

        else:
            # Default — Groq
            res = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {settings.GROQ_API_KEY}"},
                json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}], "max_tokens": 300},
                timeout=20.0
            )
            res.raise_for_status()
            return res.json()["choices"][0]["message"]["content"].strip()


# ── Slack executor ────────────────────────────────────────────────

async def _execute_slack(node_data: dict, integrations: dict, context: dict) -> str:
    webhook_url = integrations.get("slack_webhook_url")
    label = node_data.get("label", "Notification")
    parent_output = "\n".join(context.get("parent_outputs", []))
    message = parent_output or node_data.get("description", "") or f"DevFlow pipeline step: {label}"

    if not webhook_url:
        raise Exception("Slack not connected. Go to Integrations → Connect Slack.")

    async with httpx.AsyncClient() as client:
        res = await client.post(webhook_url, json={
            "text": f"*DevFlow Pipeline* — _{label}_\n{message}",
            "username": "DevFlow Bot",
            "icon_emoji": ":zap:"
        }, timeout=10.0)
        if res.status_code == 200:
            return f"Slack message sent: '{label}'"
        raise Exception(f"Slack webhook failed: {res.status_code}")


# ── Notion executor ───────────────────────────────────────────────

async def _execute_notion(node_data: dict, integrations: dict, context: dict) -> str:
    token = integrations.get("notion_token")
    if not token:
        raise Exception("Notion not connected. Go to Integrations → Connect Notion.")
    parent_output = "\n".join(context.get("parent_outputs", []))
    # Stub — real Notion page creation needs a database_id from settings
    return f"Notion: '{node_data.get('label')}' — connect Notion in Integrations to activate"


# ── Linear executor ───────────────────────────────────────────────

async def _execute_linear(node_data: dict, integrations: dict, context: dict) -> str:
    token = integrations.get("linear_token")
    if not token:
        raise Exception("Linear not connected. Go to Integrations → Connect Linear.")
    parent_output = "\n".join(context.get("parent_outputs", []))
    label = node_data.get("label", "Issue")
    description = node_data.get("description", "") or parent_output

    async with httpx.AsyncClient() as client:
        res = await client.post("https://api.linear.app/graphql",
            headers={"Authorization": token, "Content-Type": "application/json"},
            json={"query": """mutation CreateIssue($title: String!, $description: String) {
                issueCreate(input: {title: $title, description: $description}) {
                    success issue { id title url }
                }
            }""", "variables": {"title": label, "description": description}},
            timeout=10.0)
        res.raise_for_status()
        data = res.json()
        issue = data.get("data", {}).get("issueCreate", {}).get("issue", {})
        if issue:
            return f"Linear issue created: {issue.get('title')} — {issue.get('url', '')}"
        raise Exception("Linear issue creation failed")

# ── Jira executor ─────────────────────────────────────────────────
async def _execute_jira(node_data: dict, integrations: dict, context: dict) -> str:
    token = integrations.get("jira_token")
    domain = integrations.get("jira_domain")
    
    if not token or not domain:
        raise Exception("Jira not connected. Go to Integrations → Connect Jira.")
        
    import base64
    label = node_data.get("label", "Issue")
    description = node_data.get("description", "") or "\n".join(context.get("parent_outputs", []))
    
    # Get first project key
    async with httpx.AsyncClient() as client:
        auth = base64.b64encode(f"devflow:{token}".encode()).decode()
        headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}
        
        r = await client.get(f"https://{domain}/rest/api/3/project?maxResults=1", headers=headers, timeout=10.0)
        r.raise_for_status()
        projects = r.json()
        
        project_list = projects if isinstance(projects, list) else projects.get("values", [])
        
        if not project_list:
            raise Exception("No Jira projects found.")
            
        project_key = project_list[0]["key"]
        
        res = await client.post(f"https://{domain}/rest/api/3/issue", headers=headers,
            json={"fields": {
                "project": {"key": project_key},
                "summary": label,
                "description": {"type": "doc", "version": 1, "content": [
                    {"type": "paragraph", "content": [{"type": "text", "text": description or label}]}
                ]},
                "issuetype": {"name": "Task"}
            }}, timeout=10.0)
        res.raise_for_status()
        issue = res.json()
        
        return f"Jira issue created: {issue['key']} — https://{domain}/browse/{issue['key']}"

# ── WebSocket executor (same as execute_workflow but streams node updates) ────

async def execute_workflow_ws(nodes: list, edges: list, user_id: str, context: dict = {}, on_node_complete=None) -> dict:
    start = datetime.now(timezone.utc)
    logs = []
    status = "success"
    integrations = await get_user_integrations(user_id)

    node_map = {n["id"]: n for n in nodes}
    adj = {n["id"]: [] for n in nodes}
    for edge in edges:
        adj[edge["source"]].append(edge["target"])

    has_incoming = {e["target"] for e in edges}
    roots = [n["id"] for n in nodes if n["id"] not in has_incoming]
    if not roots:
        roots = [nodes[0]["id"]] if nodes else []

    visited, queue, seen = [], list(roots), set()
    while queue:
        nid = queue.pop(0)
        if nid in seen:
            continue
        seen.add(nid)
        visited.append(nid)
        queue.extend(adj.get(nid, []))

    node_outputs = {}

    for nid in visited:
        node = node_map.get(nid)
        if not node:
            continue
        node_data = node.get("data", {})
        node_type = node_data.get("type", node.get("type", "action"))
        label = node_data.get("label", "Unknown Step")

        parent_outputs = [node_outputs[e["source"]] for e in edges if e["target"] == nid and e["source"] in node_outputs]
        node_context = {**context, "parent_outputs": parent_outputs}

        # Send "running" status before executing
        if on_node_complete:
            await on_node_complete({
                "node_id": nid, "node_label": label, "type": node_type,
                "status": "running", "message": "Executing...",
                "timestamp": datetime.now(timezone.utc).isoformat()
            })

        t_start = datetime.now(timezone.utc)
        try:
            result = await _execute_node(node_type, node_data, user_id, integrations, node_context)
            node_outputs[nid] = result
            duration = f"{(datetime.now(timezone.utc) - t_start).total_seconds():.1f}s"
            log_entry = {
                "node_id": nid, "node_label": label, "type": node_type,
                "status": "success", "message": result, "duration": duration,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            logs.append(log_entry)
            if on_node_complete:
                await on_node_complete(log_entry)
        except Exception as e:
            status = "failed"
            duration = f"{(datetime.now(timezone.utc) - t_start).total_seconds():.1f}s"
            log_entry = {
                "node_id": nid, "node_label": label, "type": node_type,
                "status": "failed", "message": str(e), "duration": duration,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            logs.append(log_entry)
            if on_node_complete:
                await on_node_complete(log_entry)
            break

    end = datetime.now(timezone.utc)
    secs = (end - start).total_seconds()
    return {
        "status": status,
        "duration": f"{int(secs // 60)}m {int(secs % 60)}s" if secs >= 60 else f"{secs:.1f}s",
        "logs": logs,
        "started_at": start.isoformat(),
        "finished_at": end.isoformat()
    }
