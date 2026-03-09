import httpx
import re
import base64 as b64
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
    
    # Check explicit email field first, then scan label/description
    to_email = node_data.get("email", "")
    if not to_email:
        text = f"{label} {description}"
        match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
        if not match:
            raise Exception("No email address found. Add an email in the node config panel.")
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
                "from": "DevFlow <onboarding@resend.dev>",
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


# ── AI Code Edit executor ────────────────────────────────────────────────────

async def _execute_ai_code_edit(node_data: dict, integrations: dict, context: dict) -> str:
    token = integrations.get("github_token")
    repo = integrations.get("selected_repo_full_name")
    if not token or not repo:
        raise Exception("GitHub not connected or no repo selected.")

    description = (node_data.get("description") or node_data.get("label") or "").strip()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}

    # ── Step 1: Try to extract explicit file path from description/label ──
    file_match = re.search(r'(?:file|path|fix|edit|in|at)\s*:\s*([a-zA-Z0-9_/.-]+\.[a-zA-Z0-9]+)', description, re.IGNORECASE)
    if not file_match:
        file_match = re.search(r'[a-zA-Z0-9_/.-]+\.(?:py|js|jsx|ts|tsx|css|html|json|md|yaml|yml|java|go|rs|cpp|c|h)', description)

    if file_match:
        filepath = file_match.group(0).strip()
    else:
        filepath = None

    async with httpx.AsyncClient(timeout=45.0) as client:
        # ── Step 2: Get file tree ───────────────────────────────────────
        tree_res = await client.get(
            f"https://api.github.com/repos/{repo}/git/trees/HEAD?recursive=1",
            headers=headers
        )
        if tree_res.status_code != 200:
            raise Exception(f"Cannot access repository tree: {tree_res.json().get('message', tree_res.status_code)}")

        tree = tree_res.json().get("tree", [])
        code_files = [
            f["path"] for f in tree
            if f["type"] == "blob"
            and any(f["path"].lower().endswith(ext) for ext in [".py", ".js", ".jsx", ".ts", ".tsx", ".css", ".html", ".json", ".yaml", ".yml"])
            and not any(skip in f["path"].lower() for skip in ["node_modules/", "dist/", "build/", ".min.", "vendor/", "__pycache__/", "venv/"])
        ]

        if not code_files:
            raise Exception("No relevant code files found in the repository.")

        # ── Step 3: Select file ─────────────────────────────────────────
        if filepath and filepath in code_files:
            selected_path = filepath
        else:
            # Ask AI to pick the best file
            file_list_str = "\n".join(code_files[:60])  # limit to avoid token explosion
            pick_prompt = (
                f"Repository: {repo}\n\n"
                f"Task description: {description or 'find and fix bugs / improve code'}\n\n"
                f"From this list of files, select the SINGLE most relevant file to work on for this task.\n"
                f"Return ONLY the full file path, nothing else.\n\n"
                f"Files:\n{file_list_str}"
            )

            pick_res = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {settings.GROQ_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "llama-3.3-70b-versatile",
                    "messages": [{"role": "user", "content": pick_prompt}],
                    "max_tokens": 80,
                    "temperature": 0.1
                },
                timeout=15.0
            )

            if pick_res.status_code == 200:
                ai_choice = pick_res.json()["choices"][0]["message"]["content"].strip().strip('"').strip("'")
                if ai_choice in code_files:
                    selected_path = ai_choice
                else:
                    selected_path = code_files[0]  # fallback
            else:
                selected_path = code_files[0]  # safest fallback

    # ── Step 4: Read selected file ──────────────────────────────────────
    async with httpx.AsyncClient(timeout=30.0) as client:
        content_res = await client.get(
            f"https://api.github.com/repos/{repo}/contents/{selected_path}",
            headers=headers
        )
        if content_res.status_code != 200:
            raise Exception(f"Cannot read file {selected_path}: {content_res.json().get('message')}")

        file_data = content_res.json()
        if file_data.get("encoding") != "base64":
            raise Exception(f"Unexpected encoding for {selected_path}")

        original_content = b64.b64decode(file_data["content"]).decode("utf-8", errors="replace")
        sha = file_data["sha"]

        # Optional: skip very large files
        if len(original_content) > 180_000:
            return f"Skipped {selected_path} — file too large ({len(original_content)//1000} kB)"

        # ── Step 5: Ask AI to fix ───────────────────────────────────────
        fix_prompt = (
            f"You are an expert code reviewer and fixer.\n"
            f"File: {selected_path}\n\n"
            f"Task / context: {description or 'Find bugs, security issues, performance problems, bad patterns and fix them'}\n\n"
            f"Return ONLY the complete fixed code — no explanations, no markdown fences, no comments about changes.\n"
            f"If the code is already good, return it unchanged.\n\n"
            f"```text\n{original_content}\n```"
        )

        fix_res = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {settings.GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": fix_prompt}],
                "max_tokens": 8192,
                "temperature": 0.15
            },
            timeout=60.0
        )

        if fix_res.status_code != 200:
            raise Exception(f"AI fix request failed: {fix_res.status_code}")

        fixed_code = fix_res.json()["choices"][0]["message"]["content"].strip()

        # Clean up possible markdown fences the model sometimes adds anyway
        fixed_code = re.sub(r'^```[\w]*\n?', '', fixed_code)
        fixed_code = re.sub(r'\n?```$', '', fixed_code)

        # ── Step 6: Compare & commit only if changed ─────────────────────
        if fixed_code.strip() == original_content.strip():
            return f"✅ No issues found / no changes needed in {selected_path}"

        encoded = b64.b64encode(fixed_code.encode("utf-8")).decode("utf-8")

        # Get default branch first
        branch_res = await client.get(
            f"https://api.github.com/repos/{repo}",
            headers=headers
        )
        default_branch = branch_res.json().get("default_branch", "main") if branch_res.status_code == 200 else "main"

        commit_res = await client.put(
            f"https://api.github.com/repos/{repo}/contents/{selected_path}",
            headers=headers,
            json={
                "message": f"DevFlow AI: improved/fixed {selected_path}",
                "content": encoded,
                "sha": sha,
                "branch": default_branch
            }
        )

        if commit_res.status_code not in (200, 201):
            raise Exception(f"Commit failed: {commit_res.json().get('message', commit_res.status_code)}")

        return f"✅ Fixed and committed {selected_path} to {repo}/{default_branch}"


# ── Main node dispatcher ──────────────────────────────────────────

async def _execute_node(node_type: str, node_data: dict, user_id: str, integrations: dict, context: dict) -> str:
    label = node_data.get("label", "step").lower()
    icon = node_data.get("icon", "")
    description = node_data.get("description", "")

    if node_type == "trigger":
        return f"Trigger activated: {node_data.get('label', 'Pipeline started')}"

    elif node_type == "ai":
        return await _execute_ai(node_data, context, integrations)

    elif node_type == "action" or node_type == "notification":
        # Email detection
        node_email = node_data.get("email", "")
        if "mail" in icon or "email" in label or "@" in label or "@" in description or "@" in node_email:
            return await _execute_email(node_data, context)

        # Code edit / fix / refactor detection
        elif any(k in label for k in ["fix", "edit", "refactor", "improve", "debug", "error", "finder", "check", "scan", "review", "clean", "lint", "bug"]) or (any(k in label for k in ["push", "commit"]) and any(k in label for k in ["main", "branch", "code"])):
            return await _execute_ai_code_edit(node_data, integrations, context)

        # Generic GitHub actions
        elif any(k in label for k in ["github", "commit", "push", "pr", "pull request", "branch", "repo"]) or icon in ["git-branch", "github"]:
            return await _execute_github(node_data, integrations, context)

        # Slack / notifications
        elif any(k in label for k in ["slack", "notify", "notification", "message", "alert"]) or icon in ["bell", "slack"]:
            return await _execute_slack(node_data, integrations, context)

        # Notion / Linear / Jira
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
            parent_output = "\n".join(context.get("parent_outputs", []))
            content = parent_output or description or f"# Generated by DevFlow\n\nPipeline ran at {datetime.now(timezone.utc).isoformat()}"
            filename = f"devflow-output-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.md"
            encoded = b64.b64encode(content.encode()).decode()
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


# ── execute_workflow (sync version) ──────────────────────────────────────

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


# ── WebSocket-aware executor ──────────────────────────────────────

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


# ── Other executors (unchanged) ───────────────────────────────────

async def _execute_ai(node_data: dict, context: dict, integrations: dict) -> str:
    description = node_data.get("description", "")
    label = node_data.get("label", "AI Step")
    parent_output = "\n".join(context.get("parent_outputs", []))
    model = node_data.get("model", "groq")

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
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={settings.GEMINI_API_KEY}",
                headers={"Content-Type": "application/json"},
                json={"contents": [{"parts": [{"text": prompt}]}]},
                timeout=20.0
            )
            res.raise_for_status()
            return res.json()["candidates"][0]["content"]["parts"][0]["text"].strip()

        else:
            res = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {settings.GROQ_API_KEY}"},
                json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}], "max_tokens": 300},
                timeout=20.0
            )
            res.raise_for_status()
            return res.json()["choices"][0]["message"]["content"].strip()


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


async def _execute_notion(node_data: dict, integrations: dict, context: dict) -> str:
    token = integrations.get("notion_token")
    if not token:
        raise Exception("Notion not connected. Go to Integrations → Connect Notion.")
    return f"Notion: '{node_data.get('label')}' — connect Notion in Integrations to activate"


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


async def _execute_jira(node_data: dict, integrations: dict, context: dict) -> str:
    token = integrations.get("jira_token")
    domain = integrations.get("jira_domain")
    
    if not token or not domain:
        raise Exception("Jira not connected. Go to Integrations → Connect Jira.")
        
    import base64
    label = node_data.get("label", "Issue")
    description = node_data.get("description", "") or "\n".join(context.get("parent_outputs", []))
    
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