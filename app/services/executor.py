import httpx
import asyncio
import time
import re
import fnmatch
import base64 as b64
from datetime import datetime, timezone
from app.config import settings
from app.database import query_one
import json

from app.services.parser import sanitize_prompt, parse_intent
from app.services.snapshot import build_repo_snapshot
from app.services.ast_engine import extract_ast_data, build_dependency_graph
from app.services.bm25_engine import rank_and_retrieve_files
from app.services.ast_engine import trim_code_context
from app.services.ai_surgeon import execute_ai_planner
from app.services.ai_surgeon import execute_ai_coder
from app.services.shield_loop import local_syntax_check, detect_manifest
from app.services.shield_loop import local_syntax_check, detect_manifest
from app.services.sandbox import execute_docker_sandbox
from app.services.free_retry import execute_free_retry
from app.services.deployment import commit_to_github, send_fallback_email, log_observability_event

_TOOL_MEMORY = {}
_REPO_INDEX_CACHE = {}
_REPO_TREE_CACHE = {}
_EXECUTION_REFLECTION = {}

REPO_INDEX_TTL = 300
REPO_TREE_TTL = 120
REFLECTION_TTL = 3600

async def _get_cached_repo_tree(repo: str, tree_fetcher):

    now = time.time()

    cached = _REPO_TREE_CACHE.get(repo)

    if cached and now - cached["ts"] < REPO_TREE_TTL:
        return cached["tree"]

    tree = await tree_fetcher()

    _REPO_TREE_CACHE[repo] = {
        "tree": tree,
        "ts": now
    }

    return tree


def _remember_tool(repo: str, filepath: str):
    key = f"{repo}:{filepath}"
    _TOOL_MEMORY[key] = time.time()

def _recent_tools(repo: str):
    now = time.time()
    return [
        k.split(":",1)[1]
        for k,v in _TOOL_MEMORY.items()
        if k.startswith(repo) and now-v < 600
    ]


_http_client: httpx.AsyncClient | None = None

def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=5.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
        )
    return _http_client


_cache: dict = {}
CACHE_TTL = 60  # seconds

def cache_get(key: str):
    if key in _cache:
        val, ts = _cache[key]
        if time.time() - ts < CACHE_TTL:
            return val
        del _cache[key]
    return None

def cache_set(key: str, val):
    _cache[key] = (val, time.time())


async def _groq_request(payload: dict, timeout: float = 60.0):
    client = get_http_client()
    for attempt in range(3):
        res = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {settings.GROQ_API_KEY}",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=timeout
        )
        if res.status_code == 429:
            await asyncio.sleep(2 ** attempt)
            continue
        return res
    raise Exception("Groq rate limit exceeded after 3 retries")


# ── Fetch user integrations ───────────────────────────────────────
async def get_user_integrations(user_id: str) -> dict:
    row = query_one(
        "SELECT github_token, selected_repo_full_name, slack_webhook_url, notion_token, linear_token, jira_token, jira_domain FROM user_settings WHERE user_id = %s",
        (user_id,)
    )
    print(f"DEBUG get_user_integrations: user_id={user_id}, data={row}")
    return row or {}


# ── Email executor ────────────────────────────────────────────────
async def _execute_email(node_data: dict, context: dict) -> str:
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    label = node_data.get("label", "")
    description = node_data.get("description", "")
    parent_output = "\n".join(context.get("parent_outputs", []))

    to_email = node_data.get("email", "")
    if not to_email:
        text = f"{label} {description}"
        match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
        if not match:
            raise Exception("No email address found. Add an email in the node config panel.")
        to_email = match.group(0)

    body = parent_output or description or f"DevFlow pipeline step '{label}' completed."

    # Parse body for sections
    lines = body.split('\n')
    commit_url = next((w for line in lines for w in line.split() if 'github.com' in w and 'commit' in w), None)
    files_changed = [l.strip() for l in lines if any(l.strip().startswith(p) for p in ['- ', '📁', 'Modified:', 'Changed:', 'Fixed:'])]
    status_emoji = '✅' if any(w in body.lower() for w in ['fixed', 'success', 'no issues', 'clean']) else '⚠️'

    files_html = ''.join(f'<div style="padding:4px 0;border-bottom:1px solid #1A1A1A;color:#94A3B8;font-size:11px;">📁 {f}</div>' for f in files_changed[:10]) if files_changed else ''
    commit_html = f'<a href="{commit_url}" style="color:#6EE7B7;font-size:11px;">🔗 View Commit →</a>' if commit_url else ''

    html = f"""
    <div style="font-family:monospace;background:#080808;color:#F1F5F9;padding:28px;border-radius:16px;max-width:600px;margin:0 auto;">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:20px;">
            <span style="font-size:24px;">{status_emoji}</span>
            <div>
                <h2 style="color:#6EE7B7;margin:0;font-size:16px;">DevFlow Pipeline</h2>
                <p style="color:#64748B;margin:2px 0 0;font-size:11px;">Step: {label}</p>
            </div>
        </div>

        <div style="background:#111;border:1px solid #222;border-radius:10px;padding:16px;margin-bottom:16px;">
            <p style="color:#444;font-size:9px;uppercase;letter-spacing:2px;margin:0 0 10px;">PIPELINE OUTPUT</p>
            <pre style="color:#F1F5F9;font-size:11px;white-space:pre-wrap;margin:0;line-height:1.6;">{body}</pre>
        </div>

        {f'<div style="background:#111;border:1px solid #222;border-radius:10px;padding:16px;margin-bottom:16px;"><p style="color:#444;font-size:9px;letter-spacing:2px;margin:0 0 10px;">FILES CHANGED</p>{files_html}</div>' if files_html else ''}

        {f'<div style="margin-bottom:16px;">{commit_html}</div>' if commit_html else ''}

        <p style="color:#333;font-size:10px;margin:0;border-top:1px solid #1A1A1A;padding-top:12px;">Sent by DevFlow AI · pipeline automation</p>
    </div>
    """

    client = get_http_client()
    res = await client.post(
        "https://api.brevo.com/v3/smtp/email",
        headers={
            "api-key": settings.BREVO_API_KEY,
            "Content-Type": "application/json"
        },
        json={
            "sender": {"name": "DevFlow AI", "email": settings.GMAIL_USER},
            "to": [{"email": to_email}],
            "subject": f"DevFlow Pipeline: {label}",
            "htmlContent": html
        },
        timeout=15.0
    )
    if res.status_code in (200, 201):
        return f"✅ Email sent to {to_email}"
    raise Exception(f"Email failed: {res.status_code} {res.text}")


# Extract file filters from description
def extract_file_filters(text: str) -> list:
    filters = []
    text_lower = text.lower()
    
    # Check for folder hints like "copy/js/" or "copy/js" or "all js files"
    folder_match = re.findall(r'[a-zA-Z0-9_][a-zA-Z0-9_/.-]*/(?:[a-zA-Z0-9_.-]+/)*', text)
    for f in folder_match:
        filters.append(f)
    
    # Glob patterns like *.js
    glob_match = re.findall(r'\*\.[a-zA-Z0-9]+', text)
    filters.extend(glob_match)
    
    # Exact file paths like src/main.py
    file_match = re.findall(r'[a-zA-Z0-9_][a-zA-Z0-9_/.-]*\.[a-zA-Z0-9]{1,6}', text)
    for f in file_match:
        # skip if looks like a domain or version number
        if '/' in f or len(f.split('.')[-1]) <= 4:
            filters.append(f)
    
    return list(set(filters))

def match_files(code_files: list, filters: list) -> list:
    if not filters:
        return code_files
    matched = []
    for f in code_files:
        for pattern in filters:
            # folder prefix match — "copy/js" matches "copy/js/script.js"
            if f.startswith(pattern.rstrip('/') + '/') or f.startswith(pattern):
                if f not in matched:
                    matched.append(f)
                break
            # glob match
            elif '*' in pattern or '?' in pattern:
                if fnmatch.fnmatch(f, pattern) or fnmatch.fnmatch(f.split('/')[-1], pattern):
                    if f not in matched:
                        matched.append(f)
                    break
            # exact match
            elif f == pattern or f.endswith('/' + pattern):
                if f not in matched:
                    matched.append(f)
                break
    return matched or code_files

def _build_repo_index(repo: str, files: dict) -> list:

    index = []

    for path, content in files.items():

        imports = []
        functions = []
        keywords = set()

        for line in content.splitlines():

            line = line.strip()

            if line.startswith("import ") or line.startswith("from "):
                imports.append(line)

            if "def " in line or "function " in line:
                functions.append(line)

            for token in line.split():
                if len(token) > 4:
                    keywords.add(token.lower())

        index.append({
            "file": path,
            "imports": imports[:5],
            "functions": functions[:5],
            "keywords": list(keywords)[:20]
        })

    return index

def _rank_files_by_query(query: str, repo_index: list, limit: int = 6):

    words = set(query.lower().split())
    scored = []

    for entry in repo_index:

        searchable = " ".join(
            entry["imports"]
            + entry["functions"]
            + entry["keywords"]
            + [entry["file"]]
        ).lower()

        score = sum(1 for w in words if w in searchable)

        scored.append((score, entry["file"]))

    scored.sort(reverse=True)

    return [f for s, f in scored[:limit]]

def _get_repo_index(repo: str, file_map: dict):

    now = time.time()

    cached = _REPO_INDEX_CACHE.get(repo)

    if cached and now - cached["ts"] < REPO_INDEX_TTL:
        return cached["index"]

    index = _build_repo_index(repo, file_map)

    _REPO_INDEX_CACHE[repo] = {
        "index": index,
        "ts": now
    }

    return index

def _record_reflection(repo: str, task: str, file: str, result: str):

    key = f"{repo}:{task}"

    entries = _EXECUTION_REFLECTION.setdefault(key, [])

    entries.append({
        "file": file,
        "result": result,
        "ts": time.time()
    })

    _EXECUTION_REFLECTION[key] = entries[-5:]

def _reflection_penalty(repo: str, task: str):

    key = f"{repo}:{task}"

    history = _EXECUTION_REFLECTION.get(key, [])

    failed_files = [
        h["file"]
        for h in history
        if "failed" in h["result"].lower()
    ]

    return set(failed_files)


def _smart_chunk_file(content: str, query: str, max_chunks: int = 3) -> str:
    """
    Selects most relevant sections of code for AI context.
    Prevents sending entire files while avoiding truncation errors.
    """

    lines = content.splitlines()
    size = 120
    overlap = 30
    chunks = []

    for i in range(0, len(lines), size - overlap):
        chunk = "\n".join(lines[i:i+size])
        chunks.append(chunk)

    query_words = set(query.lower().split())
    scored = []

    for chunk in chunks:
        words = set(chunk.lower().split())
        score = len(query_words & words)
        scored.append((score, chunk))

    scored.sort(reverse=True)

    return "\n\n".join([c for s, c in scored[:max_chunks]])

async def _plan_code_task(description: str, repo: str, code_files: list, repo_index: list) -> dict:
    """
    Planner AI — decides WHAT to do before execution.
    Returns a structured plan with exact filenames from the repo.
    """

    file_list = "\n".join(code_files[:60])

    file_context = "\n".join(
        f"{e['file']} | functions: {','.join(e['functions'][:3])}"
        for e in repo_index[:40]
    )

    planner_prompt = f"""You are a software automation planner.

User task: {description}

Repository: {repo}

Repository structure:
{file_context}

Available files (ONLY use these exact paths):
{file_list}

Return ONLY valid JSON:

{{
  "target_files": ["exact/path/from/list.js"],
  "actions": [
    {{"action":"inspect","file":"exact/path.js","reason":"why"}},
    {{"action":"fix","file":"exact/path.js","focus":"what to fix"}},
    {{"action":"validate"}}
  ],
  "summary":"one line description"
}}

Rules:
- target_files MUST come only from the file list above
- NEVER invent filenames
- max 3 files
- prefer files mentioned in the task
"""

    res = await _groq_request({
        "model":"llama-3.3-70b-versatile",
        "messages":[{"role":"user","content":planner_prompt}],
        "max_tokens":400,
        "temperature":0
    }, timeout=15.0)

    if res.status_code != 200:
        return {"target_files":[code_files[0]],"actions":[],"summary":"fallback"}

    try:
        content = res.json()["choices"][0]["message"]["content"].strip()
        content = content.replace("```json","").replace("```","").strip()
        plan = json.loads(content)

        plan["target_files"] = [
            f for f in plan.get("target_files",[])
            if f in code_files
        ]

        if not plan["target_files"]:
            return {
                "status": "failed",
        "message": "Planner could not determine a valid target file."
            }

        return plan

    except:
        return {"target_files":[code_files[0]],"actions":[],"summary":"fallback"}

async def _critic_validate_fix(original_code: str,fixed_code: str,filepath: str) -> tuple[bool,str]:
    critic_prompt = f"""You are a strict code reviewer.

File: {filepath}

Original code:
{original_code[:3000]}

Modified code:
{fixed_code[:3000]}

Check for:
- syntax errors
- missing imports
- broken logic
- security issues
- regression bugs

Reply ONLY:
VALID
or
INVALID
"""
    res = await _groq_request({
        "model":"llama-3.3-70b-versatile",
        "messages":[{"role":"user","content":critic_prompt}],
        "max_tokens":5,
        "temperature":0
    }, timeout=15.0)

    if res.status_code == 200:
        verdict = res.json()["choices"][0]["message"]["content"].strip().upper()
        if verdict == "INVALID":
            return False, f"⚠️ AI fix rejected by critic for {filepath} — original code preserved"

    return True, ""


# ── AI Code Edit executor ────────────────────────────────────────────────────
async def _execute_ai_code_edit(node_data: dict, integrations: dict, context: dict) -> str:
    selected_files = context.get("selected_files") or node_data.get("selected_files") or []

    forced_file = None
    if selected_files:
        first = selected_files[0]
        if isinstance(first, dict):
            forced_file = first.get("path")
        else:
            forced_file = first
    token = integrations.get("github_token")
    repo = integrations.get("selected_repo_full_name")
    raw_prompt = (node_data.get("description") or node_data.get("label") or "").strip()

 # In a real app, pull the actual user email from your DB. Using a placeholder for now.
    user_email = "devflow-user@example.com" 
    user_id = context.get("user_id", "system")

    if not token or not repo:
        raise Exception("GitHub not connected or no repo selected.")

    client = get_http_client()

    # --- PHASE 1: THE GATEKEEPER ---
    if forced_file:
         raw_prompt = f"{raw_prompt}\n\nTarget file: {forced_file}"
    phase_1 = await execute_devflow_phase_one(repo, token, raw_prompt, client)
    if phase_1.get("status") != "success":
        raise Exception(phase_1.get("message"))
        
    snapshot = phase_1["snapshot"]
    if forced_file and forced_file not in snapshot["files"]:
        return f"❌ Selected file not found in repository: {forced_file}"
    clean_prompt = phase_1["clean_prompt"]

    # --- FETCH FILE CONTENTS ---
    # We need the raw code to build the AST. To avoid rate limits, we fetch concurrently.
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    file_contents_map = {}
    
    async def fetch_file(filepath):
        res = await client.get(f"https://api.github.com/repos/{repo}/contents/{filepath}", headers=headers)
        if res.status_code == 200:
            data = res.json()
            if data.get("encoding") == "base64":
                file_contents_map[filepath] = b64.b64decode(data["content"]).decode("utf-8", errors="replace")
                
    # Fetch the first 20 relevant files to keep things fast
    files_to_fetch = set(snapshot["files"][:20])
    if forced_file:
        files_to_fetch.add(forced_file)

    await asyncio.gather(*(fetch_file(f) for f in files_to_fetch))

    # --- PHASE 2: THE BRAIN INDEX ---
    phase_2 = await execute_devflow_phase_two(snapshot, file_contents_map)
    ast_index = phase_2["ast_index"]
    dependency_graph = phase_2["dependency_graph"]

    # Rank and trim context
    target_files = rank_and_retrieve_files(clean_prompt, ast_index, dependency_graph)
    trimmed_context = await execute_devflow_phase_two_c(clean_prompt, target_files, ast_index, file_contents_map)

    # --- PHASE 3: THE AI SURGEON ---
    if forced_file:
        execution_plan = {
            "target_file": forced_file,
            "action_type": "modify"
        }
    else:
        execution_plan = await execute_devflow_phase_three_a(clean_prompt, trimmed_context, _groq_request)
    if execution_plan.get("status") == "failed":
        return execution_plan.get("message")
    if execution_plan["target_file"] not in snapshot["files"]:
        return {
            "status": "failed",
            "message": f"AI attempted to modify unknown file: {execution_plan['target_file']}"
        }

    surgeon_result = await execute_devflow_phase_three_b(execution_plan, file_contents_map, _groq_request)
    if surgeon_result.get("status") == "failed":
        return surgeon_result.get("message")

    target_file = forced_file if forced_file else surgeon_result["target_file"]
    fixed_code = surgeon_result["fixed_code"]

    # --- PHASE 4: EXECUTION & SHIELD LOOP ---
    # We run the fast syntax check first
    shield_status = await execute_devflow_phase_four_a(fixed_code, target_file, snapshot)
    
    if shield_status["status"] == "ready_for_sandbox":
        # In a real environment, you would clone the repo to a local /tmp/workspace_dir here
        # For now, we mock the workspace dir path
        workspace_dir = f"/tmp/devflow_{repo.replace('/', '_')}" 
        
        sandbox_result = await execute_devflow_phase_four_b(
            target_file=target_file, 
            current_code=fixed_code, 
            repo_snapshot=snapshot, 
            workspace_dir=workspace_dir, 
            http_client=client
        )
    else:
        sandbox_result = shield_status # Pass the failure forward

    # --- PHASE 5: DEPLOYMENT ---
    final_result = await execute_devflow_phase_five(
        sandbox_result=sandbox_result,
        repo=repo,
        filepath=target_file,
        user_email=user_email,
        user_id=user_id,
        github_token=token,
        http_client=client
    )

    return final_result["message"]


# ── Helper: evaluate a conditional edge ──────────────────────────────
async def _evaluate_condition(condition: str, parent_output: str) -> bool:
    """Uses AI to evaluate if a condition passes, prioritizing success markers."""
    if not condition or condition in ("always", ""):
        return True # 

    # Pre-check for definitive success markers to avoid AI hallucination
    if "✅ Fixed and committed" in parent_output or "Successfully fixed" in parent_output:
        if condition == "no_errors": return True # 
        if condition == "errors_found": return False # 

    prompt = f"""You are a workflow condition evaluator.
    Parent node output: "{parent_output}"
    Edge condition: "{condition}"

    Rules:
    - If the output contains "✅ Fixed and committed", the task SUCCEEDED.
    - If it contains "Phase 1 Error" but ALSO a success URL, ignore the error.
    - "no_errors" = The code is now clean and pushed.
    - "errors_found" = The system completely failed to push code.

    Reply ONLY: true or false""" # 

    res = await _groq_request({
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 5,
        "temperature": 0
    }, timeout=10.0) # 

    if res.status_code == 200:
        answer = res.json()["choices"][0]["message"]["content"].strip().lower()
        return answer == "true" # 
    return True


# ── Helper: Get all ancestor outputs ─────────────────────────────────────────
def _get_all_ancestor_outputs(node_id: str, edges: list, all_node_outputs: dict) -> str:
    """Walk the full ancestor chain and return all outputs as one string."""
    if not node_id or not edges or not all_node_outputs:
        return " ".join(str(v) for v in all_node_outputs.values())
    visited = set()
    queue = [node_id]
    all_outputs = []
    while queue:
        nid = queue.pop(0)
        if nid in visited:
            continue
        visited.add(nid)
        parents = [e["source"] for e in edges if e["target"] == nid]
        for parent_id in parents:
            if parent_id in all_node_outputs:
                all_outputs.append(str(all_node_outputs[parent_id]))
            if parent_id not in visited:
                queue.append(parent_id)
    if not all_outputs:
        return " ".join(str(v) for v in all_node_outputs.values())
    return " ".join(all_outputs)


async def _classify_node_intent(label: str, description: str) -> str:
    prompt = f"""You are a workflow node classifier. Classify what this node should do.
Node label: {label}
Node description: {description}

Rules (in strict priority order):
- If it sends an EMAIL, has words like "alert", "notify via email", "send email", "all clear email", "error alert" → return: email
- If it analyzes, scans, inspects, reviews, fixes, or edits CODE or FILES → return: ai_code_edit  
- If it creates/reviews/merges a Pull Request or PR → return: pr
- If it creates issues, branches, or commits on GitHub (NOT email, NOT PR) → return: github
- If it explicitly mentions Slack or sends a Slack message → return: slack
- If it explicitly mentions Notion → return: notion
- If it explicitly mentions Linear → return: linear
- If it explicitly mentions Jira → return: jira
- If it is a general AI task → return: ai
- If the phrase contains "fix", "update", "check file", "scan file", "edit code" → ALWAYS return: ai_code_edit

IMPORTANT:
- "notify", "alert", "notification" WITHOUT mentioning Slack/Notion/Jira = email
- NEVER return slack unless the word "slack" is explicitly in the label or description
- NEVER return github for PR actions — use pr instead

Return ONLY one word. No explanation."""

    res = await _groq_request({
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 10,
        "temperature": 0
    }, timeout=10.0)
    if res.status_code == 200:
        return res.json()["choices"][0]["message"]["content"].strip().lower()
    return "ai"  # safe fallback


# ── Main node dispatcher ─────────────────────────────────────────────
async def _execute_node(node_type: str, node_data: dict, user_id: str, integrations: dict, context: dict) -> str:
    label = node_data.get("label", "step").lower()
    icon = node_data.get("icon", "")
    description = node_data.get("description", "")

    if node_type == "trigger":
        return f"Trigger activated: {node_data.get('label', 'Pipeline started')}"

    elif node_type == "ai":
        return await _execute_ai(node_data, context, integrations)

    elif node_type in ("action", "notification"):
        intent = await _classify_node_intent(label, description)

        if intent == "email":
            ancestor_text = _get_all_ancestor_outputs(
                context.get("current_node_id", ""),
                context.get("edges", []),
                context.get("all_node_outputs", {})
            )
            code_was_fixed = "✅ Fixed and committed" in ancestor_text
            code_was_clean = (
                "✅ No issues found" in ancestor_text or
                "no issues found" in ancestor_text.lower() or
                "no changes needed" in ancestor_text.lower() or
                "✅ no" in ancestor_text.lower()
            )

            # Use AI to decide if this is an error email or success email
            node_text = (label + " " + description).lower()
            is_error_email = any(k in node_text for k in ["error", "alert", "fail", "problem", "issue", "bug"])
            is_no_error_email = any(k in node_text for k in ["success", "all clear", "no issue", "clean", "passed", "succeeded"])

            if is_error_email and not is_no_error_email:
                if code_was_clean and not code_was_fixed:
                    return f"⏭️ Skipped '{label}' — no errors found"
                return await _execute_email(node_data, context)

            elif is_no_error_email and not is_error_email:
                if code_was_fixed and not code_was_clean:
                    return f"⏭️ Skipped '{label}' — errors were found"
                return await _execute_email(node_data, context)

            else:
                return await _execute_email(node_data, context)

        elif intent == "ai_code_edit":
            return await _execute_ai_code_edit(node_data, integrations, context)
        elif intent == "github":
            token = integrations.get("github_token")
            repo = integrations.get("selected_repo_full_name")
            client = get_http_client()

            phase_1 = await execute_devflow_phase_one(repo, token, description, client)
            if phase_1.get("status") in ["error", "failed"]:
               return phase_1.get("message")
        
            return await _execute_github(node_data, integrations, context)
        
        elif intent == "slack":
            return await _execute_slack(node_data, integrations, context)
        elif intent == "notion":
            return await _execute_notion(node_data, integrations, context)
        elif intent == "linear":
            return await _execute_linear(node_data, integrations, context)
        elif intent == "jira":
            return await _execute_jira(node_data, integrations, context)
        elif intent == "pr":
            return await _execute_pr(node_data, integrations, context)
        else:
            return await _execute_ai(node_data, context, integrations)
        

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

    client = get_http_client()
    if any(k in label for k in ["branch", "create branch"]):
        branch_name = f"devflow/{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        r = await client.get(f"https://api.github.com/repos/{repo}", headers=headers)
        r.raise_for_status()
        default_branch = r.json()["default_branch"]
        r2 = await client.get(f"https://api.github.com/repos/{repo}/git/ref/heads/{default_branch}", headers=headers)
        r2.raise_for_status()
        sha = r2.json()["object"]["sha"]
        r3 = await client.post(
            f"https://api.github.com/repos/{repo}/git/refs",
            headers=headers,
            json={"ref": f"refs/heads/{branch_name}", "sha": sha}
        )
        r3.raise_for_status()
        return f"Branch created: {branch_name} in {repo}"

    elif any(k in label for k in ["pr", "pull request", "open pr"]):
        parent_output = context.get("parent_outputs", [""])
        body = parent_output[-1] if parent_output else description or "Automated PR via DevFlow"
        r = await client.get(f"https://api.github.com/repos/{repo}", headers=headers)
        default_branch = r.json().get("default_branch", "main")
        r2 = await client.post(
            f"https://api.github.com/repos/{repo}/pulls",
            headers=headers,
            json={
                "title": f"DevFlow: {node_data.get('label', 'Automated PR')}",
                "body": body,
                "head": default_branch,
                "base": default_branch
            }
        )
        if r2.status_code in [200, 201]:
            pr = r2.json()
            return f"PR #{pr['number']} created: {pr['html_url']}"
        return f"GitHub action completed on {repo}"

    elif any(k in label for k in ["commit", "push", "upload", "file"]):
        parent_output = "\n".join(context.get("parent_outputs", []))
        content = parent_output or description or f"# Generated by DevFlow\n\nPipeline ran at {datetime.now(timezone.utc).isoformat()}"
        filename = f"devflow-output-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.md"
        encoded = b64.b64encode(content.encode()).decode()
        r = await client.put(
            f"https://api.github.com/repos/{repo}/contents/{filename}",
            headers=headers,
            json={
                "message": f"DevFlow: {node_data.get('label', 'automated commit')}",
                "content": encoded
            }
        )
        if r.status_code in [200, 201]:
            return f"Committed {filename} to {repo}"
        raise Exception(f"GitHub commit failed: {r.json().get('message', r.status_code)}")

    else:
        parent_output = "\n".join(context.get("parent_outputs", []))
        body = parent_output or description or "Automated issue created by DevFlow pipeline"
        r = await client.post(
            f"https://api.github.com/repos/{repo}/issues",
            headers=headers,
            json={
                "title": node_data.get("label", "DevFlow Issue"),
                "body": body,
                "labels": ["devflow", "automated"]
            }
        )
        if r.status_code == 201:
            issue = r.json()
            return f"Issue #{issue['number']} created: {issue['html_url']}"
        raise Exception(f"GitHub API error: {r.json().get('message', r.status_code)}")


# ── Other executors ───────────────────────────────────────────────
async def _execute_ai(node_data: dict, context: dict, integrations: dict) -> str:
    description = node_data.get("description", "")
    label = node_data.get("label", "AI Step")
    parent_output = "\n".join(context.get("parent_outputs", []))
    model = node_data.get("model", "groq")

    prompt = f"""You are an AI step in a workflow pipeline.
Previous step output: {parent_output or 'None'}
Your task: {description or label}
Respond with a concise, actionable result (2-4 sentences max)."""

    client = get_http_client()
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
        res = await _groq_request({
            "model": "llama-3.3-70b-versatile",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 300
        }, timeout=20.0)
        res.raise_for_status()
        return res.json()["choices"][0]["message"]["content"].strip()


async def _execute_slack(node_data: dict, integrations: dict, context: dict) -> str:
    webhook_url = integrations.get("slack_webhook_url")
    label = node_data.get("label", "Notification")
    parent_output = "\n".join(context.get("parent_outputs", []))
    message = parent_output or node_data.get("description", "") or f"DevFlow pipeline step: {label}"

    if not webhook_url:
        raise Exception("Slack not connected. Go to Integrations → Connect Slack.")

    client = get_http_client()
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

    client = get_http_client()
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

    label = node_data.get("label", "Issue")
    description = node_data.get("description", "") or "\n".join(context.get("parent_outputs", []))

    client = get_http_client()
    auth = b64.b64encode(f"devflow:{token}".encode()).decode()
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


# ── Workflow execution (parallel) ──────────────────────────────────
async def execute_workflow(nodes: list, edges: list, user_id: str, context: dict = {}) -> dict:
    start = datetime.now(timezone.utc)
    logs = []
    status = "success"
    integrations = await get_user_integrations(user_id)

    node_map = {n["id"]: n for n in nodes}
    adj: dict[str, list[dict]] = {n["id"]: [] for n in nodes}
    for edge in edges:
        adj[edge["source"]].append(edge)

    has_incoming = {e["target"] for e in edges}
    roots = [n["id"] for n in nodes if n["id"] not in has_incoming]
    if not roots:
        roots = [nodes[0]["id"]] if nodes else []

    # Calculate depths for parallel execution
    depths = {nid: 0 for nid in roots}
    queue = list(roots)
    while queue:
        curr = queue.pop(0)
        for edge in adj[curr]:
            target = edge["target"]
            depths[target] = max(depths.get(target, 0), depths[curr] + 1)
            if target not in queue:
                queue.append(target)

    # Group nodes by depth
    levels = {}
    for nid, d in depths.items():
        levels.setdefault(d, []).append(nid)

    node_outputs: dict[str, str] = {}
    skipped_nodes: set[str] = set()

    for d in sorted(levels.keys()):
        level_nodes = levels[d]
        
        async def run_node(nid):
            node = node_map.get(nid)
            if not node: return
            
            node_data = node.get("data", {})
            node_type = node_data.get("type", node.get("type", "action"))
            label = node_data.get("label", "Unknown Step")

            # Check conditions
            incoming_edges = [e for e in edges if e["target"] == nid]
            should_skip = False
            for edge in incoming_edges:
                if edge["source"] in skipped_nodes:
                    should_skip = True; break
                condition = edge.get("condition", "always")
                if condition and condition != "always":
                    parent_out = node_outputs.get(edge["source"], "")
                    if not await _evaluate_condition(condition, parent_out):
                        should_skip = True; break
            
            if should_skip:
                skipped_nodes.add(nid)
                logs.append({
                    "node_id": nid, "node_label": label, "type": node_type,
                    "status": "skipped", "message": "⏭️ Skipped — condition not met",
                    "duration": "0.0s", "timestamp": datetime.now(timezone.utc).isoformat()
                })
                return

            parent_outputs = [node_outputs[e["source"]] for e in edges if e["target"] == nid and e["source"] in node_outputs]
            node_context = {**context, "parent_outputs": parent_outputs, "all_node_outputs": node_outputs, "edges": edges, "current_node_id": nid, "snapshot": context.get("snapshot")}
            
            t_start = datetime.now(timezone.utc)
            try:
                result = await _execute_node(node_type, node_data, user_id, integrations, node_context)
                node_outputs[nid] = result
                logs.append({
                    "node_id": nid, "node_label": label, "type": node_type,
                    "status": "success", "message": result,
                    "duration": f"{(datetime.now(timezone.utc) - t_start).total_seconds():.1f}s",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
            except Exception as e:
                nonlocal status
                status = "failed"
                logs.append({
                    "node_id": nid, "node_label": label, "type": node_type,
                    "status": "failed", "message": str(e),
                    "duration": f"{(datetime.now(timezone.utc) - t_start).total_seconds():.1f}s",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
                return "STOP"

        tasks = [run_node(nid) for nid in level_nodes]
        results = await asyncio.gather(*tasks)
        if "STOP" in results:
            break

    end = datetime.now(timezone.utc)
    secs = (end - start).total_seconds()
    return {
        "status": status,
        "duration": f"{int(secs // 60)}m {int(secs % 60)}s" if secs >= 60 else f"{secs:.1f}s",
        "logs": sorted(logs, key=lambda x: x["timestamp"]),
        "started_at": start.isoformat(),
        "finished_at": end.isoformat()
    }


# ── WebSocket-aware executor (parallel) ───────────────────────────────
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
    adj: dict[str, list[dict]] = {n["id"]: [] for n in nodes}
    for edge in edges:
        adj[edge["source"]].append(edge)

    has_incoming = {e["target"] for e in edges}
    roots = [n["id"] for n in nodes if n["id"] not in has_incoming]
    if not roots:
        roots = [nodes[0]["id"]] if nodes else []

    # Calculate depths
    depths = {nid: 0 for nid in roots}
    queue = list(roots)
    while queue:
        curr = queue.pop(0)
        for edge in adj[curr]:
            target = edge["target"]
            depths[target] = max(depths.get(target, 0), depths[curr] + 1)
            if target not in queue:
                queue.append(target)

    levels = {}
    for nid, d in depths.items():
        levels.setdefault(d, []).append(nid)

    node_outputs: dict[str, str] = {}
    skipped_nodes: set[str] = set()

    for d in sorted(levels.keys()):
        level_nodes = levels[d]
        
        async def run_node(nid):
            node = node_map.get(nid)
            if not node: return
            
            node_data = node.get("data", {})
            node_type = node_data.get("type", node.get("type", "action"))
            label = node_data.get("label", "Unknown Step")

            if on_node_complete:
                await on_node_complete({
                    "node_id": nid, "node_label": label, "type": node_type,
                    "status": "running", "message": "Executing...",
                    "duration": None, "timestamp": datetime.now(timezone.utc).isoformat()
                })

            incoming_edges = [e for e in edges if e["target"] == nid]
            should_skip = False
            for edge in incoming_edges:
                if edge["source"] in skipped_nodes:
                    should_skip = True; break
                condition = edge.get("condition", "always")
                if condition and condition != "always":
                    parent_out = node_outputs.get(edge["source"], "")
                    if not await _evaluate_condition(condition, parent_out):
                        should_skip = True; break
            
            if should_skip:
                skipped_nodes.add(nid)
                log_entry = {
                    "node_id": nid, "node_label": label, "type": node_type,
                    "status": "skipped", "message": "⏭️ Skipped — condition not met",
                    "duration": "0.0s", "timestamp": datetime.now(timezone.utc).isoformat()
                }
                logs.append(log_entry)
                if on_node_complete: await on_node_complete(log_entry)
                return

            parent_outputs = [node_outputs[e["source"]] for e in edges if e["target"] == nid and e["source"] in node_outputs]
            node_context = {**context, "parent_outputs": parent_outputs, "all_node_outputs": node_outputs, "edges": edges, "current_node_id": nid, "snapshot": context.get("snapshot")}
            
            t_start = datetime.now(timezone.utc)
            try:
                result = await _execute_node(node_type, node_data, user_id, integrations, node_context)
                node_outputs[nid] = result
                log_entry = {
                    "node_id": nid, "node_label": label, "type": node_type,
                    "status": "success", "message": result,
                    "duration": f"{(datetime.now(timezone.utc) - t_start).total_seconds():.1f}s",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                logs.append(log_entry)
                if on_node_complete: await on_node_complete(log_entry)
            except Exception as e:
                nonlocal status
                status = "failed"
                log_entry = {
                    "node_id": nid, "node_label": label, "type": node_type,
                    "status": "failed", "message": str(e),
                    "duration": f"{(datetime.now(timezone.utc) - t_start).total_seconds():.1f}s",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                logs.append(log_entry)
                if on_node_complete: await on_node_complete(log_entry)
                return "STOP"

        tasks = [run_node(nid) for nid in level_nodes]
        results = await asyncio.gather(*tasks)
        if "STOP" in results:
            break

    end = datetime.now(timezone.utc)
    secs = (end - start).total_seconds()
    return {
        "status": status,
        "duration": f"{int(secs // 60)}m {int(secs % 60)}s" if secs >= 60 else f"{secs:.1f}s",
        "logs": sorted(logs, key=lambda x: x["timestamp"]),
        "started_at": start.isoformat(),
        "finished_at": end.isoformat()
    }

async def _execute_pr(node_data: dict, integrations: dict, context: dict) -> str:
    token = integrations.get("github_token")
    repo = integrations.get("selected_repo_full_name")
    
    if not token or not repo:
        raise Exception("GitHub not connected or no repo selected.")
    
    label = node_data.get("label", "").lower()
    description = node_data.get("description", "").lower()
    combined = label + " " + description
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    client = get_http_client()
    parent_output = "\n".join(context.get("parent_outputs", []))

    # Get default branch
    branch_cache_key = f"branch:{repo}"
    default_branch = cache_get(branch_cache_key)
    if not default_branch:
        r = await client.get(f"https://api.github.com/repos/{repo}", headers=headers)
        default_branch = r.json().get("default_branch", "main") if r.status_code == 200 else "main"
        cache_set(branch_cache_key, default_branch)

    # ── MERGE PR ─────────────────────────────────────────────────
    if any(k in combined for k in ["merge", "auto merge", "automerge"]):
        # Get open PRs
        prs_res = await client.get(
            f"https://api.github.com/repos/{repo}/pulls?state=open",
            headers=headers
        )
        if prs_res.status_code != 200 or not prs_res.json():
            return "No open PRs found to merge"
        
        pr = prs_res.json()[0]  # merge most recent
        pr_number = pr["number"]
        
        merge_res = await client.put(
            f"https://api.github.com/repos/{repo}/pulls/{pr_number}/merge",
            headers=headers,
            json={
                "commit_title": f"DevFlow AI: Auto-merged PR #{pr_number}",
                "merge_method": "squash"
            }
        )
        if merge_res.status_code == 200:
            return f"✅ Merged PR #{pr_number}: {pr['title']}"
        raise Exception(f"Merge failed: {merge_res.json().get('message')}")

    # ── REVIEW PR ────────────────────────────────────────────────
    elif any(k in combined for k in ["review", "check pr", "inspect pr"]):
        prs_res = await client.get(
            f"https://api.github.com/repos/{repo}/pulls?state=open",
            headers=headers
        )
        if prs_res.status_code != 200 or not prs_res.json():
            return "No open PRs found to review"
        
        pr = prs_res.json()[0]
        pr_number = pr["number"]
        
        # Get PR diff
        diff_res = await client.get(
            f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files",
            headers=headers
        )
        files_changed = diff_res.json() if diff_res.status_code == 200 else []
        diff_summary = "\n".join([
            f"File: {f['filename']} (+{f['additions']} -{f['deletions']})\n{f.get('patch', '')[:500]}"
            for f in files_changed[:5]
        ])

        # AI review
        review_prompt = f"""You are an expert code reviewer.
PR Title: {pr['title']}
PR Description: {pr.get('body', 'No description')}

Files changed:
{diff_summary}

Provide a concise code review (3-5 bullet points). 
End with either APPROVE or REQUEST_CHANGES."""

        res = await _groq_request({
            "model": "llama-3.3-70b-versatile",
            "messages": [{"role": "user", "content": review_prompt}],
            "max_tokens": 500,
            "temperature": 0.3
        })

        review_body = res.json()["choices"][0]["message"]["content"].strip()
        approved = "APPROVE" in review_body

        # Post review to GitHub
        await client.post(
            f"https://api.github.com/repos/{repo}/pulls/{pr_number}/reviews",
            headers=headers,
            json={
                "body": f"**DevFlow AI Review:**\n\n{review_body}",
                "event": "APPROVE" if approved else "REQUEST_CHANGES"
            }
        )

        return f"{'✅ Approved' if approved else '⚠️ Changes requested'} PR #{pr_number}: {pr['title']}\n\n{review_body}"

    # ── CREATE PR (default) ───────────────────────────────────────
    else:
        # Create new branch
        pr_branch = f"devflow/fix-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        
        # Get HEAD sha
        ref_res = await client.get(
            f"https://api.github.com/repos/{repo}/git/ref/heads/{default_branch}",
            headers=headers
        )
        if ref_res.status_code != 200:
            raise Exception("Could not get branch reference")
        
        head_sha = ref_res.json()["object"]["sha"]
        
        # Create branch
        await client.post(
            f"https://api.github.com/repos/{repo}/git/refs",
            headers=headers,
            json={"ref": f"refs/heads/{pr_branch}", "sha": head_sha}
        )

        import base64 as b64
        await client.put(
            f"https://api.github.com/repos/{repo}/contents/devflow-pr-update.md",
            headers=headers,
            json={
                "message": "DevFlow AI: Initialize PR branch",
                "content": b64.b64encode(b"Automated PR placeholder.").decode(),
                "branch": pr_branch
            }
        )

        # AI generates PR title and description
        pr_prompt = f"""Generate a pull request title and description based on this context:
{parent_output or description or 'Code improvements by DevFlow AI'}

Respond in this exact format:
TITLE: <one line title>
DESCRIPTION: <2-3 sentence description of changes>"""

        pr_res = await _groq_request({
            "model": "llama-3.3-70b-versatile",
            "messages": [{"role": "user", "content": pr_prompt}],
            "max_tokens": 200,
            "temperature": 0.4
        })

        pr_content = pr_res.json()["choices"][0]["message"]["content"].strip()
        title_match = re.search(r'TITLE:\s*(.+)', pr_content)
        desc_match = re.search(r'DESCRIPTION:\s*(.+)', pr_content, re.DOTALL)

        pr_title = title_match.group(1).strip() if title_match else f"DevFlow AI: {node_data.get('label', 'improvements')}"
        pr_body = desc_match.group(1).strip() if desc_match else parent_output or "Automated improvements by DevFlow AI"

        # Create PR
        create_res = await client.post(
            f"https://api.github.com/repos/{repo}/pulls",
            headers=headers,
            json={
                "title": pr_title,
                "body": f"{pr_body}\n\n---\n*Created automatically by DevFlow AI*",
                "head": pr_branch,
                "base": default_branch
            }
        )

        if create_res.status_code == 201:
            pr = create_res.json()
            return f"✅ PR #{pr['number']} created: {pr['html_url']}\n\nTitle: {pr_title}\n{pr_body}"
        
        raise Exception(f"PR creation failed: {create_res.json().get('message')}")
    
async def execute_devflow_phase_one(repo: str, token: str, raw_prompt: str, http_client):
    try:
        # 1. Fetch the context
        repo_snapshot = await build_repo_snapshot(repo, token, http_client)
        
        # If the snapshot is just a string, it's an error message from GitHub
        if isinstance(repo_snapshot, str):
            return {"status": "failed", "message": f"GitHub Error: {repo_snapshot}"}
            
    except Exception as e:
        return {"status": "error", "message": f"System Error: {str(e)}"}

    # 2. Now, sanitize the prompt
    clean_prompt = sanitize_prompt(raw_prompt)

    # ── GHOST FILE DETECTION ─────────────────────────────
    repo_files = repo_snapshot.get("files", [])

    file_match = re.findall(r'[\w\/\.-]+\.[a-zA-Z]+', clean_prompt)

    missing_files = [
        f for f in file_match
        if not any(r.endswith(f.split("/")[-1]) for r in repo_files)
    ]

    if missing_files:
        return {
            "status": "failed",
            "message": f"❌ File not found in repository: {missing_files[0]}"
        }
    
    # 3. Safely extract files (only if repo_snapshot is a list of dictionaries)
    repo_files = []
    if isinstance(repo_snapshot, list):
        repo_files = repo_snapshot.get("files", [])
    
    # 4. Parse intent with the verified file list
    intent = parse_intent(clean_prompt, repo_files=repo_files)

    # 5. Handle Logic Errors (Missing files, unknown actions)
    if intent.get("action") == "error":
        return {"status": "failed", "message": intent.get("message")}
    
    if intent.get("action") == "unknown":
        return {"status": "error", "message": "Could not determine action from prompt."}

    return {
        "status": "success",
        "clean_prompt": clean_prompt,
        "intent": intent,
        "snapshot": repo_snapshot
    }
async def execute_devflow_phase_two(repo_snapshot: dict, file_contents_map: dict):
    # file_contents_map is a dict of {"path/to/file.py": "raw code string..."}
    
    ast_index = []
    
    # 1. Parse all relevant files into the AST Index
    for file_path in repo_snapshot["files"]:
        content = file_contents_map.get(file_path, "")
        if content:
            ast_data = extract_ast_data(file_path, content)
            ast_index.append(ast_data)
            
    # 2. Build the exact Dependency Graph
    dependency_graph = build_dependency_graph(ast_index, repo_snapshot["files"])
    
    return {
        "ast_index": ast_index,
        "dependency_graph": dependency_graph
    }

async def execute_devflow_phase_two_c(clean_prompt: str, target_files: list, ast_index: list, file_contents_map: dict):
    
    # Trim the fat. This turns 10,000 tokens of raw code into ~500 tokens of pure context.
    optimized_payload = trim_code_context(
        target_files=target_files,
        file_contents_map=file_contents_map,
        ast_index=ast_index,
        clean_prompt=clean_prompt
    )
    
    return optimized_payload

async def execute_devflow_phase_three_a(clean_prompt: str, trimmed_context: dict, _groq_request):
    
    # The Planner decides exactly what to do (Cheap AI Call)
    execution_plan = await execute_ai_planner(
        clean_prompt=clean_prompt,
        trimmed_context=trimmed_context,
        _groq_request_func=_groq_request
    )
    
    if "error" in execution_plan:
        return {"status": "failed", "message": execution_plan["error"]}
        
    return execution_plan

async def execute_devflow_phase_three_b(execution_plan: dict, file_contents_map: dict, _groq_request):
    
    target_file = execution_plan.get("target_file")
    
    # Grab the original full code for the targeted file
    original_code = file_contents_map.get(target_file, "")
    
    if not original_code and execution_plan.get("action_type") != "create":
        return {"status": "failed", "message": f"Target file {target_file} is empty or missing."}

    # The Executor writes the actual fix (Heavy AI Call)
    fixed_code = await execute_ai_coder(
        execution_plan=execution_plan,
        original_file_content=original_code,
        _groq_request_func=_groq_request
    )
    
    return {
        "target_file": target_file,
        "fixed_code": fixed_code,
        "original_code": original_code
    }

async def execute_devflow_phase_four_a(fixed_code: str, target_file: str, repo_snapshot: dict):
    
    # 1. Zero-Cost Syntax Shield
    is_valid, error_msg = local_syntax_check(target_file, fixed_code)
    
    if not is_valid:
        # BOUNCE TO FREE RETRY LOOP: Code is broken!
        return {
            "status": "failed", 
            "reason": "syntax_error", 
            "console_log": error_msg
        }
        
    # 2. Setup the Docker Sandbox environment
    sandbox_config = detect_manifest(repo_snapshot["files"])
    
    return {
        "status": "ready_for_sandbox",
        "sandbox_config": sandbox_config
    }

async def execute_devflow_phase_four_b(target_file: str, current_code: str, repo_snapshot: dict, workspace_dir: str, http_client):
    
    max_retries = 2
    attempts = 0
    
    sandbox_config = detect_manifest(repo_snapshot["files"])
    
    while attempts <= max_retries:
        # 1. Zero-Cost Syntax Shield
        is_valid, error_msg = local_syntax_check(target_file, current_code)
        
        if not is_valid:
            sandbox_result = {"status": "failed", "console_log": error_msg}
        else:
            # 2. Run the secure Sandbox
            # (Assume current_code has been written to workspace_dir/target_file before running)
            sandbox_result = execute_docker_sandbox(workspace_dir, sandbox_config)
            
        # 3. Check Results
        if sandbox_result["status"] == "success":
            return {"status": "success", "final_code": current_code}
            
        # 4. If failed, trigger the Free Retry Loop (unless we hit the max attempts)
        attempts += 1
        if attempts <= max_retries:
            print(f"Attempt {attempts} failed. Triggering Free Retry Loop...")
            current_code = await execute_free_retry(
                target_file=target_file,
                broken_code=current_code,
                error_log=sandbox_result["console_log"],
                http_client=http_client
            )
            
            # If OpenRouter is down or returned nothing, break the loop
            if not current_code:
                break
                
    # If we exit the loop, the AI completely failed to fix the code
    return {"status": "total_failure", "console_log": sandbox_result.get("console_log", "Unknown error")}

async def execute_devflow_phase_five(sandbox_result: dict, repo: str, filepath: str, user_email: str, user_id: str, github_token: str, http_client):
    
    # 1. Handle the Outcome
    if sandbox_result["status"] == "success":
        # WIN: Commit the code
        final_code = sandbox_result["final_code"]
        deploy_res = await commit_to_github(repo, filepath, final_code, github_token, http_client)
        
        # Log to PostHog
        await log_observability_event(user_id, "ai_fix_success", {"repo": repo, "file": filepath}, http_client)
        
        return {"status": "completed", "message": f"✅ Fixed and committed {filepath} to {repo}\n{deploy_res.get('url')}"}
        
    else:
        # LOSS: Trigger the Fallback Email
        error_log = sandbox_result.get("console_log", "Unknown testing error.")
        await send_fallback_email(user_email, filepath, error_log, http_client)
        
        # Log to PostHog
        await log_observability_event(user_id, "ai_fix_failed", {"repo": repo, "file": filepath, "error": error_log}, http_client)
        
        return {"status": "aborted", "message": f"Sandbox tests failed. Original code preserved. Incident report sent to {user_email}."}