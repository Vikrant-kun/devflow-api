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

_TOOL_MEMORY = {}
_REPO_INDEX_CACHE = {}
_REPO_TREE_CACHE = {}
_EXECUTION_REFLECTION = {}

REPO_INDEX_TTL = 300
REPO_TREE_TTL = 120
REFLECTION_TTL = 3600

def _get_cached_repo_tree(repo: str, tree_fetcher):

    now = time.time()

    cached = _REPO_TREE_CACHE.get(repo)

    if cached and now - cached["ts"] < REPO_TREE_TTL:
        return cached["tree"]

    tree = tree_fetcher()

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

async def _plan_code_task(description: str, repo: str, code_files: list) -> dict:
    """
    Planner AI — decides WHAT to do before execution.
    Returns a structured plan with exact filenames from the repo.
    """

    file_list = "\n".join(code_files[:60])

    planner_prompt = f"""You are a software automation planner.

User task: {description}

Repository: {repo}

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
            plan["target_files"]=[code_files[0]]

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
    token = integrations.get("github_token")
    repo = integrations.get("selected_repo_full_name")
    if not token or not repo:
        raise Exception("GitHub not connected or no repo selected.")

    description = (node_data.get("description") or node_data.get("label") or "").strip()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}

    filters = extract_file_filters(description)

    client = get_http_client()

    async def fetch_tree():
        tree_url = f"https://api.github.com/repos/{repo}/git/trees/HEAD?recursive=1"
        repo_url = f"https://api.github.com/repos/{repo}"
        
        tree_res, repo_res = await asyncio.gather(
            client.get(tree_url, headers=headers),
            client.get(repo_url, headers=headers)
        )

        if tree_res.status_code != 200:
            raise Exception(f"Cannot access repository tree: {tree_res.json().get('message', tree_res.status_code)}")
        
        t = tree_res.json().get("tree", [])
        if repo_res.status_code == 200:
            cache_set(f"branch:{repo}", repo_res.json().get("default_branch", "main"))
        return t

    tree = await _get_cached_repo_tree(repo, fetch_tree)

    code_files = [
        f["path"] for f in tree
        if f["type"] == "blob"
        and any(f["path"].lower().endswith(ext) for ext in [".py", ".js", ".jsx", ".ts", ".tsx", ".css", ".html", ".json", ".yaml", ".yml"])
        and not any(skip in f["path"].lower() for skip in ["node_modules/", "dist/", "build/", ".min.", "vendor/", "__pycache__/", "venv/"])
    ]

    if not code_files:
        raise Exception("No relevant code files found in the repository.")

    file_map = {f["path"]: "" for f in tree if f["type"] == "blob"}
    repo_index = _get_repo_index(repo, file_map)
    ranked_files = _rank_files_by_query(description, repo_index)
    failed_files = _reflection_penalty(repo, description)
    ranked_files = [f for f in ranked_files if f not in failed_files]

    filtered_files = match_files(code_files, filters)

    if ranked_files:
        filtered_files = [f for f in ranked_files if f in filtered_files] or filtered_files

    print(f"DEBUG INDEX RANKED FILES: {ranked_files[:5]}")
    print(f"DEBUG REFLECTION AVOID FILES: {failed_files}")

    plan = await _plan_code_task(description, repo, filtered_files)
    print(f"DEBUG PLANNER: selected={plan['target_files']} | plan={plan.get('summary')}")

    results = []
    for selected_path in plan["target_files"][:3]:
        content_res = await client.get(
            f"https://api.github.com/repos/{repo}/contents/{selected_path}",
            headers=headers
        )
        if content_res.status_code != 200:
            msg = f"Cannot read file {selected_path}: {content_res.json().get('message')}"
            results.append(msg)
            _record_reflection(repo, description, selected_path, "failed - " + msg)
            continue

        file_data = content_res.json()
        if file_data.get("encoding") != "base64":
            msg = f"Unexpected encoding for {selected_path}"
            results.append(msg)
            _record_reflection(repo, description, selected_path, "failed - " + msg)
            continue

        original_content = b64.b64decode(file_data["content"]).decode("utf-8", errors="replace")
        sha = file_data["sha"]

        if len(original_content) > 180_000:
            msg = f"Skipped {selected_path} — file too large ({len(original_content)//1000} kB)"
            results.append(msg)
            _record_reflection(repo, description, selected_path, msg)
            continue

        context_code = _smart_chunk_file(original_content, description)

        fix_prompt = (
            f"You are an expert code reviewer and fixer.\n"
            f"File: {selected_path}\n\n"
            f"Task / context: {description or 'Find bugs, security issues, performance problems, bad patterns and fix them'}\n\n"
            f"Return ONLY the complete fixed code — no explanations, no markdown fences, no comments about changes.\n"
            f"If the code is already good, return it unchanged.\n\n"
            f"Code context:\n{context_code}"
        )

        fix_res = await _groq_request({
            "model": "llama-3.3-70b-versatile",
            "messages": [{"role": "user", "content": fix_prompt}],
            "max_tokens": 8192,
            "temperature": 0
        }, timeout=60.0)

        if fix_res.status_code != 200:
            msg = f"AI fix request failed for {selected_path}: {fix_res.status_code}"
            results.append(msg)
            _record_reflection(repo, description, selected_path, "failed - " + msg)
            continue

        fixed_code = fix_res.json()["choices"][0]["message"]["content"].strip()
        fixed_code = re.sub(r'^```[\w]*\n?', '', fixed_code)
        fixed_code = re.sub(r'\n?```$', '', fixed_code)

        if fixed_code.strip() == original_content.strip():
            msg = f"✅ No issues found / no changes needed in {selected_path}"
            results.append(msg)
            _record_reflection(repo, description, selected_path, msg)
            continue

        is_valid, critic_msg = await _critic_validate_fix(original_content, fixed_code, selected_path)
        if not is_valid:
            results.append(critic_msg)
            _record_reflection(repo, description, selected_path, "failed - " + critic_msg)
            continue

        # Basic syntax check
        if selected_path.endswith('.py'):
            import ast
            try:
                ast.parse(fixed_code)
            except SyntaxError as e:
                msg = f"⚠️ Python syntax error in AI fix for {selected_path}: {e} — original preserved"
                results.append(msg)
                _record_reflection(repo, description, selected_path, "failed - syntax")
                continue

        if selected_path.endswith('.js') or selected_path.endswith('.jsx'):
            # check for obvious JS issues
            js_issues = []
            open_braces = fixed_code.count('{') - fixed_code.count('}')
            open_parens = fixed_code.count('(') - fixed_code.count(')')
            if abs(open_braces) > 2:
                js_issues.append(f"unbalanced braces ({open_braces:+d})")
            if abs(open_parens) > 2:
                js_issues.append(f"unbalanced parens ({open_parens:+d})")
            if js_issues:
                msg = f"⚠️ JS syntax issues in AI fix for {selected_path}: {', '.join(js_issues)} — original preserved"
                results.append(msg)
                _record_reflection(repo, description, selected_path, "failed - syntax")
                continue

        encoded = b64.b64encode(fixed_code.encode("utf-8")).decode("utf-8")

        branch_cache_key = f"branch:{repo}"
        default_branch = cache_get(branch_cache_key)
        if not default_branch:
            branch_res = await client.get(f"https://api.github.com/repos/{repo}", headers=headers)
            default_branch = branch_res.json().get("default_branch", "main") if branch_res.status_code == 200 else "main"
            cache_set(branch_cache_key, default_branch)

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
            err_body = commit_res.json()
            msg = f"Commit failed ({commit_res.status_code}): {err_body.get('message')} | repo={repo} | file={selected_path}"
            results.append(msg)
            _record_reflection(repo, description, selected_path, "failed - commit error")
            continue

        # Auto PR if description mentions "pr" or "pull request"
        pr_url = ""
        if any(k in description.lower() for k in ["pr", "pull request", "open pr", "create pr"]):
            pr_branch = f"devflow/fix-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
            # create branch
            ref_res = await client.get(
                f"https://api.github.com/repos/{repo}/git/ref/heads/{default_branch}",
                headers=headers
            )
            if ref_res.status_code == 200:
                sha_branch = ref_res.json()["object"]["sha"]
                await client.post(
                    f"https://api.github.com/repos/{repo}/git/refs",
                    headers=headers,
                    json={"ref": f"refs/heads/{pr_branch}", "sha": sha_branch}
                )
                pr_res = await client.post(
                    f"https://api.github.com/repos/{repo}/pulls",
                    headers=headers,
                    json={
                        "title": f"DevFlow AI: fixes in {selected_path}",
                        "body": f"Auto-generated PR by DevFlow AI\n\nFixed: `{selected_path}`",
                        "head": pr_branch,
                        "base": default_branch
                    }
                )
                if pr_res.status_code == 201:
                    pr_url = f"\n🔀 PR created: {pr_res.json()['html_url']}"

        _remember_tool(repo, selected_path)
        print(f"DEBUG CRITIC APPROVED: {selected_path}")
        success_msg = f"✅ Fixed and committed {selected_path} to {repo}/{default_branch}{pr_url}"
        results.append(success_msg)
        _record_reflection(repo, description, selected_path, success_msg)

    return "\n".join(results)


# ── Helper: evaluate a conditional edge ──────────────────────────────
async def _evaluate_condition(condition: str, parent_output: str) -> bool:
    """Use AI to evaluate if a condition passes based on parent output."""
    if not condition or condition in ("always", ""):
        return True

    prompt = f"""You are a workflow condition evaluator.

Parent node output:
"{parent_output}"

Edge condition to evaluate:
"{condition}"

Important context:
- "✅ Fixed and committed" means errors WERE found and fixed → errors exist
- "✅ No issues found" means code was clean → no errors
- "no changes needed" means code was clean → no errors

Does the parent output satisfy this condition?
Reply with ONLY: true or false"""

    res = await _groq_request({
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 5,
        "temperature": 0
    }, timeout=10.0)
    if res.status_code == 200:
        answer = res.json()["choices"][0]["message"]["content"].strip().lower()
        return answer == "true"
    return True  # safe fallback


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
            node_context = {**context, "parent_outputs": parent_outputs, "all_node_outputs": node_outputs, "edges": edges, "current_node_id": nid}
            
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
            node_context = {**context, "parent_outputs": parent_outputs, "all_node_outputs": node_outputs, "edges": edges, "current_node_id": nid}
            
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