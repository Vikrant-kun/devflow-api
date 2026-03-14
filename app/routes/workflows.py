import json
import httpx
from fastapi import APIRouter, Depends, HTTPException
from app.auth import get_current_user
from app.database import query, query_one
from app.models.workflow import SaveWorkflowRequest, RunWorkflowRequest, GenerateWorkflowRequest
from app.services.executor import execute_workflow
from app.services.parser import sanitize_prompt, parse_intent  # ← FIX 5: was missing
from app.config import settings
import re

router = APIRouter(prefix="/workflows", tags=["workflows"])

@router.get("/")
async def list_workflows(user: dict = Depends(get_current_user)):
    rows = query(
        "SELECT * FROM workflows WHERE user_id = %s ORDER BY created_at DESC",
        (user["user_id"],)
    )
    return {"workflows": rows}

@router.get("/{workflow_id}")
async def get_workflow(workflow_id: str, user: dict = Depends(get_current_user)):
    row = query_one(
        "SELECT * FROM workflows WHERE id = %s AND user_id = %s",
        (workflow_id, user["user_id"])
    )
    if not row:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return row

@router.post("/")
async def save_workflow(body: SaveWorkflowRequest, user: dict = Depends(get_current_user)):
    row = query_one(
        """INSERT INTO workflows (user_id, name, nodes, edges, status)
           VALUES (%s, %s, %s, %s, %s) RETURNING *""",
        (user["user_id"], body.name, json.dumps(body.nodes), json.dumps(body.edges), body.status or "draft")
    )
    if not row:
        raise HTTPException(status_code=500, detail="Failed to save workflow")
    return row

@router.put("/{workflow_id}")
async def update_workflow(workflow_id: str, body: SaveWorkflowRequest, user: dict = Depends(get_current_user)):
    row = query_one(
        """UPDATE workflows SET name=%s, nodes=%s, edges=%s, status=%s, updated_at=NOW()
           WHERE id=%s AND user_id=%s RETURNING *""",
        (body.name, json.dumps(body.nodes), json.dumps(body.edges), body.status, workflow_id, user["user_id"])
    )
    if not row:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return row

@router.delete("/{workflow_id}")
async def delete_workflow(workflow_id: str, user: dict = Depends(get_current_user)):
    query("DELETE FROM workflows WHERE id=%s AND user_id=%s", (workflow_id, user["user_id"]))
    return {"deleted": True}

@router.delete("/")
async def delete_all_workflows(user: dict = Depends(get_current_user)):
    query("DELETE FROM workflows WHERE user_id=%s", (user["user_id"],))
    return {"deleted": True}

@router.post("/run")
async def run_workflow(body: RunWorkflowRequest, user: dict = Depends(get_current_user)):
    raw_prompt = getattr(body.snapshot, "prompt", "")

    # Phase 1: Clean and Parse (Zero AI Cost)
    clean_prompt = sanitize_prompt(raw_prompt)
    intent = parse_intent(clean_prompt)

    # Fast-Fail Logic
    if intent["action"] == "unknown" and intent["target"] == "unknown":
        return {
            "status": "failed",
            "message": "Could not understand the objective. Please specify if you want to fix, scan, or create code.",
            "logs": []
        }

    # FIX 4: Only call execute_workflow once with the correct context
    result = await execute_workflow(
        nodes=body.snapshot.nodes,
        edges=body.snapshot.edges,
        user_id=user["user_id"],
        context={"prompt": clean_prompt, "intent": intent}
    )

    run_row = query_one(
        """INSERT INTO workflow_runs (user_id, workflow_id, workflow_name, status, started_at, duration, triggered_by, snapshot, logs)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
        (user["user_id"], body.workflow_id, body.workflow_name, result["status"],
         result["started_at"], result["duration"], "manual",
         json.dumps(body.snapshot.model_dump()), json.dumps(result["logs"]))
    )
    return {
        "run_id": run_row["id"] if run_row else None,
        "status": result["status"],
        "duration": result["duration"],
        "logs": result["logs"]
    }

@router.post("/generate")
async def generate_workflow(body: GenerateWorkflowRequest, user: dict = Depends(get_current_user)):

    # FIX 1: Hard block — if no file selected, reject immediately
    if not body.selected_files:
        raise HTTPException(
            status_code=400,
            detail="⚠️ Please select a file from your repository before generating a pipeline."
        )

    # FIX 2: Extract the path string correctly, not the whole dict
    selected_file = body.selected_files[0]
    if isinstance(selected_file, dict):
        selected_file_path = selected_file.get("path") or selected_file.get("name") or ""
    else:
        selected_file_path = str(selected_file)

    selected_file_hint = f"\n\nSELECTED FILE (YOU MUST USE THIS EXACT PATH — NO EXCEPTIONS): {selected_file_path}"

    # Fetch real repo files
    repo_context = ""
    real_files = []

    try:
        settings_row = query_one(
            "SELECT github_token, selected_repo_full_name FROM user_settings WHERE user_id = %s",
            (user["user_id"],)
        )
        if settings_row and settings_row.get("github_token") and settings_row.get("selected_repo_full_name"):
            token = settings_row["github_token"]
            repo = settings_row["selected_repo_full_name"]
            async with httpx.AsyncClient() as client:
                tree_res = await client.get(
                    f"https://api.github.com/repos/{repo}/git/trees/HEAD?recursive=1",
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
                )
                if tree_res.status_code == 200:
                    files = [
                        f["path"] for f in tree_res.json().get("tree", [])
                        if f["type"] == "blob"
                        and any(f["path"].endswith(ext) for ext in [".py", ".js", ".jsx", ".ts", ".tsx", ".css", ".html", ".json"])
                        and not any(s in f["path"] for s in ["node_modules/", "dist/", "build/", ".min."])
                    ]
                    real_files = files
                    repo_context = f"\n\nREPO: {repo}\nREAL FILES (use ONLY these — never invent paths):\n" + "\n".join(files[:50])
    except Exception as e:
        print(f"repo_context failed: {e}")

    system_prompt = f"""You are a workflow automation expert. Convert the user's description into a structured pipeline. Return ONLY valid JSON, no markdown:
{{"name":"Short workflow name","nodes":[{{"id":"1","type":"trigger|action|ai|notification","label":"Short Name","description":"What this step does","icon":"git-branch|zap|sparkles|bell|code|database|mail"}}],"edges":[{{"source":"1","target":"2"}}]}}

Rules:
- First node always trigger
- Max 8 nodes, labels 2-4 words
- SELECTED FILE is mandatory — every file operation node MUST use exactly: {selected_file_path}
- NEVER invent or guess any filename. Only use files from REAL FILES list.
- If a filename is needed and it is not in REAL FILES, use the SELECTED FILE path.
- Node descriptions must contain ONLY filenames that exist in the REAL FILES list below.{selected_file_hint}{repo_context}

Edge rules:
- If a step scans or checks code, it must create two edges:
  - errors_found → Fix Errors
  - no_errors → No Errors notification
- Fix Errors should ONLY run when errors_found
- Success notification should run only when no_errors
"""

    async with httpx.AsyncClient() as client:
        res = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {settings.GROQ_API_KEY}"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": body.prompt}
                ],
                "max_tokens": 1024,
                "temperature": 0.2,
                "top_p": 0.9
            },
            timeout=20.0
        )
    if res.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Groq error: {res.status_code}")

    raw = res.json()["choices"][0]["message"]["content"].replace("```json", "").replace("```", "").strip()
    try:
        data = json.loads(raw)

        # FIX 3: Replace hallucinated filenames with the actual selected file, not empty string
        valid_files = set(real_files)
        if valid_files and selected_file_path:
            nodes = data.get("nodes", [])
            for node in nodes:
                desc = node.get("description", "")
                matches = re.findall(r"[a-zA-Z0-9_/.-]+\.(?:js|py|ts|jsx|tsx|css|html|json)", desc)
                for f in matches:
                    if f not in valid_files:
                        desc = desc.replace(f, selected_file_path)
                node["description"] = desc.strip()

        return data
    except Exception:
        raise HTTPException(status_code=422, detail="Failed to parse AI response")