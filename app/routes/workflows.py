import json
import httpx
from fastapi import APIRouter, Depends, HTTPException
from app.auth import get_current_user
from app.database import query, query_one
from app.models.workflow import SaveWorkflowRequest, RunWorkflowRequest, GenerateWorkflowRequest
from app.services.executor import execute_workflow
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
    prompt_val = getattr(body.snapshot, "prompt", "")
    result = await execute_workflow(
        nodes=body.snapshot.nodes,
        edges=body.snapshot.edges,
        user_id=user["user_id"],
        context={"prompt": prompt_val}
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
                    repo_context = f"\n\nREPO: {repo}\nREAL FILES (use ONLY these in node descriptions):\n" + "\n".join(files[:50])
    except Exception as e:
        print(f"repo_context failed: {e}")

    system_prompt = f"""You are a workflow automation expert. Convert the user's description into a structured pipeline. Return ONLY valid JSON, no markdown:
{{"name":"Short workflow name","nodes":[{{"id":"1","type":"trigger|action|ai|notification","label":"Short Name","description":"What this step does","icon":"git-branch|zap|sparkles|bell|code|database|mail"}}],"edges":[{{"source":"1","target":"2"}}]}}

Rules:
- First node always trigger
- Max 8 nodes, labels 2-4 words
- In node descriptions, ONLY reference files that exist in the repo file list below
- NEVER invent filenames. ONLY use files from the REAL FILES list provided.
- Node descriptions must reference EXACT filenames from the list, not guesses.
- If no specific file is mentioned by user, reference the most relevant real file{repo_context}"""

    async with httpx.AsyncClient() as client:
        res = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {settings.GROQ_API_KEY}"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": body.prompt}],
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
        
        valid_files = set(real_files)
        if valid_files:
            nodes = data.get("nodes", [])
            for node in nodes:
                desc = node.get("description", "")
                matches = re.findall(r"[a-zA-Z0-9_/.-]+\.(?:js|py|ts|jsx|tsx)", desc)
                for f in matches:
                    if f not in valid_files:
                        desc = desc.replace(f, "")
                node["description"] = desc

        return data
    except:
        raise HTTPException(status_code=422, detail="Failed to parse AI response")

        