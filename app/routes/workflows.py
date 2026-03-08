from fastapi import APIRouter, Depends, HTTPException
from app.auth import get_current_user
from app.database import supabase
from app.models.workflow import (
    SaveWorkflowRequest, RunWorkflowRequest, GenerateWorkflowRequest
)
from app.services.executor import execute_workflow
import httpx
from app.config import settings

router = APIRouter(prefix="/workflows", tags=["workflows"])


@router.get("/")
async def list_workflows(user: dict = Depends(get_current_user)):
    result = (
        supabase.table("workflows")
        .select("*")
        .eq("user_id", user["user_id"])
        .order("created_at", desc=True)
        .execute()
    )
    return {"workflows": result.data}


@router.get("/{workflow_id}")
async def get_workflow(workflow_id: str, user: dict = Depends(get_current_user)):
    result = (
        supabase.table("workflows")
        .select("*")
        .eq("id", workflow_id)
        .eq("user_id", user["user_id"])
        .execute()
    )
    if not result.data:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return result.data[0]


@router.post("/")
async def save_workflow(
    body: SaveWorkflowRequest,
    user: dict = Depends(get_current_user)
):
    result = (
        supabase.table("workflows")
        .insert({
            "user_id": user["user_id"],
            "name": body.name,
            "nodes": body.nodes,
            "edges": body.edges,
            "status": body.status or "draft"
        })
        .execute()
    )
    if not result.data:
        raise HTTPException(status_code=500, detail="Failed to save workflow")
    return result.data[0]


@router.put("/{workflow_id}")
async def update_workflow(
    workflow_id: str,
    body: SaveWorkflowRequest,
    user: dict = Depends(get_current_user)
):
    result = (
        supabase.table("workflows")
        .update({
            "name": body.name,
            "nodes": body.nodes,
            "edges": body.edges,
            "status": body.status,
        })
        .eq("id", workflow_id)
        .eq("user_id", user["user_id"])
        .execute()
    )
    if not result.data:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return result.data[0]


@router.delete("/{workflow_id}")
async def delete_workflow(
    workflow_id: str,
    user: dict = Depends(get_current_user)
):
    supabase.table("workflows").delete().eq("id", workflow_id).eq("user_id", user["user_id"]).execute()
    return {"deleted": True}


@router.post("/run")
async def run_workflow(
    body: RunWorkflowRequest,
    user: dict = Depends(get_current_user)
):
    result = await execute_workflow(
        nodes=body.snapshot.nodes,
        edges=body.snapshot.edges,
        user_id=user["user_id"]
    )

    run_data = {
        "user_id": user["user_id"],
        "workflow_id": body.workflow_id,
        "workflow_name": body.workflow_name,
        "status": result["status"],
        "started_at": result["started_at"],
        "duration": result["duration"],
        "triggered_by": "manual",
        "snapshot": body.snapshot.model_dump(),
        "logs": result["logs"]
    }

    run_result = supabase.table("workflow_runs").insert(run_data).execute()

    return {
        "run_id": run_result.data[0]["id"] if run_result.data else None,
        "status": result["status"],
        "duration": result["duration"],
        "logs": result["logs"]
    }


@router.post("/generate")
async def generate_workflow(
    body: GenerateWorkflowRequest,
    user: dict = Depends(get_current_user)
):
    system_prompt = """You are a workflow automation expert. Convert the user's description into a structured pipeline. Return ONLY valid JSON, no markdown:
{"name":"Short workflow name","nodes":[{"id":"1","type":"trigger|action|ai|notification","label":"Short Name","description":"What this step does","icon":"git-branch|zap|sparkles|bell|code|database|mail"}],"edges":[{"source":"1","target":"2"}]}
Rules: first node always trigger, max 8 nodes, labels 2-4 words."""

    async with httpx.AsyncClient() as client:
        res = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {settings.GROQ_API_KEY}"
            },
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": body.prompt}
                ],
                "max_tokens": 1024,
                "temperature": 0.7
            },
            timeout=20.0
        )

    if res.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Groq error: {res.status_code}")

    data = res.json()
    raw = data["choices"][0]["message"]["content"].replace("```json", "").replace("```", "").strip()

    import json
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(status_code=422, detail="Failed to parse AI response")

    return parsed
