from fastapi import APIRouter, Depends, HTTPException
from app.auth import get_current_user
from app.database import supabase

router = APIRouter(prefix="/runs", tags=["runs"])


# ── GET /runs — all runs for user ────────────────────────────────
@router.get("/")
async def list_runs(user: dict = Depends(get_current_user)):
    result = (
        supabase.table("workflow_runs")
        .select("*")
        .eq("user_id", user["user_id"])
        .order("started_at", desc=True)
        .limit(50)
        .execute()
    )
    return {"runs": result.data}


# ── GET /runs/:id — single run with snapshot for replay ──────────
@router.get("/{run_id}")
async def get_run(run_id: str, user: dict = Depends(get_current_user)):
    result = (
        supabase.table("workflow_runs")
        .select("*")
        .eq("id", run_id)
        .eq("user_id", user["user_id"])
        .single()
        .execute()
    )
    if not result.data:
        raise HTTPException(status_code=404, detail="Run not found")
    return result.data


# ── GET /runs/workflow/:workflow_id — runs for a specific workflow
@router.get("/workflow/{workflow_id}")
async def runs_for_workflow(
    workflow_id: str,
    user: dict = Depends(get_current_user)
):
    result = (
        supabase.table("workflow_runs")
        .select("*")
        .eq("workflow_id", workflow_id)
        .eq("user_id", user["user_id"])
        .order("started_at", desc=True)
        .execute()
    )
    return {"runs": result.data}
