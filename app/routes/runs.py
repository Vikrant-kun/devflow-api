from fastapi import APIRouter, Depends, HTTPException
from app.auth import get_current_user
from app.database import query, query_one

router = APIRouter(prefix="/runs", tags=["runs"])

@router.get("/")
async def list_runs(user: dict = Depends(get_current_user)):
    rows = query(
        "SELECT * FROM workflow_runs WHERE user_id = %s ORDER BY started_at DESC LIMIT 50",
        (user["user_id"],)
    )
    return {"runs": rows}

@router.get("/{run_id}")
async def get_run(run_id: str, user: dict = Depends(get_current_user)):
    row = query_one(
        "SELECT * FROM workflow_runs WHERE id = %s AND user_id = %s",
        (run_id, user["user_id"])
    )
    if not row:
        raise HTTPException(status_code=404, detail="Run not found")
    return row

@router.get("/workflow/{workflow_id}")
async def runs_for_workflow(workflow_id: str, user: dict = Depends(get_current_user)):
    rows = query(
        "SELECT * FROM workflow_runs WHERE workflow_id = %s AND user_id = %s ORDER BY started_at DESC",
        (workflow_id, user["user_id"])
    )
    return {"runs": rows}