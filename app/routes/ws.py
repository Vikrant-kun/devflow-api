from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.database import query
from app.services.executor import execute_workflow_ws
import json

router = APIRouter()

@router.websocket("/ws/run/{user_id}")
async def websocket_run(websocket: WebSocket, user_id: str):
    await websocket.accept()
    try:
        data = await websocket.receive_text()
        body = json.loads(data)
        nodes = body.get("nodes", [])
        edges = body.get("edges", [])
        workflow_id = body.get("workflow_id")
        workflow_name = body.get("workflow_name", "Pipeline")
        snapshot = body.get("snapshot", {})

        async def on_node_update(log_entry):
            await websocket.send_text(json.dumps({"type": "node_update", "data": log_entry}))

        result = await execute_workflow_ws(
            nodes=nodes, edges=edges, user_id=user_id,
            context={"prompt": snapshot.get("prompt", "")},
            on_node_complete=on_node_update
        )

        query(
            """INSERT INTO workflow_runs (user_id, workflow_id, workflow_name, status, started_at, duration, triggered_by, snapshot, logs)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (user_id, workflow_id, workflow_name, result["status"],
             result["started_at"], result["duration"], "manual",
             json.dumps(snapshot), json.dumps(result["logs"]))
        )

        await websocket.send_text(json.dumps({"type": "complete", "data": result}))

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_text(json.dumps({"type": "error", "message": str(e)}))
        except:
            pass