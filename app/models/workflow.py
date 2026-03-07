from pydantic import BaseModel
from typing import Any, Optional

class WorkflowSnapshot(BaseModel):
    title: str
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    prompt: Optional[str] = None

class RunWorkflowRequest(BaseModel):
    workflow_id: str
    workflow_name: str
    snapshot: WorkflowSnapshot

class SaveWorkflowRequest(BaseModel):
    name: str
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    status: Optional[str] = "draft"

class GenerateWorkflowRequest(BaseModel):
    prompt: str
    model: Optional[str] = "groq"
