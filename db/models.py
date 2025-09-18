from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ScriptInput(BaseModel):
    """Input parameter for a CEA script"""
    name: str
    type: str
    description: str
    required: bool = True
    default: Optional[Any] = None


class ScriptOutput(BaseModel):
    """Output produced by a CEA script"""
    name: str
    type: str
    description: str
    format: Optional[str] = None


class Script(BaseModel):
    """CEA script model"""
    id: Optional[str] = None
    name: str
    path: str
    cli: Optional[str] = None
    doc: Optional[str] = None
    inputs: List[ScriptInput] = Field(default_factory=list)
    outputs: List[ScriptOutput] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class WorkflowStep(BaseModel):
    """A step in a CEA workflow"""
    step: int
    script_id: str
    script_name: Optional[str] = None
    action: str
    description: Optional[str] = None
    depends_on: List[int] = Field(default_factory=list)
    parameters: Dict[str, Any] = Field(default_factory=dict)


class Workflow(BaseModel):
    """CEA workflow model"""
    id: Optional[str] = None
    name: str
    description: Optional[str] = None
    steps: List[WorkflowStep]
    tags: List[str] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ScriptSearchCriteria(BaseModel):
    """Search criteria for scripts"""
    tags: Optional[List[str]] = None
    name: Optional[str] = None
    description: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = 0


class WorkflowSearchCriteria(BaseModel):
    """Search criteria for workflows"""
    name: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    limit: Optional[int] = None
    offset: Optional[int] = 0