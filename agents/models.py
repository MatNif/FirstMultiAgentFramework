"""
Pydantic models for agent communication and task representation
"""

from typing import Dict, List, Literal, Optional
from pydantic import BaseModel, Field


class Task(BaseModel):
    """Task schema for normalized user requests"""
    intent: str = Field(..., description="The primary intent/action the user wants to perform")
    scope: Optional[Literal["building", "district"]] = Field(None, description="Analysis scope - building or district level")
    inputs: Dict[str, str] = Field(default_factory=dict, description="Input files and parameters extracted from user text")
    constraints: Dict[str, str] = Field(default_factory=dict, description="Constraints and requirements mentioned by user")
    raw_text: str = Field(..., description="Original user input text")

    class Config:
        json_encoders = {
            # Ensure proper JSON serialization
        }

    def to_dict(self) -> dict:
        """Convert to dictionary for message content"""
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: dict) -> "Task":
        """Create Task from dictionary"""
        return cls(**data)


class PlanStep(BaseModel):
    """A single step in an execution plan"""
    script_id: str = Field(..., description="ID of the script to execute")
    args: Dict[str, str] = Field(default_factory=dict, description="Arguments to pass to the script")

    def to_dict(self) -> dict:
        """Convert to dictionary for message content"""
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: dict) -> "PlanStep":
        """Create PlanStep from dictionary"""
        return cls(**data)


class Plan(BaseModel):
    """Complete execution plan for a task"""
    plan: List[PlanStep] = Field(..., description="List of steps to execute")
    explain: str = Field(..., description="Explanation of why this workflow fits the task")
    assumptions: List[str] = Field(default_factory=list, description="Assumptions made during planning")
    missing: List[str] = Field(default_factory=list, description="Missing inputs required for execution")

    def to_dict(self) -> dict:
        """Convert to dictionary for message content"""
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: dict) -> "Plan":
        """Create Plan from dictionary"""
        return cls(**data)