"""Pydantic response / request models."""
from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class VersionInfo(BaseModel):
    version: int
    change_type: str
    detail: str
    created_at: str


class UploadResult(BaseModel):
    status: str = "committed"
    data_version: int
    change_type: str
    inserted_rows: int
    duplicate_rows_merged: int = 0
    total_rows_after: int
    verification: "RunStatus"


class Conflict(BaseModel):
    sample_a: str
    group_a: str
    sample_b: str
    group_b: str
    component_size: int
    path: List[str] = Field(description="一条实际连接路径（端点到端点的样本号序列）")


class RunStatus(BaseModel):
    run_id: int
    data_version: int
    status: str  # queued | running | passed | failed | superseded
    progress_pct: float
    vertices_total: Optional[int] = None
    vertices_processed: int = 0
    edges_total: Optional[int] = None
    components_total: Optional[int] = None
    conflicts_total: int = 0
    attempts: int = 1
    error_message: Optional[str] = None
    queued_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    conflicts: Optional[List[Conflict]] = None


class CurrentStatus(BaseModel):
    current_data_version: int
    accepted_version: Optional[int] = None
    acceptance_active: bool = False
    latest_run: Optional[RunStatus] = None
    download_available: bool
    download_version: Optional[int] = None
    relation_edges: int
    grouped_samples: int


class Health(BaseModel):
    status: str
    worker_id: str


UploadResult.model_rebuild()
