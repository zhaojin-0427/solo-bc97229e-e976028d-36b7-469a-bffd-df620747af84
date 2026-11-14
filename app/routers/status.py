"""Status / progress endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from ..models import CurrentStatus, RunStatus, VersionInfo
from ..services.main_service import LeakCheckService

router = APIRouter(prefix="/api", tags=["status"])


def get_service() -> LeakCheckService:
    return LeakCheckService()


@router.get("/status", response_model=CurrentStatus)
def current_status(
    service: LeakCheckService = Depends(get_service),
) -> CurrentStatus:
    """当前数据版本、放行状态、最新核查进度与下载可用性。"""
    return CurrentStatus(**service.current_status())


@router.get("/runs/latest", response_model=RunStatus)
def latest_run(
    conflicts: bool = Query(True, description="失败时是否返回跨组冲突明细"),
    service: LeakCheckService = Depends(get_service),
) -> RunStatus:
    run = service.run_status(with_conflicts=conflicts)
    if run is None:
        raise HTTPException(status_code=404, detail="尚无任何核查任务")
    return run


@router.get("/runs/{run_id}", response_model=RunStatus)
def get_run(
    run_id: int,
    conflicts: bool = Query(True),
    service: LeakCheckService = Depends(get_service),
) -> RunStatus:
    run = service.run_status(run_id, with_conflicts=conflicts)
    if run is None:
        raise HTTPException(status_code=404, detail=f"核查任务 {run_id} 不存在")
    return run


@router.get("/versions", response_model=list[VersionInfo])
def list_versions(
    limit: int = Query(50, ge=1, le=500),
    service: LeakCheckService = Depends(get_service),
) -> list[VersionInfo]:
    return [VersionInfo(**dict(r)) for r in service.business.list_versions(limit)]
