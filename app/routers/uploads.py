"""Upload endpoints: derivation relations and group assignments."""
from __future__ import annotations

from fastapi import APIRouter, Depends, File, UploadFile

from ..config import settings
from ..errors import CsvValidationError
from ..models import UploadResult
from ..services.main_service import LeakCheckService

router = APIRouter(prefix="/api", tags=["upload"])


def get_service() -> LeakCheckService:
    return LeakCheckService()


async def _read_csv(file: UploadFile) -> bytes:
    if file is None or not file.filename:
        raise CsvValidationError("缺少上传文件")
    if not file.filename.lower().endswith(".csv"):
        raise CsvValidationError("仅支持 .csv 文件")
    raw = await file.read()
    if len(raw) > settings.max_upload_bytes:
        raise CsvValidationError(
            f"文件超过大小上限 {settings.max_upload_bytes} 字节"
        )
    return raw


@router.post("/upload/relations", response_model=UploadResult)
async def upload_relations(
    file: UploadFile = File(...),
    service: LeakCheckService = Depends(get_service),
) -> UploadResult:
    """上传 LIMS 衍生关系（原样本号, 衍生样本号）。关系只增，重复合一。"""
    raw = await _read_csv(file)
    outcome = service.upload_relations(raw, settings.max_upload_bytes)
    run = service.run_status(outcome["run_id"], with_conflicts=False)
    return UploadResult(
        data_version=outcome["data_version"],
        change_type=outcome["change_type"],
        inserted_rows=outcome["inserted_rows"],
        duplicate_rows_merged=outcome["duplicate_rows_merged"],
        total_rows_after=outcome["total_rows_after"],
        verification=run,
    )


@router.post("/upload/groups", response_model=UploadResult)
async def upload_groups(
    file: UploadFile = File(...),
    service: LeakCheckService = Depends(get_service),
) -> UploadResult:
    """上传分组（样本号, 组别）。整份替换，同一样本仅允许一组。"""
    raw = await _read_csv(file)
    outcome = service.upload_groups(raw, settings.max_upload_bytes)
    run = service.run_status(outcome["run_id"], with_conflicts=False)
    return UploadResult(
        data_version=outcome["data_version"],
        change_type=outcome["change_type"],
        inserted_rows=outcome["inserted_rows"],
        duplicate_rows_merged=outcome["duplicate_rows_merged"],
        total_rows_after=outcome["total_rows_after"],
        verification=run,
    )
