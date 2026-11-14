"""Release download endpoint.

Only an *active* acceptance (i.e. the newest data version passed verification
and no upload happened afterwards) enables download. The served file is the
frozen snapshot written at acceptance time, so in-flight changes can never be
mixed into it.
"""
from __future__ import annotations

import os

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse

from ..errors import NotReleasedError
from ..services.main_service import LeakCheckService

router = APIRouter(prefix="/api", tags=["release"])


def get_service() -> LeakCheckService:
    return LeakCheckService()


@router.get("/release/groups.csv")
def download_release(
    service: LeakCheckService = Depends(get_service),
) -> FileResponse:
    artifact = service.download_artifact()
    if artifact is None:
        raise NotReleasedError(
            "当前不存在有效放行：最新核查尚未通过，或核查通过后数据又发生变更。"
            "未完成核查前不报安全、不提供下载。"
        )
    file_path, row_count, version = artifact
    if not os.path.exists(file_path):
        raise NotReleasedError(
            f"放行版本 v{version} 的快照文件缺失，请等待重新核查后再下载"
        )
    return FileResponse(
        file_path,
        media_type="text/csv; charset=utf-8",
        filename=f"groups_release_v{version}.csv",
        headers={
            "X-Release-Version": str(version),
            "X-Release-Rows": str(row_count),
            "Cache-Control": "no-store",
        },
    )
