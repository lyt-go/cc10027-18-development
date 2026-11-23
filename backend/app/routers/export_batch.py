"""多源导出批次清单接口：建立批次、幂等下载、断点接续、逐条对账。

路由前缀 /api/export-batches 必须在通用 /{module} 路由之前注册，
否则固定路径会被动态模块路径抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.services import service_for
from app.services.format_output import FormatOutputService

router = APIRouter(prefix="/api/export-batches", tags=["多源导出批次"])


def _service() -> FormatOutputService:
    svc = service_for("format_output")
    assert isinstance(svc, FormatOutputService)
    return svc


def _manifest_or_404(batch_id: str) -> dict[str, Any]:
    manifest = _service().get_export_batch(batch_id)
    if "error" in manifest:
        raise HTTPException(status_code=404, detail=manifest["error"])
    return manifest


class CreateBatchPayload(BaseModel):
    source_keys: list[str] | None = None
    format: str = "jsonl"
    run: bool = Field(default=True, description="建立清单后立即发放可用条目的成品")


@router.post("")
def create_export_batch(payload: CreateBatchPayload) -> dict[str, Any]:
    """根据各源已确认采集位点建立批次清单，并发放可用条目的成品文件。"""
    svc = _service()
    manifest = svc.create_export_batch(payload.source_keys, payload.format)
    if payload.run:
        manifest = svc.run_export_batch(manifest["batch_id"], action="download")
    return manifest


@router.get("")
def list_export_batches() -> dict[str, Any]:
    """列出全部多源导出批次清单。"""
    batches = _service().list_export_batches()
    return {"items": batches, "total": len(batches)}


@router.get("/{batch_id}")
def get_export_batch(batch_id: str) -> dict[str, Any]:
    """读取批次清单：每份成品关联的源、位点范围、规则版本与校验摘要。"""
    return _manifest_or_404(batch_id)


@router.post("/{batch_id}/download")
def download_export_batch(batch_id: str) -> dict[str, Any]:
    """幂等下载：已交付成品直接复用，缺件按原批冻结范围接续。"""
    svc = _service()
    manifest = svc.run_export_batch(batch_id, action="download")
    if "error" in manifest:
        raise HTTPException(status_code=404, detail=manifest["error"])
    return manifest


@router.post("/{batch_id}/resume")
def resume_export_batch(batch_id: str) -> dict[str, Any]:
    """中断接续：只补未交付的条目，保留先前已确认成品的下载依据。"""
    svc = _service()
    manifest = svc.resume_export_batch(batch_id)
    if "error" in manifest:
        raise HTTPException(status_code=404, detail=manifest["error"])
    return manifest


@router.get("/{batch_id}/verify")
def verify_export_batch(batch_id: str) -> dict[str, Any]:
    """对账：逐条核对已交付成品是否仍与原批清单对应。"""
    svc = _service()
    report = svc.verify_export_batch(batch_id)
    if "error" in report:
        raise HTTPException(status_code=404, detail=report["error"])
    return report


@router.get("/{batch_id}/files/{source_key}")
def download_batch_file(batch_id: str, source_key: str) -> FileResponse:
    """下载批次中指定源的成品文件。"""
    manifest = _manifest_or_404(batch_id)
    for entry in manifest.get("entries", []):
        if entry.get("source_key") == source_key:
            if entry.get("status") != "已交付" or not entry.get("artifact"):
                raise HTTPException(status_code=409, detail=f"{source_key} 尚未交付，无法下载")
            from pathlib import Path
            artifact = Path(entry["artifact"])
            if not artifact.exists():
                raise HTTPException(status_code=410, detail="成品文件已缺失，请重新导出")
            return FileResponse(artifact, filename=artifact.name)
    raise HTTPException(status_code=404, detail=f"批次 {batch_id} 中不存在源 {source_key}")
