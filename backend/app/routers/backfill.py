"""回填工作台与合并入口的专用接口。

这些接口让“逐分区结果 → 可用回填数据范围 → 交付边界”的交接可以直接验证：
- 回填详情只看回填侧的分区实际结果；
- 合并入口只看它最终会接收 / 拒绝 / 留在回填侧的分区。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.backfill import normalize_mode_input
from app.services import service_for
from app.store import store

router = APIRouter(prefix="/api/backfill", tags=["回填交付"])


def _checked_mode(value: str | None) -> str | None:
    mode, error = normalize_mode_input(value)
    if error:
        raise HTTPException(400, error)
    return mode


class PartitionResultPayload(BaseModel):
    partition: str
    rows: int = 0
    status: str | None = None
    reason: str | None = None


class ExecutePayload(BaseModel):
    delivery_mode: str | None = None
    results: list[PartitionResultPayload] = Field(default_factory=list)


class RecoverPayload(BaseModel):
    delivery_mode: str | None = None
    results: list[PartitionResultPayload] | None = None


class DeliveryModePayload(BaseModel):
    delivery_mode: str | None = None


class MergeEvaluatePayload(BaseModel):
    backfill_id: int | None = None
    delivery_mode: str | None = None
    stream_records: list[dict[str, Any]] | None = None


def _backfill_service():
    service = service_for("backfill_pipeline")
    if service is None:
        raise HTTPException(404, "backfill_pipeline 服务未注册")
    return service


def _merge_service():
    service = service_for("batch_merge")
    if service is None:
        raise HTTPException(404, "batch_merge 服务未注册")
    return service


def _results(payload: ExecutePayload | RecoverPayload) -> list[dict[str, Any]]:
    if payload.results is None:
        return []
    return [item.model_dump() for item in payload.results]


def _require_backfill(entry_id: int) -> dict[str, Any]:
    entry = store.find("backfill_pipeline", entry_id)
    if entry is None:
        raise HTTPException(404, f"回填任务 {entry_id} 不存在")
    return entry


@router.get("/{backfill_id}/detail")
def backfill_detail(backfill_id: int, delivery_mode: str | None = None) -> dict[str, Any]:
    """回填工作台详情：进度、完成行数、可用回填数据范围都来自逐分区实际结果。"""
    _require_backfill(backfill_id)
    return _backfill_service().describe(backfill_id, _checked_mode(delivery_mode))


@router.post("/{backfill_id}/execute")
def backfill_execute(backfill_id: int, payload: ExecutePayload) -> dict[str, Any]:
    """执行一批分区回填；状态按排队中→回填中→已完成/回填失败逐级推进。"""
    _require_backfill(backfill_id)
    return _backfill_service().execute_partitions(
        backfill_id, _results(payload), _checked_mode(payload.delivery_mode),
    )


@router.post("/{backfill_id}/recover")
def backfill_recover(backfill_id: int, payload: RecoverPayload) -> dict[str, Any]:
    """失败恢复：回填失败→回填中→已完成，已写入分区不重复累计。"""
    _require_backfill(backfill_id)
    return _backfill_service().recover(
        backfill_id, _results(payload) if payload.results is not None else None,
        _checked_mode(payload.delivery_mode),
    )


@router.post("/{backfill_id}/delivery-mode")
def backfill_delivery_mode(backfill_id: int, payload: DeliveryModePayload) -> dict[str, Any]:
    """设置交付边界口径（按分区交付 / 整批确认），并返回新的交付范围。"""
    _require_backfill(backfill_id)
    mode = _checked_mode(payload.delivery_mode)
    if mode is None:
        raise HTTPException(400, "必须指定交付口径：按分区交付 / 整批确认")
    return _backfill_service().set_delivery_mode(backfill_id, mode)


@router.get("/{backfill_id}/boundary")
def backfill_boundary(backfill_id: int, delivery_mode: str | None = None) -> dict[str, Any]:
    """合并入口使用的交付边界：接收、拒绝、留在回填侧的分区及合并待办。"""
    _require_backfill(backfill_id)
    return _backfill_service().delivery_view(backfill_id, _checked_mode(delivery_mode))


@router.post("/merge/{merge_id}/evaluate")
def merge_evaluate(merge_id: int, payload: MergeEvaluatePayload) -> dict[str, Any]:
    """合并入口预览：按回填交付边界决定接收范围，拒绝无产出分区。"""
    service = _merge_service()
    if store.find("batch_merge", merge_id) is None:
        raise HTTPException(404, f"合并管道 {merge_id} 不存在")
    result = service.evaluate_delivery(  # type: ignore[attr-defined]
        merge_id,
        backfill_id=payload.backfill_id,
        delivery_mode=_checked_mode(payload.delivery_mode),
        stream_records=payload.stream_records,
    )
    if not result.get("ok"):
        raise HTTPException(400, result.get("message", "合并入口无法确定交付边界"))
    return result
