"""批流合并领域服务：按回填交付边界接收分区，拒绝无产出分区。"""
from __future__ import annotations

from typing import Any

from app.backfill import (
    MODE_BATCH,
    reconcile_backfill_with_stream,
    resolve_delivery_mode,
)
from app.services.base import BaseModuleService
from app.store import store


class BatchMergeService(BaseModuleService):
    MODULE = "batch_merge"
    ENTITY = "合并管道"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["管道标识", "流来源", "批来源", "目标湖表", "去重字段", "冲突解决", "合并窗口", "上次合并"]
    REQUIRED_FIELDS = ["管道标识", "流来源", "批来源"]
    STATUS_ORDER = ["待配置", "待校验", "运行中", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启动合并": "运行中", "停用合并": "已停用"}
    NEGATIVE_ACTIONS = ["停用合并"]

    def _stream_records(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        """真实流记录：优先取合并管道上登记的流样本，没有才退回默认样例。"""
        records = entry.get("流记录")
        if isinstance(records, list) and records:
            return [dict(item) for item in records if isinstance(item, dict)]
        return [
            {"id": 1, "val": "stream-a", "source": "stream"},
            {"id": 2, "val": "stream-b", "source": "stream"},
        ]

    def _backfill_view(self, backfill_id: Any, delivery_mode: Any) -> dict[str, Any] | None:
        if backfill_id is None:
            return None
        try:
            backfill_entry_id = int(backfill_id)
        except (TypeError, ValueError):
            return {"ok": False, "message": f"回填任务 {backfill_id} 不存在"}
        # 延迟导入避免服务注册表与合并服务互相导入
        from app.services import service_for
        service = service_for("backfill_pipeline")
        if service is None:
            return {"ok": False, "message": "backfill_pipeline 服务未注册"}
        return service.delivery_view(backfill_entry_id, delivery_mode)  # type: ignore[attr-defined]

    def domain_merge_batch_stream(
        self,
        entry_id: int,
        backfill_id: Any = None,
        delivery_mode: Any = None,
        stream_records: Any = None,
    ) -> dict[str, Any]:
        """合并回填交付范围与真实流记录；无产出 / 失效分区一律拒收。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:        # 返回查询处理结果，含标准化校验和批次批次信息
            return {"ok": False, "message": f"合并管道 {entry_id} 不存在"}

        payload = self.evaluate_delivery(
            entry_id, backfill_id, delivery_mode, stream_records,
        )
        if not payload.get("ok"):
            return payload

        merge_todos = payload["merge_todos"]
        boundary = payload["delivery_boundary"]
        entry["合并待办"] = merge_todos
        entry["交付边界"] = boundary
        entry["上次合并"] = {
            "delivery_mode": payload["delivery_mode"],
            "accepted_partitions": payload["accepted_partitions"],
            "rejected_partitions": [item["partition"] for item in payload["rejected_partitions"]],
            "overwritten": payload.get("overwritten", False),
        }
        return payload

    def evaluate_delivery(
        self,
        entry_id: int,
        backfill_id: Any = None,
        delivery_mode: Any = None,
        stream_records: Any = None,
        *,
        persist: bool = False,
    ) -> dict[str, Any]:
        """计算合并入口会接收的交付边界（只读；persist=True 时回写合并待办）。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"合并管道 {entry_id} 不存在"}

        requested_mode = delivery_mode
        merge_mode = entry.get("交付模式")
        backfill_entry = None
        if backfill_id is not None:
            try:
                backfill_entry = store.find("backfill_pipeline", int(backfill_id))
            except (TypeError, ValueError):
                backfill_entry = None
        backfill_mode = (backfill_entry or {}).get("交付模式")
        # 显式选择的口径优先；没有显式选择时，合并侧与回填侧的已存配置按
        # 优先级统一（整批确认 > 按分区交付），冲突会改变下一批读取范围。
        if requested_mode is not None:
            mode_choice = resolve_delivery_mode(requested_mode)
            configured = [merge_mode, MODE_BATCH if entry.get("整批确认") else None, backfill_mode]
            configured_choice = resolve_delivery_mode(*configured)
            mode_choice["conflict"] = (
                configured_choice["mode"] != mode_choice["mode"]
                and bool(configured_choice["candidates"])
            )
            mode_choice["candidates"] = list(dict.fromkeys(
                mode_choice["candidates"] + configured_choice["candidates"]
            ))
            if mode_choice["conflict"]:
                mode_choice["reason"] = (
                    f"显式选择{mode_choice['label']}，覆盖已存配置"
                    f"{configured_choice['label']}"
                )
        else:
            mode_choice = resolve_delivery_mode(
                merge_mode,
                MODE_BATCH if entry.get("整批确认") else None,
                backfill_mode,
            )

        backfill = self._backfill_view(backfill_id, mode_choice["mode"])
        if backfill is not None and not backfill.get("ok"):
            return backfill

        if backfill is not None:
            # 回填侧按最终口径重新计算交付边界，合并入口不自行放宽或收紧。
            boundary = backfill["delivery_boundary"]
            entry_level_conflict = mode_choice["conflict"]
            entry_level_reason = mode_choice["reason"]
            mode_choice = {
                "mode": backfill["delivery_mode"],
                "label": backfill["delivery_mode_label"],
                "priority": boundary.get("priority"),
                "conflict": boundary.get("mode_conflict", False) or entry_level_conflict,
                "candidates": list(dict.fromkeys(
                    mode_choice["candidates"] + boundary.get("mode_candidates", [])
                )),
                # 合并入口识别到的口径冲突比回填侧单次计算的说明更能反映实际边界。
                "reason": entry_level_reason if entry_level_conflict
                else boundary.get("mode_reason", entry_level_reason),
            }

        if stream_records is not None:
            stream = [dict(item) for item in stream_records if isinstance(item, dict)]
        else:
            stream = self._stream_records(entry)

        boundary = (backfill or {}).get("delivery_boundary") or {}
        if boundary:
            # 交付边界带上合并入口自己的口径结论：显式选择覆盖已存配置时，
            # 冲突与原因必须出现在最终边界上，避免被回填侧单次计算覆盖。
            boundary = {
                **boundary,
                "mode": mode_choice["mode"],
                "label": mode_choice["label"],
                "mode_conflict": mode_choice["conflict"],
                "mode_candidates": mode_choice["candidates"],
                "mode_reason": mode_choice["reason"],
            }
        delivered = list(boundary.get("delivered_partitions") or [])
        rejected = list(boundary.get("rejected_partitions") or [])
        delivered_rows = int((backfill or {}).get("delivered_rows") or 0)

        # 批来源只接收完整成功的分区；被拒绝分区的行数绝不进入合并输入。
        batch_records: list[dict[str, Any]] = []
        for partition in delivered:
            batch_records.append({"id": f"partition:{partition}", "val": partition, "source": "backfill"})

        result = reconcile_backfill_with_stream(
            stream,
            batch_records,
            delivered_rows=delivered_rows,
            delivered_partitions=delivered,
            dedup_key="id",
        )

        # 未交付的 partition（含无产出 / 失效范围）不得出现在合并结果中。
        accepted_partitions = [
            item["partition"]
            for item in (backfill or {}).get("usable_range_detail", [])
            if item.get("delivered")
        ]
        no_output_rejected = [
            item["partition"] for item in rejected if item.get("status") == "无产出"
        ]

        merge_todos = list((backfill or {}).get("merge_todos") or [])
        held = list(boundary.get("held_partitions") or [])
        if held and not boundary.get("complete"):
            merge_todos.append({
                "type": "held_until_batch_confirm",
                "todo": f"{'、'.join(held)} 可用但本批未交付，留在回填侧等待整批确认",
                "partitions": held,
                "status": "待定",
            })

        payload = {
            "ok": True,
            "pipeline": entry.get("管道标识"),
            "table": entry.get("目标湖表"),
            "dedup_field": entry.get("去重字段", "id"),
            "conflict_resolution": entry.get("冲突解决", "batch-wins"),
            "delivery_mode": mode_choice["mode"],
            "delivery_mode_label": mode_choice["label"],
            "delivery_boundary": boundary,
            "accepted_partitions": accepted_partitions,
            "delivered_partitions": delivered,
            "held_partitions": held,
            "rejected_partitions": rejected,
            "rejected_no_output": no_output_rejected,
            "next_batch_read_scope": (backfill or {}).get("next_batch_read_scope", []),
            "merge_todos": merge_todos,
            "merge_pending": {
                "has_todos": bool(merge_todos),
                "todos": merge_todos,
            },
            **result,
        }
        if persist:
            entry["合并待办"] = merge_todos
            entry["交付边界"] = boundary
        return payload

    def apply_delivery_mode(self, entry_id: int, mode: Any) -> dict[str, Any]:
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"合并管道 {entry_id} 不存在"}
        choice = resolve_delivery_mode(mode)
        entry["交付模式"] = choice["mode"]
        entry["整批确认"] = choice["mode"] == MODE_BATCH
        return {"ok": True, "mode": choice["mode"], "label": choice["label"]}
