"""数据回填领域服务：逐分区结果驱动进度、可用范围和写入台账。

回填工作台展示的进度、完成行数、可用回填数据范围都来自逐分区的实际结果；
失败 / 无产出分区只作为拒绝依据保留，不会计入完成进度，也不会被合并入口
当成已完成交付。
"""
from __future__ import annotations

from typing import Any

from app.backfill import (
    plan_backfill_delivery,
    resolve_delivery_mode,
)
from app.services.base import BaseModuleService
from app.store import store


class BackfillPipelineService(BaseModuleService):
    MODULE = "backfill_pipeline"
    ENTITY = "回填任务"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["任务标识", "目标湖表", "回填范围", "并行分区", "覆盖模式", "回填状态", "完成行数", "失败分区"]
    REQUIRED_FIELDS = ["任务标识", "目标湖表", "回填范围"]
    STATUS_ORDER = ["排队中", "回填中", "已完成", "回填失败"]
    # 动作目标与状态流转保持一致：启动进入回填中，取消作为终止动作进入回填失败。
    ACTION_RULES = {"启动回填": "回填中", "取消回填": "回填失败"}
    NEGATIVE_ACTIONS = ["取消回填"]

    # 状态只能逐级前进：失败后恢复必须先回到回填中，不能从排队中直接跳完成。
    # 取消是终止动作，允许从排队中或回填中进入回填失败。
    TRANSITIONS = {
        "排队中": {"回填中", "回填失败"},
        "回填中": {"已完成", "回填失败"},
        "回填失败": {"回填中"},
        "已完成": set(),
    }
    ACTION_TARGET_OVERRIDES = {
        "启动回填": "回填中",
        "取消回填": "回填失败",
        "完成回填": "已完成",
        "标记失败": "回填失败",
        "恢复回填": "回填中",
    }

    # ------------------------------------------------------------------
    # 状态流转
    # ------------------------------------------------------------------

    def transition_status(self, entry: dict[str, Any], target: str) -> tuple[bool, str]:
        """校验并执行状态流转，跳级流转会被拒绝。"""
        current = str(entry.get("status") or self.STATUS_ORDER[0])
        if current == target:
            return True, f"回填任务已处于{target}，无需重复流转"
        if target not in self.STATUS_ORDER:
            return False, f"未知回填状态：{target}"
        if target not in self.TRANSITIONS.get(current, set()):
            return False, (
                f"回填状态不能从{current}直接流转到{target}，请按"
                f"{'→'.join(self.STATUS_ORDER)}逐级推进"
            )
        entry["status"] = target
        entry["pending"] = target not in ("已完成", "回填失败")
        entry["abnormal"] = target == "回填失败"
        entry["回填状态"] = target
        return True, f"回填任务状态已从{current}流转到{target}"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action not in self.ACTION_RULES and action not in self.ACTION_TARGET_OVERRIDES:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        target = self.ACTION_TARGET_OVERRIDES.get(action, self.ACTION_RULES.get(action))
        # 先校验流转是否合法（跳级会被这里拒绝），再校验完成依据。
        current = str(entry.get("status") or self.STATUS_ORDER[0])
        if target != current and target not in self.TRANSITIONS.get(current, set()):
            return None, (
                f"回填状态不能从{current}直接流转到{target}，请按"
                f"{'→'.join(self.STATUS_ORDER)}逐级推进"
            )
        if target == "已完成":
            # 没有全部分区成功的依据时，手工完成等同于把失效范围当成交付，
            # 这里直接拒绝，只能通过执行 / 恢复推进到已完成。
            detail = self.describe(entry_id)
            if not detail.get("complete"):
                return None, (
                    "逐分区结果尚未全部成功，不能标记为已完成；"
                    f"当前进度 {detail.get('progress_percent', 0)}%"
                )
        ok, message = self.transition_status(entry, target or "")
        if not ok:
            return None, message
        return entry, message

    # ------------------------------------------------------------------
    # 分区结果与写入台账
    # ------------------------------------------------------------------

    def _ledger(self, entry: dict[str, Any]) -> dict[str, dict[str, Any]]:
        ledger = entry.get("写入台账")
        if not isinstance(ledger, dict):
            ledger = {}
            entry["写入台账"] = ledger
        return ledger

    def _stored_results(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        stored = entry.get("分区结果")
        return stored if isinstance(stored, list) else []

    @staticmethod
    def _split_names(value: Any, task_id: Any = None) -> list[str]:
        if value is None:
            return []
        text = str(value).strip()
        if not text or text in ("无", "—", "-"):
            return []
        for separator in ("，", ";", "；", "|", "\n"):
            text = text.replace(separator, ",")
        names = [token.strip() for token in text.split(",") if token.strip()]
        # 排除示例占位值，避免把“数据回填样例1”之类的占位当成分区
        return [
            name for name in names
            if "样例" not in name and name != str(task_id or "")
        ]

    def _declared_results(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        """没有结构化分区结果时，按登记字段还原逐分区依据。

        只认“完成行数 / 失败分区 / 回填范围”里能核对的实际结果，不能反推出
        分区产出时返回空，而不是把任务当成已完成。
        """
        task_id = entry.get("任务标识")
        failed_names = self._split_names(entry.get("失败分区"), task_id)
        range_names = [
            name for name in self._split_names(entry.get("回填范围"), task_id)
            if name not in failed_names
        ]
        completed_rows = self._safe_int(entry.get("完成行数"), 0)
        if completed_rows <= 0 and not failed_names:
            return []
        if completed_rows <= 0:
            return [{"partition": name, "rows": 0, "status": "失败"} for name in failed_names]
        if not range_names:
            range_names = ["整体回填"]
        base, extra = divmod(completed_rows, len(range_names))
        results = []
        for index, name in enumerate(range_names):
            rows = base + (1 if index < extra else 0)
            results.append({"partition": name, "rows": rows, "status": "成功"})
        results.extend({"partition": name, "rows": 0, "status": "失败"} for name in failed_names)
        return results

    def _persist_results(
        self,
        entry: dict[str, Any],
        results: Any,
        delivery_mode: Any = None,
    ) -> dict[str, Any]:
        """记录一批逐分区结果；成功分区按分区去重写入，重复提交只记一次。"""
        candidates = plan_backfill_delivery(
            results, delivery_mode, entry.get("交付模式"), mode_is_explicit=delivery_mode is not None,
        )
        # 即使本批没有分区结果，显式选择的口径也要落库，否则下一次读取会退回旧配置。
        entry["交付模式"] = candidates["delivery_mode"]
        entry["交付模式说明"] = candidates["delivery_mode_label"]
        incoming = candidates["partitions"]
        if not incoming:
            return {
                "incoming": [],
                "written": [],
                "duplicates": [],
                "ledger": self._ledger(entry),
            }

        existing = {item["partition"]: dict(item) for item in self._stored_results(entry)}
        order = [item["partition"] for item in self._stored_results(entry)]
        ledger = self._ledger(entry)
        written: list[dict[str, Any]] = []
        duplicates: list[dict[str, Any]] = []

        for record in incoming:
            name = record["partition"]
            if name not in existing:
                order.append(name)
            if name in ledger:
                # 分区已经写过：保留首次写入结果，后续无论重复提交成功还是失败，
                # 都不再累加、也不回退已写入的 20 行。
                duplicates.append({
                    "partition": name,
                    "reported_rows": record["rows"],
                    "reported_status": record["status"],
                    "kept_rows": ledger[name]["rows"],
                    "reason": "分区已写入，重复提交只记一次",
                })
                existing[name] = {
                    **record,
                    "status": "成功",
                    "rows": ledger[name]["rows"],
                    "accepted_rows": ledger[name]["rows"],
                    "deliverable": True,
                    "reason": ledger[name].get("reason", record["reason"]),
                    "duplicate": True,
                }
                continue
            if record["status"] == "成功":
                ledger[name] = {
                    "rows": record["rows"],
                    "count": 1,
                    "reason": record["reason"],
                }
                written.append({"partition": name, "rows": record["rows"], "count": 1})
                existing[name] = {**record, "duplicate": False}
            else:
                # 失败 / 无产出分区同样保留依据，但不写入台账。
                existing[name] = {**record, "duplicate": False}

        entry["分区结果"] = [existing[name] for name in order]
        return {
            "incoming": incoming,
            "written": written,
            "duplicates": duplicates,
            "ledger": ledger,
        }

    def _record_attempt(self, entry: dict[str, Any], results: Any) -> None:
        attempts = entry.get("执行记录")
        if not isinstance(attempts, list):
            attempts = []
            entry["执行记录"] = attempts
        attempts.append({
            "attempt": len(attempts) + 1,
            "partitions": [item["partition"] for item in results],
        })

    def execute_partitions(
        self,
        entry_id: int,
        results: Any,
        delivery_mode: Any = None,
    ) -> dict[str, Any]:
        """执行回填：排队中→回填中→已完成/回填失败，逐级推进。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"回填任务 {entry_id} 不存在"}

        current = str(entry.get("status") or self.STATUS_ORDER[0])
        if current == "已完成":
            # 已完成任务的重复提交不再重复写入，只返回当前台账。
            detail = self.describe(entry_id, delivery_mode)
            detail["message"] = "回填任务已完成，重复提交未重复写入"
            return detail
        if current == "排队中":
            ok, message = self.transition_status(entry, "回填中")
            if not ok:
                return {"ok": False, "message": message}
        elif current == "回填失败":
            ok, message = self.transition_status(entry, "回填中")
            if not ok:
                return {"ok": False, "message": message}

        outcome = self._persist_results(entry, results, delivery_mode)
        if outcome["incoming"]:
            self._record_attempt(entry, outcome["incoming"])
        detail = self.describe(entry_id, delivery_mode)

        target = detail.get("suggested_status") or "回填中"
        ok, message = self.transition_status(entry, target)
        if not ok:
            return {"ok": False, "message": message, **detail}

        entry["完成行数"] = detail["usable_rows"]
        entry["失败分区"] = "、".join(detail["failed_partitions"]) or "无"
        return self.describe(entry_id, delivery_mode)

    def recover(
        self,
        entry_id: int,
        results: Any = None,
        delivery_mode: Any = None,
    ) -> dict[str, Any]:
        """失败恢复：回填失败→回填中→已完成，沿用台账保留已写入分区。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"回填任务 {entry_id} 不存在"}
        if str(entry.get("status")) == "已完成":
            detail = self.describe(entry_id, delivery_mode)
            detail["message"] = "回填任务已完成，无需恢复，重复提交未重复写入"
            return detail
        if results is None:
            # 未提供新结果时不编造产出：失败分区缺少实际行数就无法确认交付，
            # 调用方必须带上修复后的分区行数再恢复。
            retry_results = [
                {"partition": item["partition"], "rows": 0, "status": "失败"}
                for item in self._stored_results(entry)
                if item.get("status") == "失败"
            ]
            if not retry_results:
                detail = self.describe(entry_id, delivery_mode)
                detail["message"] = "没有待恢复的失败分区；如需重跑请提供逐分区结果"
                return detail
            results = retry_results
        return self.execute_partitions(entry_id, results, delivery_mode)

    # ------------------------------------------------------------------
    # 详情与交付边界
    # ------------------------------------------------------------------

    def describe(self, entry_id: int, delivery_mode: Any = None) -> dict[str, Any]:
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"回填任务 {entry_id} 不存在"}

        stored = self._stored_results(entry)
        ledger = self._ledger(entry)
        declared_basis = False
        if not stored:
            declared = self._declared_results(entry)
            if declared:
                # 只读还原：不写回台账，避免读详情时把还原结果当成真实写入。
                stored = declared
                declared_basis = True
        # 成功分区以台账中的实际写入行数为准，失败分区保留最近一次结果。
        effective: list[dict[str, Any]] = []
        for item in stored:
            name = item.get("partition", "")
            if item.get("status") == "成功" and name in ledger:
                effective.append({
                    **item,
                    "rows": ledger[name]["rows"],
                    "reason": ledger[name].get("reason", item.get("reason", "")),
                })
            else:
                effective.append(dict(item))

        plan = plan_backfill_delivery(
            effective,
            delivery_mode,
            entry.get("交付模式"),
            mode_is_explicit=delivery_mode is not None,
        )
        boundary = plan["delivery_boundary"]

        # 可用回填数据范围由逐分区实际结果组成；每个分区都带着依据，
        # 被拒绝的分区保留 basis=partition_result 的说明。
        basis = "declared_fields" if declared_basis else "partition_result"
        usable_range_detail = []
        for item in plan["partitions"]:
            written = ledger.get(item["partition"], {}).get("rows", item["rows"])
            usable_range_detail.append({
                "partition": item["partition"],
                "status": item["status"],
                "rows": written,
                "accepted_rows": item["accepted_rows"],
                "usable": item["status"] == "成功",
                "delivered": item["partition"] in boundary["delivered_partitions"],
                "basis": basis,
                "reason": item["reason"],
            })

        merge_todos = list(plan["merge_todos"])
        entry["合并待办"] = merge_todos

        # 状态与逐分区依据必须一致：标成“已完成”但没有全部分区成功依据时，
        # 详情按实际结果展示，并明确给出不一致原因，不能沿用状态冒充完成。
        status = str(entry.get("status") or "")
        status_consistent = (
            status != "已完成" or plan["complete"]
        )
        status_evidence = "状态与逐分区结果一致"
        if not status_consistent:
            status_evidence = (
                f"状态为{status}，但逐分区结果{'为空' if not plan['partitions'] else '并未全部成功'}，"
                "不能据此视为完成交付"
            )

        return {
            "ok": True,
            "task_id": entry.get("任务标识"),
            "table": entry.get("目标湖表"),
            "backfill_range": entry.get("回填范围"),
            "parallel_partitions": self._safe_int(entry.get("并行分区"), len(stored) or 4),
            "overwrite_mode": entry.get("覆盖模式", "append"),
            "status": entry.get("status"),
            "回填状态": entry.get("status"),
            "status_consistent": status_consistent,
            "status_evidence": status_evidence,
            "completed_rows": plan["usable_rows"],
            "usable_rows": plan["usable_rows"],
            "delivered_rows": plan["delivered_rows"],
            "progress_percent": plan["progress_percent"],
            "progress_basis": "逐分区实际结果",
            "complete": plan["complete"],
            "failed_partitions": "、".join(plan["failed_partitions"]) or "无",
            "failed_partition_names": plan["failed_partitions"],
            "successful_partitions": plan["successful_partitions"],
            "empty_partitions": plan["empty_partitions"],
            "pending_partitions": plan["pending_partitions"],
            "partitions": plan["partitions"],
            "usable_backfill_range": plan["usable_backfill_range"],
            "usable_range_detail": usable_range_detail,
            "delivered_partitions": boundary["delivered_partitions"],
            "held_partitions": boundary["held_partitions"],
            "rejected_partitions": plan["rejected_partitions"],
            "delivery_mode": plan["delivery_mode"],
            "delivery_mode_label": plan["delivery_mode_label"],
            "delivery_boundary": boundary,
            "next_batch_read_scope": plan["next_batch_read_scope"],
            "merge_todos": merge_todos,
            "write_ledger": {
                name: {
                    "rows": value.get("rows", 0),
                    "write_count": value.get("count", 1),
                }
                for name, value in ledger.items()
            },
            "suggested_status": plan["suggested_status"],
            "duplicate_policy": "同一分区重复提交只记一次，保留首次写入行数",
        }

    def set_delivery_mode(self, entry_id: int, mode: Any) -> dict[str, Any]:
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"回填任务 {entry_id} 不存在"}
        choice = resolve_delivery_mode(mode)
        if mode is None:
            return {"ok": False, "message": "必须指定交付口径：按分区交付 / 整批确认"}
        entry["交付模式"] = choice["mode"]
        entry["交付模式说明"] = choice["label"]
        return self.describe(entry_id)

    def delivery_view(self, entry_id: int, delivery_mode: Any = None) -> dict[str, Any]:
        """合并入口看到的交付边界：接收哪些分区、哪些留在回填侧、待办是什么。"""
        detail = self.describe(entry_id, delivery_mode)
        if not detail.get("ok"):
            return detail
        boundary = detail["delivery_boundary"]
        return {
            "ok": True,
            "task_id": detail["task_id"],
            "table": detail["table"],
            "delivery_mode": detail["delivery_mode"],
            "delivery_mode_label": detail["delivery_mode_label"],
            "delivery_boundary": boundary,
            "usable_backfill_range": detail["usable_backfill_range"],
            "usable_range_detail": detail["usable_range_detail"],
            "usable_rows": detail["usable_rows"],
            "usable_rows_basis": detail["usable_range_detail"][0]["basis"] if detail["usable_range_detail"] else "partition_result",
            "delivered_partitions": detail["delivered_partitions"],
            "delivered_rows": detail["delivered_rows"],
            "held_partitions": detail["held_partitions"],
            "rejected_partitions": detail["rejected_partitions"],
            "next_batch_read_scope": detail["next_batch_read_scope"],
            "merge_todos": detail["merge_todos"],
            "progress_percent": detail["progress_percent"],
            "complete": detail["complete"],
            "status": detail["status"],
        }

    # ------------------------------------------------------------------
    # 领域动作
    # ------------------------------------------------------------------

    def _safe_int(self, val: Any, default: int = 0) -> int:
        try:
            return int(val)
        except (ValueError, TypeError):
            return default

    def domain_backfill(self, entry_id: int, delivery_mode: Any = None) -> dict[str, Any]:
        """输出回填详情：进度和可用范围来自逐分区实际结果，失败不冒充完成。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:        # 返回字段处理结果，含配置校验和结果批次信息
            return {"ok": False, "message": f"回填任务 {entry_id} 不存在"}

        detail = self.describe(entry_id, delivery_mode)
        if detail.get("ok"):
            return detail

        # 尚未登记任何分区结果时，仍返回任务级信息，供链路判断是否具备交付条件。
        parts = self._safe_int(entry.get("并行分区"), 4)
        plan = plan_backfill_delivery([], delivery_mode, entry.get("交付模式"))
        return {
            "ok": True,
            "task_id": entry.get("任务标识"),
            "table": entry.get("目标湖表"),
            "backfill_range": entry.get("回填范围"),
            "parallel_partitions": parts,
            "overwrite_mode": entry.get("覆盖模式", "append"),
            "completed_rows": plan["usable_rows"],
            "usable_rows": plan["usable_rows"],
            "progress_percent": plan["progress_percent"],
            "progress_basis": "尚无分区结果",
            "complete": False,
            "delivery_ready": False,
            "failed_partitions": entry.get("失败分区", "无"),
            "partitions": [],
            "usable_backfill_range": [],
            "delivery_boundary": plan["delivery_boundary"],
            "merge_todos": plan["merge_todos"],
            "next_batch_read_scope": [],
        }
