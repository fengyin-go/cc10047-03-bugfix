"""回填分区结果 → 可用回填数据范围 → 交付边界。

回填工作台和批流合并入口共用这里的一套口径：

- 进度按逐分区的实际结果计算，存在失败 / 无产出分区时不会显示 100%；
- 可用回填数据范围只包含真正写入成功的分区，失败和无产出分区单独保留拒绝依据；
- 交付边界支持“按分区交付”和“整批确认”两种口径，两者都不会把失效范围当成
  完成交付，也不会把无产出分区交给下游。

本模块只做纯计算，不读写 store，方便服务层和测试复用。
"""
from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

STATUS_SUCCESS = "成功"
STATUS_FAILED = "失败"
STATUS_EMPTY = "无产出"
STATUS_PENDING = "待定"

MODE_PARTITION = "partition"
MODE_BATCH = "batch"

MODE_LABELS = {
    MODE_PARTITION: "按分区交付",
    MODE_BATCH: "整批确认",
}
# 两种口径同时出现时，整批确认优先级更高：它要求同批分区全部成功后才统一交付，
# 失败批次不允许提前把部分分区暴露给下游。
MODE_PRIORITY = {
    MODE_PARTITION: 1,
    MODE_BATCH: 2,
}

_MODE_ALIASES = {
    "partition": MODE_PARTITION,
    "per-partition": MODE_PARTITION,
    "per_partition": MODE_PARTITION,
    "per-partition-delivery": MODE_PARTITION,
    "按分区": MODE_PARTITION,
    "按分区交付": MODE_PARTITION,
    "分区交付": MODE_PARTITION,
    "batch": MODE_BATCH,
    "whole-batch": MODE_BATCH,
    "whole_batch": MODE_BATCH,
    "batch-confirm": MODE_BATCH,
    "batch_confirm": MODE_BATCH,
    "整批": MODE_BATCH,
    "整批确认": MODE_BATCH,
    "整批交付": MODE_BATCH,
}

_SUCCESS_WORDS = {
    "成功", "已完成", "完成", "已交付", "成功分区", "正常",
    "success", "succeeded", "ok", "done", "completed", "delivered",
}
_FAILED_WORDS = {
    "失败", "回填失败", "失败分区", "异常", "错误",
    "failed", "failure", "fail", "error", "failed_partition",
}
_EMPTY_WORDS = {
    "无产出", "空", "空分区", "无数据", "无结果",
    "empty", "no_output", "no-output", "nooutput", "skipped", "void",
}
_PENDING_WORDS = {
    "待定", "待回填", "待执行", "排队中", "回填中", "等待", "未完成",
    "pending", "waiting", "queued", "running",
}

_PARTITION_KEYS = ("partition", "分区", "partition_name", "partition_id", "分区名", "name")
_ROWS_KEYS = (
    "rows", "row_count", "written_rows", "rows_written",
    "行数", "写入行数", "完成行数", "实际行数",
)
_STATUS_KEYS = ("status", "状态", "结果", "result", "state")
_REASON_KEYS = ("reason", "原因", "message", "说明", "error")


# ------------------------------------------------------------------
# 基础解析
# ------------------------------------------------------------------

def _to_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        text = str(value or "").strip()
        if not text:
            return default
        digits = "".join(ch for ch in text if ch.isdigit())
        return int(digits) if digits else default


def normalize_delivery_mode(value: Any) -> str | None:
    """把中英文别名统一成 partition / batch；无法识别时返回 None。"""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return _MODE_ALIASES.get(text) or _MODE_ALIASES.get(text.casefold())


def resolve_delivery_mode(*candidates: Any) -> dict[str, Any]:
    """解析交付边界；同时传入多种口径时按优先级选择，并标记冲突。"""
    raw: list[Any] = []
    for candidate in candidates:
        if candidate is None:
            continue
        if isinstance(candidate, (list, tuple, set)):
            raw.extend(candidate)
        else:
            raw.append(candidate)
    modes: list[str] = []
    for item in raw:
        mode = normalize_delivery_mode(item)
        if mode:
            modes.append(mode)
    if not modes:
        mode = MODE_PARTITION
        return {
            "mode": mode,
            "label": MODE_LABELS[mode],
            "priority": MODE_PRIORITY[mode],
            "candidates": [],
            "conflict": False,
            "reason": "未配置交付边界，默认按分区交付",
        }
    selected = max(modes, key=lambda item: MODE_PRIORITY[item])
    conflict = len(set(modes)) > 1
    return {
        "mode": selected,
        "label": MODE_LABELS[selected],
        "priority": MODE_PRIORITY[selected],
        "candidates": modes,
        "conflict": conflict,
        "reason": (
            f"同时配置了{'、'.join(MODE_LABELS[m] for m in dict.fromkeys(modes))}，"
            f"按优先级选择{MODE_LABELS[selected]}"
            if conflict
            else f"交付边界为{MODE_LABELS[selected]}"
        ),
    }


def normalize_mode_input(value: Any) -> tuple[str | None, str | None]:
    """校验外部传入的交付口径。

    返回 ``(mode, error)``：空值表示未指定；无法识别时给出错误说明，
    避免把拼错的口径静默当成默认的按分区交付。
    """
    if value is None:
        return None, None
    mode = normalize_delivery_mode(value)
    if mode:
        return mode, None
    if isinstance(value, str) and not value.strip():
        return None, None
    return None, f"无法识别的交付口径：{value}（可选：按分区交付 / 整批确认）"


def _explicit_choice(mode: str) -> dict[str, Any]:
    return {
        "mode": mode,
        "label": MODE_LABELS[mode],
        "priority": MODE_PRIORITY[mode],
        "candidates": [mode],
        "conflict": False,
        "reason": f"显式选择{MODE_LABELS[mode]}",
    }


def _normalize_status_word(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    folded = text.casefold()
    for words, status in (
        (_PENDING_WORDS, STATUS_PENDING),
        (_EMPTY_WORDS, STATUS_EMPTY),
        (_FAILED_WORDS, STATUS_FAILED),
        (_SUCCESS_WORDS, STATUS_SUCCESS),
    ):
        if text in words or folded in words:
            return status
    # 容忍“p18 回填失败”之类的描述性状态
    if "无产出" in text or "无数据" in text:
        return STATUS_EMPTY
    if "失败" in text or "异常" in text:
        return STATUS_FAILED
    if "待" in text or "等待" in text:
        return STATUS_PENDING
    if "成功" in text or "完成" in text:
        return STATUS_SUCCESS
    return None


def _first_present(source: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in source and source[key] is not None:
            return source[key]
    return None


def _record_from_mapping(source: Mapping[str, Any]) -> dict[str, Any]:
    record = dict(source)
    partition = _first_present(record, _PARTITION_KEYS)
    rows = _first_present(record, _ROWS_KEYS)
    status = _first_present(record, _STATUS_KEYS)
    reason = _first_present(record, _REASON_KEYS)
    return {
        "partition": "" if partition is None else str(partition).strip(),
        "rows": _to_int(rows, 0),
        "raw_status": status,
        "raw_reason": "" if reason is None else str(reason),
    }


def _record_from_pair(partition: Any, value: Any) -> dict[str, Any]:
    name = str(partition).strip()
    if isinstance(value, Mapping):
        record = _record_from_mapping(value)
        record["partition"] = name
        return record
    if isinstance(value, str):
        status = _normalize_status_word(value)
        if status is not None and not value.strip().isdigit():
            return {"partition": name, "rows": 0, "raw_status": value, "raw_reason": ""}
    return {"partition": name, "rows": _to_int(value, 0), "raw_status": None, "raw_reason": ""}


def _parse_text_result(text: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for token in text.replace("\n", ",").replace(";", ",").replace("；", ",").split(","):
        token = token.strip()
        if not token:
            continue
        separator = "=" if "=" in token else (":" if ":" in token else None)
        if separator:
            partition, value = token.split(separator, 1)
            records.append(_record_from_pair(partition.strip(), value.strip()))
        else:
            records.append({"partition": token, "rows": 0, "raw_status": None, "raw_reason": ""})
    return records


def _raw_partition_records(results: Any) -> list[dict[str, Any]]:
    if results is None:
        return []
    if isinstance(results, str):
        text = results.strip()
        if text[:1] in ("{", "["):
            # 同步接口里持久化的分区结果常常是一段 JSON 文本
            try:
                return _raw_partition_records(json.loads(text))
            except (ValueError, TypeError):
                pass
        return _parse_text_result(results)
    if isinstance(results, Mapping):
        if "partitions" in results and isinstance(results["partitions"], (list, Mapping, str)):
            return _raw_partition_records(results["partitions"])
        if "results" in results and isinstance(results["results"], (list, Mapping, str)):
            return _raw_partition_records(results["results"])
        if any(key in results for key in _PARTITION_KEYS):
            return [_record_from_mapping(results)]
        return [_record_from_pair(key, value) for key, value in results.items()]
    if isinstance(results, Iterable):
        records: list[dict[str, Any]] = []
        for item in results:
            if isinstance(item, Mapping):
                records.append(_record_from_mapping(item))
            elif isinstance(item, str):
                parsed = _parse_text_result(item)
                if parsed:
                    records.extend(parsed)
            elif isinstance(item, Iterable) and not isinstance(item, (bytes, bytearray)):
                pair = list(item)
                if len(pair) >= 2:
                    records.append(_record_from_pair(pair[0], pair[1]))
        return records
    return []


def normalize_partition_results(results: Any) -> list[dict[str, Any]]:
    """把各种输入形态归一成逐分区结果，重复分区以最后一次提交为准。

    支持：
    - ``[{"partition": "p17", "rows": 20, "status": "成功"}, ...]``
    - ``{"p17": 20, "p18": {"rows": 0, "status": "失败"}}``
    - ``"p17:20,p18:失败"``
    """
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for raw in _raw_partition_records(results):
        name = str(raw.get("partition") or "").strip()
        if not name:
            continue
        raw_status = _normalize_status_word(raw.get("raw_status"))
        rows = max(0, _to_int(raw.get("rows"), 0))
        raw_reason = str(raw.get("raw_reason") or "").strip()

        if raw_status == STATUS_PENDING:
            status = STATUS_PENDING
        elif raw_status == STATUS_FAILED:
            status = STATUS_FAILED
        elif raw_status == STATUS_EMPTY:
            status = STATUS_EMPTY
        elif rows > 0:
            status = STATUS_SUCCESS
        elif raw_status == STATUS_SUCCESS:
            # “成功”但没有行数，仍然是无产出，不能被下游接收
            status = STATUS_EMPTY
        else:
            status = STATUS_EMPTY

        if status == STATUS_SUCCESS:
            reason = raw_reason or f"分区回填成功，写入 {rows} 行"
        elif status == STATUS_FAILED:
            reason = raw_reason or "分区执行失败，产出不可用"
        elif status == STATUS_EMPTY:
            reason = raw_reason or "分区无实际产出，拒绝交付"
        else:
            reason = raw_reason or "分区结果尚未确定"

        record = {
            "partition": name,
            "status": status,
            "rows": rows,
            "accepted_rows": rows if status == STATUS_SUCCESS else 0,
            "deliverable": status == STATUS_SUCCESS,
            "reason": reason,
            "raw_status": raw.get("raw_status"),
        }
        if name not in merged:
            order.append(name)
        merged[name] = record
    return [merged[name] for name in order]


def _partition_sort_key(name: str) -> tuple[int, str]:
    digits = "".join(ch for ch in name if ch.isdigit())
    return (int(digits) if digits else 10 ** 9, name)


# ------------------------------------------------------------------
# 可用回填数据范围 + 交付边界
# ------------------------------------------------------------------

def plan_backfill_delivery(
    results: Any,
    delivery_mode: Any = None,
    configured_mode: Any = None,
    *,
    mode_is_explicit: bool = False,
) -> dict[str, Any]:
    """根据逐分区实际结果计算可用回填数据范围和交付边界。

    ``mode_is_explicit`` 为真时，传入的口径优先于已存配置；否则多个来源按
    优先级（整批确认 > 按分区交付）统一。
    """
    records = normalize_partition_results(results)
    records.sort(key=lambda item: _partition_sort_key(item["partition"]))
    explicit = normalize_delivery_mode(delivery_mode) if mode_is_explicit else None
    if mode_is_explicit and delivery_mode is not None and explicit is None:
        # 无法识别的显式口径不能被静默当成默认值。
        choice = resolve_delivery_mode(None)
        choice = {
            **choice,
            "reason": f"无法识别的交付口径：{delivery_mode}，已按{choice['label']}处理",
        }
    elif explicit:
        choice = _explicit_choice(explicit)
    else:
        choice = resolve_delivery_mode(delivery_mode, configured_mode)
    mode = choice["mode"]

    successful = [item for item in records if item["status"] == STATUS_SUCCESS]
    failed = [item for item in records if item["status"] == STATUS_FAILED]
    empty = [item for item in records if item["status"] == STATUS_EMPTY]
    pending = [item for item in records if item["status"] == STATUS_PENDING]
    rejected = failed + empty

    total = len(records)
    complete = total > 0 and len(successful) == total
    if mode == MODE_BATCH:
        # 整批确认：只有全部分区成功才统一交付，否则成功分区也留在回填侧等待
        delivered = successful if complete else []
    else:
        # 按分区交付：只接收完整成功的分区，失败 / 无产出 / 待定分区都不交付
        delivered = list(successful)
    delivered_names = [item["partition"] for item in delivered]
    delivered_set = set(delivered_names)
    # 留在回填侧的分区只包括“可用但本批不交付”的成功分区；失败 / 无产出
    # 分区走拒绝依据，不进入等待队列。
    held = [item for item in successful if item["partition"] not in delivered_set]

    usable_rows = sum(item["accepted_rows"] for item in successful)
    delivered_rows = sum(item["accepted_rows"] for item in delivered)
    success_names = [item["partition"] for item in successful]
    failed_names = [item["partition"] for item in failed]
    empty_names = [item["partition"] for item in empty]
    pending_names = [item["partition"] for item in pending]
    evidence = [{
        "partition": item["partition"],
        "status": item["status"],
        "rows": item["rows"],
        "accepted_rows": item["accepted_rows"],
        "usable": item["status"] == STATUS_SUCCESS,
        "deliverable": item["deliverable"],
        "delivered": item["partition"] in delivered_set,
        "reason": item["reason"],
        "basis": "partition_result",
    } for item in records]

    rejected_partitions = [{
        "partition": item["partition"],
        "status": item["status"],
        "rows": item["rows"],
        "accepted_rows": 0,
        "usable": False,
        "reason": item["reason"],
        "basis": "partition_result",
        "deliverable": False,
    } for item in rejected]

    merge_todos: list[dict[str, Any]] = []
    if mode == MODE_BATCH and not complete:
        held_names = [item["partition"] for item in held]
        waiting = "、".join(success_names) if success_names else "无"
        merge_todos.append({
            "type": "batch_confirm",
            "todo": (
                f"整批交付待确认：{('、'.join(item['partition'] for item in records))} 尚未全部成功，"
                f"{waiting} 留在回填侧等待"
            ),
            "partitions": held_names,
            "status": "待整批确认",
            "reason": "整批确认要求同批分区全部成功后才统一交付",
        })
    for item in failed:
        merge_todos.append({
            "type": "missing_delivery",
            "todo": f"{item['partition']} 尚缺交付结论",
            "partition": item["partition"],
            "status": item["status"],
            "reason": item["reason"],
        })
    for item in empty:
        merge_todos.append({
            "type": "no_output",
            "todo": f"{item['partition']} 无产出，需重新回填后再交付",
            "partition": item["partition"],
            "status": item["status"],
            "reason": item["reason"],
        })
    for item in pending:
        merge_todos.append({
            "type": "pending_partition",
            "todo": f"{item['partition']} 尚未回填完成",
            "partition": item["partition"],
            "status": item["status"],
            "reason": item["reason"],
        })

    progress = 0 if total == 0 else int(round(100 * len(successful) / total))
    if complete:
        progress = 100

    if complete:
        boundary_reason = "全部分区成功，按当前口径统一交付"
        next_scope = success_names
    elif mode == MODE_BATCH:
        boundary_reason = (
            f"整批确认：{'、'.join(item['partition'] for item in records)} 未全部成功，"
            f"{'、'.join(success_names) or '成功分区'}留在回填侧等待，下一批不读取本批分区"
        )
        next_scope = []
    else:
        boundary_reason = (
            f"按分区交付：接收完整分区{'、'.join(delivered_names) or '（无）'}，"
            f"拒绝{'、'.join(item['partition'] for item in rejected) or '（无）'}"
        )
        next_scope = delivered_names

    delivery_boundary = {
        "mode": mode,
        "label": choice["label"],
        "priority": choice["priority"],
        "mode_conflict": choice["conflict"],
        "mode_candidates": choice["candidates"],
        "mode_reason": choice["reason"],
        "complete": complete,
        "delivery_ready": bool(delivered_names),
        "delivered_partitions": delivered_names,
        "held_partitions": [item["partition"] for item in held],
        "next_batch_read_scope": next_scope,
        "rejected_partitions": rejected_partitions,
        "rejected_failed": failed_names,
        "rejected_no_output": empty_names,
        "holds_partition": bool(held),
        "reason": boundary_reason,
    }

    suggested_status: str | None
    if total == 0:
        suggested_status = None
    elif complete:
        suggested_status = "已完成"
    elif failed or empty:
        suggested_status = "回填失败"
    else:
        suggested_status = "回填中"

    return {
        "partitions": records,
        "total_partitions": total,
        "successful_partitions": success_names,
        "failed_partitions": failed_names,
        "empty_partitions": empty_names,
        "pending_partitions": pending_names,
        "rejected_partitions": rejected_partitions,
        "delivered_partitions": delivered_names,
        "held_partitions": [item["partition"] for item in held],
        # 可用回填数据范围由逐分区的实际结果组成：成功分区即便因整批确认暂时
        # 留在回填侧，也仍然属于可用范围；交付边界单独决定本批是否交付。
        "usable_range": success_names,
        "usable_backfill_range": success_names,
        "usable_rows": usable_rows,
        "delivered_rows": delivered_rows,
        "completed_rows": usable_rows,
        "progress_percent": progress,
        "complete": complete,
        "delivery_mode": mode,
        "delivery_mode_label": choice["label"],
        "delivery_boundary": delivery_boundary,
        "next_batch_read_scope": next_scope,
        "merge_todos": merge_todos,
        "evidence": evidence,
        "suggested_status": suggested_status,
    }


# ------------------------------------------------------------------
# 回填依据与真实流记录的合并
# ------------------------------------------------------------------

def reconcile_backfill_with_stream(
    stream_records: Any = None,
    backfill_records: Any = None,
    *,
    delivered_rows: int = 0,
    delivered_partitions: Iterable[str] | None = None,
    dedup_key: str = "id",
) -> dict[str, Any]:
    """把可用回填数据范围与真实流记录合并。

    无产出的旧回填范围不能覆盖真实流记录：当可用范围为空时原样保留流记录，
    并明确标记 ``overwritten=False``。
    """
    stream = list(stream_records) if isinstance(stream_records, (list, tuple)) else []
    backfill = list(backfill_records) if isinstance(backfill_records, (list, tuple)) else []
    delivered = [str(item) for item in (delivered_partitions or [])]

    if delivered_rows <= 0 or not delivered:
        return {
            "stream_rows": len(stream),
            "backfill_rows": 0,
            "reconciled_total": len(stream),
            "duplicates_resolved": 0,
            "preserved_stream_rows": len(stream),
            "stream_preserved": True,
            "overwritten": False,
            "backfill_applied": False,
            "reason": "可用回填数据范围为空，保留真实流记录，不用无产出的旧范围覆盖",
        }

    if backfill:
        stream_keys = {
            str(record.get(dedup_key))
            for record in stream
            if isinstance(record, Mapping) and record.get(dedup_key) is not None
        }
        appended = [
            record for record in backfill
            if not (isinstance(record, Mapping) and str(record.get(dedup_key)) in stream_keys)
        ]
        duplicates = len(backfill) - len(appended)
        reconciled_total = len(stream) + delivered_rows
    else:
        duplicates = 0
        appended = []
        reconciled_total = len(stream) + delivered_rows

    return {
        "stream_rows": len(stream),
        "backfill_rows": delivered_rows,
        "reconciled_total": reconciled_total,
        "duplicates_resolved": duplicates,
        "appended_backfill_rows": len(appended),
        "preserved_stream_rows": len(stream),
        "stream_preserved": True,
        "overwritten": False,
        "backfill_applied": True,
        "reason": "流记录优先保留，回填只补充不重叠部分",
    }
