"""回填交付边界测试：逐分区结果、交付口径优先级、状态流转和幂等写入。"""
from __future__ import annotations

import sys
sys.path.insert(0, '.')

import pytest

from app.backfill import plan_backfill_delivery
from app.services import service_for
from app.services.backfill_pipeline import BackfillPipelineService
from app.store import store


P17 = {"partition": "p17", "rows": 20, "status": "成功"}
P18_FAIL = {"partition": "p18", "rows": 0, "status": "失败"}
P18_OK = {"partition": "p18", "rows": 7, "status": "成功"}


def _reset(entry_id: int = 1) -> None:
    row = store.find("backfill_pipeline", entry_id)
    for key in ("分区结果", "写入台账", "执行记录", "合并待办", "交付模式", "交付模式说明"):
        row.pop(key, None)
    row.update(status="排队中", pending=True, abnormal=False, 完成行数=0, 失败分区="无")


def _reset_merge(entry_id: int = 1) -> None:
    row = store.find("batch_merge", entry_id)
    for key in ("合并待办", "交付边界", "上次合并", "交付模式", "整批确认"):
        row.pop(key, None)


def _backfill():
    return service_for("backfill_pipeline")


def _merge():
    return service_for("batch_merge")


class TestProgressFromPartitionResults:
    def test_failed_partition_never_reports_100_percent(self):
        _reset()
        detail = _backfill().execute_partitions(1, [P17, P18_FAIL], "partition")
        assert detail["progress_percent"] == 50
        assert detail["complete"] is False
        assert detail["completed_rows"] == 20
        assert detail["usable_rows"] == 20
        assert detail["usable_backfill_range"] == ["p17"]
        assert detail["failed_partition_names"] == ["p18"]

    def test_all_success_reports_100_percent(self):
        _reset()
        detail = _backfill().execute_partitions(1, [P17, P18_OK], "partition")
        assert detail["progress_percent"] == 100
        assert detail["complete"] is True
        assert detail["completed_rows"] == 27

    def test_detail_keeps_rejected_evidence(self):
        _reset()
        detail = _backfill().execute_partitions(1, [P17, P18_FAIL], "partition")
        rejected = {item["partition"]: item for item in detail["rejected_partitions"]}
        assert rejected["p18"]["basis"] == "partition_result"
        assert rejected["p18"]["usable"] is False
        assert rejected["p18"]["accepted_rows"] == 0
        assert all(item["basis"] == "partition_result" for item in detail["usable_range_detail"])


class TestDeliveryBoundaries:
    def test_partition_mode_delivers_complete_partitions(self):
        _reset()
        detail = _backfill().execute_partitions(1, [P17, P18_FAIL], "partition")
        boundary = detail["delivery_boundary"]
        assert boundary["delivered_partitions"] == ["p17"]
        assert boundary["rejected_failed"] == ["p18"]
        assert boundary["next_batch_read_scope"] == ["p17"]

    def test_batch_mode_holds_successful_partitions(self):
        _reset()
        detail = _backfill().execute_partitions(1, [P17, P18_FAIL], "batch")
        boundary = detail["delivery_boundary"]
        assert boundary["delivered_partitions"] == []
        assert boundary["held_partitions"] == ["p17"]
        assert boundary["next_batch_read_scope"] == []
        # 可用范围仍记录 p17 的实际结果，但本批不交付
        assert detail["usable_backfill_range"] == ["p17"]

    def test_batch_mode_releases_after_all_success(self):
        _reset()
        _backfill().execute_partitions(1, [P17, P18_FAIL], "batch")
        detail = _backfill().execute_partitions(1, [P18_OK], "batch")
        assert detail["delivery_boundary"]["delivered_partitions"] == ["p17", "p18"]
        assert detail["delivery_boundary"]["held_partitions"] == []
        assert detail["delivery_boundary"]["complete"] is True

    def test_no_output_partition_rejected_in_both_modes(self):
        for mode in ("partition", "batch"):
            _reset()
            detail = _backfill().execute_partitions(
                1,
                [
                    P17,
                    {"partition": "p19", "rows": 0, "status": "成功"},
                    {"partition": "p20", "rows": 0, "status": "无产出"},
                ],
                mode,
            )
            assert detail["empty_partitions"] == ["p19", "p20"]
            assert "p19" not in detail["delivery_boundary"]["delivered_partitions"]
            assert "p20" not in detail["delivery_boundary"]["delivered_partitions"]
            for item in detail["rejected_partitions"]:
                if item["partition"] in ("p19", "p20"):
                    assert item["deliverable"] is False
                    assert item["basis"] == "partition_result"

    def test_batch_mode_writes_merge_todo_for_missing_delivery(self):
        _reset()
        detail = _backfill().execute_partitions(1, [P17, P18_FAIL], "batch")
        todos = detail["merge_todos"]
        assert any(item["type"] == "missing_delivery" and "p18" in item["todo"] for item in todos)
        assert any(item["type"] == "batch_confirm" for item in todos)

    def test_mode_priority_batch_wins(self):
        result = plan_backfill_delivery([P17, P18_FAIL], "partition", "batch")
        assert result["delivery_boundary"]["mode"] == "batch"
        assert result["delivery_boundary"]["mode_conflict"] is True
        assert result["delivered_partitions"] == []
        assert result["held_partitions"] == ["p17"]

    def test_next_batch_scope_follows_priority(self):
        partition = plan_backfill_delivery([P17, P18_FAIL], "partition")
        assert partition["next_batch_read_scope"] == ["p17"]
        batch = plan_backfill_delivery([P17, P18_FAIL], "batch")
        assert batch["next_batch_read_scope"] == []


class TestMergeHandover:
    def test_partition_mode_merge_receives_p17_and_records_p18_todo(self):
        _reset()
        _reset_merge()
        _backfill().execute_partitions(1, [P17, P18_FAIL], "partition")
        result = _merge().domain_merge_batch_stream(1, backfill_id=1, delivery_mode="partition")
        assert result["delivery_mode"] == "partition"
        assert result["accepted_partitions"] == ["p17"]
        assert [item["partition"] for item in result["rejected_partitions"]] == ["p18"]
        assert any("p18" in item["todo"] for item in result["merge_todos"])
        assert store.find("batch_merge", 1)["合并待办"] == result["merge_todos"]

    def test_batch_mode_merge_holds_p17_and_records_todo(self):
        _reset()
        _reset_merge()
        _backfill().execute_partitions(1, [P17, P18_FAIL], "batch")
        result = _merge().domain_merge_batch_stream(1, backfill_id=1, delivery_mode="batch")
        assert result["accepted_partitions"] == []
        assert result["held_partitions"] == ["p17"]
        assert result["next_batch_read_scope"] == []
        assert result["merge_pending"]["has_todos"] is True
        assert any("p17" in item["todo"] for item in result["merge_todos"])

    def test_merge_never_accepts_no_output_partitions(self):
        _reset()
        _reset_merge()
        _backfill().execute_partitions(
            1, [P17, {"partition": "p21", "rows": 0, "status": "无产出"}], "partition",
        )
        result = _merge().domain_merge_batch_stream(1, backfill_id=1, delivery_mode="partition")
        assert result["accepted_partitions"] == ["p17"]
        assert result["rejected_no_output"] == ["p21"]
        assert "p21" not in result["delivered_partitions"]

    def test_merge_precedence_conflict_prefers_batch(self):
        _reset()
        _reset_merge()
        _backfill().execute_partitions(1, [P17, P18_FAIL], "partition")
        result = _merge().evaluate_delivery(1, backfill_id=1, delivery_mode="batch")
        assert result["delivery_mode"] == "batch"
        assert result["delivered_partitions"] == []
        assert result["held_partitions"] == ["p17"]


class TestStreamSourceConflict:
    def test_empty_backfill_range_does_not_overwrite_stream(self):
        _reset()
        _reset_merge()
        _backfill().execute_partitions(1, [{"partition": "p19", "rows": 0, "status": "无产出"}], "partition")
        stream = [{"id": 1, "val": "real-a"}, {"id": 2, "val": "real-b"}]
        result = _merge().evaluate_delivery(1, backfill_id=1, stream_records=stream)
        assert result["backfill_rows"] == 0
        assert result["overwritten"] is False
        assert result["stream_preserved"] is True
        assert result["reconciled_total"] == len(stream)

    def test_delivered_range_appends_without_overwriting_stream(self):
        _reset()
        _reset_merge()
        _backfill().execute_partitions(1, [P17], "partition")
        stream = [{"id": 1, "val": "real-a"}]
        result = _merge().evaluate_delivery(1, backfill_id=1, stream_records=stream)
        assert result["overwritten"] is False
        assert result["backfill_applied"] is True
        assert result["reconciled_total"] == 1 + 20


class TestStatusTransitions:
    def test_cannot_skip_states(self):
        _reset()
        service = _backfill()
        entry, message = service.run_action(1, "完成回填")
        assert entry is None
        assert "不能从排队中直接流转到已完成" in message
        assert store.find("backfill_pipeline", 1)["status"] == "排队中"

    def test_failure_then_recovery_keeps_writes(self):
        _reset()
        detail = _backfill().execute_partitions(1, [P17, P18_FAIL], "partition")
        assert detail["status"] == "回填失败"
        assert detail["write_ledger"]["p17"] == {"rows": 20, "write_count": 1}

        recovered = _backfill().recover(1, [P18_OK], "partition")
        assert recovered["status"] == "已完成"
        assert recovered["write_ledger"] == {
            "p17": {"rows": 20, "write_count": 1},
            "p18": {"rows": 7, "write_count": 1},
        }

    def test_repeated_recovery_writes_each_partition_once(self):
        _reset()
        _backfill().execute_partitions(1, [P17, P18_FAIL], "partition")
        first = _backfill().recover(1, [P18_OK], "partition")
        second = _backfill().recover(1, [P18_OK], "partition")
        third = _backfill().execute_partitions(1, [P17, P18_OK], "partition")
        for detail in (first, second, third):
            assert detail["write_ledger"]["p17"] == {"rows": 20, "write_count": 1}
            assert detail["write_ledger"]["p18"] == {"rows": 7, "write_count": 1}
            assert detail["usable_rows"] == 27
        assert store.find("backfill_pipeline", 1)["分区结果"][0]["rows"] == 20

    def test_duplicate_submission_does_not_double_count(self):
        _reset()
        _backfill().execute_partitions(1, [P17], "partition")
        detail = _backfill().execute_partitions(1, [P17], "partition")
        assert detail["write_ledger"]["p17"] == {"rows": 20, "write_count": 1}
        assert detail["usable_rows"] == 20

    def test_seed_placeholders_still_describe_without_crash(self):
        service = _backfill()
        detail = service.domain_backfill(3)
        assert detail["ok"] is True
        assert detail["progress_percent"] == 0


class TestChainHandover:
    def test_schema_evolve_chain_does_not_mark_failed_backfill_complete(self):
        from fastapi.testclient import TestClient
        from app.main import app

        _reset()
        _reset_merge()
        client = TestClient(app)
        _backfill().execute_partitions(1, [P17, P18_FAIL], "partition")
        resp = client.post(
            "/api/chains/schema-evolve",
            params={
                "schema_id": 1, "backfill_id": 1, "merge_id": 1,
                "checkpoint_id": 1, "delivery_mode": "partition",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        summary = data["summary"]
        assert summary["backfill_progress_percent"] == 50
        assert summary["usable_backfill_range"] == ["p17"]
        assert summary["delivered_partitions"] == ["p17"]
        assert summary["delivery_complete"] is False
        assert any("p18" in item["todo"] for item in summary["merge_todos"])


class TestRecoveryWithoutResults:
    def test_recover_without_results_does_not_invent_rows(self):
        _reset()
        _backfill().execute_partitions(1, [P17, P18_FAIL], "partition")
        before = _backfill().describe(1, "partition")
        recovered = _backfill().recover(1, None, "partition")
        assert recovered["status"] == "回填失败"
        assert recovered["progress_percent"] == before["progress_percent"] == 50
        assert recovered["usable_backfill_range"] == ["p17"]
        assert recovered["write_ledger"]["p17"] == {"rows": 20, "write_count": 1}
        assert "p18" in recovered["failed_partition_names"]


class TestDeclaredFieldFallback:
    def test_declared_fields_do_not_claim_completion(self):
        row = store.find("backfill_pipeline", 2)
        for key in ("分区结果", "写入台账", "执行记录", "交付模式"):
            row.pop(key, None)
        row.update(status="回填失败", 完成行数=20, 失败分区="p18", 回填范围="p17,p18")
        detail = _backfill().describe(2, "partition")
        assert detail["progress_percent"] == 50
        assert detail["usable_backfill_range"] == ["p17"]
        assert detail["usable_range_detail"][0]["basis"] == "declared_fields"
        assert [item["partition"] for item in detail["rejected_partitions"]] == ["p18"]

    def test_placeholder_seed_fields_are_not_partitions(self):
        detail = _backfill().domain_backfill(3)
        assert detail["progress_percent"] == 0
        assert detail["usable_backfill_range"] == []
        assert detail["status_consistent"] is False


class TestExplicitModeOverride:
    def test_merge_can_choose_partition_when_backfill_default_is_batch(self):
        _reset()
        _reset_merge()
        _backfill().execute_partitions(1, [P17, P18_FAIL], "batch")
        assert store.find("backfill_pipeline", 1)["交付模式"] == "batch"
        result = _merge().evaluate_delivery(1, backfill_id=1, delivery_mode="按分区交付")
        assert result["delivery_mode"] == "partition"
        assert result["accepted_partitions"] == ["p17"]
        assert result["delivery_boundary"]["mode_conflict"] is True
        assert "覆盖已存配置" in result["delivery_boundary"]["mode_reason"]

    def test_backfill_execute_without_explicit_mode_keeps_stored_batch(self):
        _reset()
        _backfill().execute_partitions(1, [P17, P18_FAIL], "batch")
        detail = _backfill().execute_partitions(1, [P18_FAIL], None)
        assert detail["delivery_mode"] == "batch"
        assert detail["held_partitions"] == ["p17"]


class TestInvalidDeliveryMode:
    def test_unknown_mode_is_rejected_not_defaulted(self):
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        resp = client.get("/api/backfill/1/detail", params={"delivery_mode": "完整分区"})
        assert resp.status_code == 400
        assert "无法识别的交付口径" in resp.json()["detail"]

    def test_chain_rejects_unknown_mode(self):
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        resp = client.post(
            "/api/chains/schema-evolve",
            params={
                "schema_id": 1, "backfill_id": 1, "merge_id": 1,
                "checkpoint_id": 1, "delivery_mode": "all",
            },
        )
        assert resp.status_code == 400


class TestManualCompletionGuard:
    def test_manual_complete_rejected_when_partitions_incomplete(self):
        _reset()
        _backfill().execute_partitions(1, [P17, P18_FAIL], "partition")
        _backfill().run_action(1, "恢复回填")
        entry, message = _backfill().run_action(1, "完成回填")
        assert entry is None
        assert "逐分区结果尚未全部成功" in message
        assert store.find("backfill_pipeline", 1)["status"] == "回填中"

    def test_manual_complete_rejected_after_cancel(self):
        _reset()
        _backfill().run_action(1, "取消回填")
        _backfill().run_action(1, "恢复回填")
        entry, message = _backfill().run_action(1, "完成回填")
        assert entry is None
        assert "尚未全部成功" in message
