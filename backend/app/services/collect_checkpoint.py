"""采集位点管理：采集进度记录、位点提交、回滚和过期管理。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CollectCheckpointService(BaseModuleService):
    MODULE = "collect_checkpoint"
    ENTITY = "位点记录"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["记录编号", "关联源", "文件偏移", "处理行号", "位点时间", "校验摘要", "提交状态", "回滚位点"]
    REQUIRED_FIELDS = ["记录编号", "关联源", "文件偏移"]
    STATUS_ORDER = ["采集中", "已提交", "已回滚", "已过期"]
    ACTION_RULES = {"确认提交": "已提交", "发起回滚": "已回滚", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    def save_checkpoint(self, source_key: str, offset: int, line_number: int) -> dict[str, Any]:
        """保存采集位点状态。"""
        state = {
            "source_key": source_key,
            "offset": offset,
            "line_number": line_number,
            "status": "active",
            "updated_at": time.time(),
        }
        self._write_checkpoint(source_key, state)

        ckpt_dir = self._data_dir("checkpoints")
        self._write_jsonl(ckpt_dir / "checkpoint_history.jsonl", [{
            "source_key": source_key, "offset": offset,
            "line_number": line_number, "at": time.time(), "action": "save",
        }])
        return state

    def rollback_to(self, source_key: str, to_offset: int) -> dict[str, Any]:
        """将指定位点回滚到指定偏移位置。"""
        cp = self._read_checkpoint(source_key)
        if cp is None:
            return {"error": f"位点 {source_key} 不存在"}

        cp["offset"] = to_offset
        cp["status"] = "rolled_back"
        cp["rolled_back_at"] = time.time()
        cp["previous_offset"] = cp.get("previous_offset", cp["offset"])
        self._write_checkpoint(source_key, cp)

        self._write_jsonl(self._data_dir("checkpoints") / "checkpoint_history.jsonl", [{
            "source_key": source_key, "offset": to_offset, "at": time.time(), "action": "rollback",
        }])
        return cp

    def get_checkpoint_state(self, source_key: str) -> dict[str, Any]:
        """获取指定源的位点信息。"""
        cp = self._read_checkpoint(source_key)
        if cp is None:
            return {"source_key": source_key, "status": "no_checkpoint", "offset": 0}
        return cp

    def list_all_checkpoints(self) -> list[dict[str, Any]]:
        """列出所有位点状态。"""
        all_cps = []
        for src in self._collect_sources():
            key = src["源编号"]
            cp = self._read_checkpoint(key)
            if cp:
                all_cps.append(cp)
        return all_cps

    def commit_checkpoint(self, source_key: str, establish_batch: bool = False,
                          batch_sources: list[str] | None = None) -> dict[str, Any]:
        """提交采集位点，记录已确认输入范围并自动触发下游格式化输出。

        establish_batch=True 时，把已确认输入范围交给格式输出，
        据此建立多源导出批次清单（仅建立，不执行导出）。
        """
        cp = self._read_checkpoint(source_key)
        if cp is None:
            return {"error": f"位点 {source_key} 不存在"}
        cp["status"] = "committed"
        cp["committed_at"] = time.time()
        self._write_checkpoint(source_key, cp)

        # 写入位点历史
        self._write_jsonl(self._data_dir("checkpoints") / "checkpoint_history.jsonl", [{
            "source_key": source_key, "offset": cp.get("offset", 0),
            "at": time.time(), "action": "commit",
        }])

        # 记录已确认位点快照：格式输出以此作为多源导出批次的已确认输入范围。
        # 快照独立于日常采集位点，后续继续采集不会覆盖已确认范围。
        confirmed = {
            "source_key": source_key,
            "line_number": int(cp.get("line_number", 0) or 0),
            "offset": int(cp.get("offset", 0) or 0),
            "status": "已确认",
            "confirmed_at": time.time(),
        }
        self._write_checkpoint(f"{source_key}_confirmed", confirmed)

        result = {
            "source_key": source_key,
            "committed_at": time.time(),
            "offset": cp.get("offset", 0),
            "line_number": cp.get("line_number", 0),
            "confirmed_range": {
                "end_line": confirmed["line_number"],
                "end_offset": confirmed["offset"],
            },
        }

        # 提交后自动触发格式输出
        export_r = self._trigger_export_for_source(source_key)
        if "error" not in export_r:
            result["auto_export"] = export_r

        # 同时触发质量诊断
        quality_r = self._trigger_quality_diagnosis(source_key)
        if "error" not in quality_r:
            result["auto_quality"] = quality_r

        # 提交新位点时建立多源导出批次清单（采集位点 -> 格式输出的范围交接）
        if establish_batch:
            try:
                cls = self._safe_import("app.services.format_output", "FormatOutputService")
                result["auto_batch"] = cls().create_export_batch(
                    source_keys=batch_sources, trigger=f"commit:{source_key}")
            except Exception as e:  # noqa: BLE001 - 自动触发不阻断位点提交
                result["auto_batch"] = {"error": str(e)}

        return result

    def get_confirmed_range(self, source_key: str) -> dict[str, Any] | None:
        """读取指定源已确认的采集输入范围；未提交过位点则返回 None。"""
        conf = self._read_checkpoint(f"{source_key}_confirmed")
        if conf is None:
            return None
        return {
            "source_key": source_key,
            "end_line": int(conf.get("line_number", 0) or 0),
            "end_offset": int(conf.get("offset", 0) or 0),
            "confirmed_at": conf.get("confirmed_at"),
            "status": conf.get("status", "已确认"),
        }

    def confirmed_ranges(self, source_keys: list[str] | None = None) -> dict[str, dict[str, Any] | None]:
        """批量读取已确认输入范围，交给格式输出建立多源导出批次清单。"""
        if source_keys is None:
            source_keys = [s["源编号"] for s in self._collect_sources()]
        return {key: self.get_confirmed_range(key) for key in source_keys}
        """验证采集位点的校验摘要。"""
        cp = self._read_checkpoint(source_key)
        if cp is None:
            return {"source_key": source_key, "status": "no_checkpoint", "match": False}

        actual = cp.get("checksum", "")
        match = actual == expected_checksum
        cp["last_verified_at"] = time.time()
        cp["last_verified_match"] = match
        self._write_checkpoint(source_key, cp)

        return {"source_key": source_key, "match": match, "expected": expected_checksum, "actual": actual}