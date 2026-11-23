"""格式输出：将标准化数据按输出格式配置写入指定路径。"""
from __future__ import annotations

import json
import os
import time
import gzip
import hashlib
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ExportInterrupted(Exception):
    """导出中断：部分数据已落盘，但成品未提交，批次条目保持未交付状态。"""

    def __init__(self, message: str, partial: int = 0) -> None:
        super().__init__(message)
        self.partial = partial


class FormatOutputService(BaseModuleService):
    MODULE = "format_output"
    ENTITY = "输出配置"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["配置编号", "关联源", "输出格式", "输出路径", "压缩方式", "分片大小", "命名规则", "配置版本"]
    REQUIRED_FIELDS = ["配置编号", "关联源", "输出格式"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用配置": "已停用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    def export_source(self, source_key: str, format_type: str = "jsonl") -> dict[str, Any]:
        """将指定日志源的标准化数据导出为指定格式。"""
        mapped_file = self._data_dir("logs") / f"{source_key}_mapped.jsonl"
        if not mapped_file.exists():
            return {"error": f"日志源 {source_key} 无映射数据，请先执行字段映射", "exported": 0}

        records = self._read_jsonl(mapped_file)
        exports_dir = self._data_dir("exports")

        if format_type == "jsonl":
            output_file = exports_dir / f"{source_key}_export.jsonl"
            written = self._write_jsonl(output_file, records)

        elif format_type == "json":
            output_file = exports_dir / f"{source_key}_export.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2, default=str)
            written = len(records)

        elif format_type == "gz":
            output_file = exports_dir / f"{source_key}_export.jsonl.gz"
            with gzip.open(output_file, "wt", encoding="utf-8", compresslevel=6) as f:
                for rec in records:
                    f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            written = len(records)

        elif format_type == "csv":
            output_file = exports_dir / f"{source_key}_export.csv"
            if records:
                keys = list(records[0].get("standardized", records[0]).keys())
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(",".join(keys) + "\n")
                    for rec in records:
                        std = rec.get("standardized", rec)
                        f.write(",".join(str(std.get(k, "")) for k in keys) + "\n")
                written = len(records)
            else:
                written = 0
        else:
            return {"error": f"不支持的输出格式: {format_type}"}

        self._write_checkpoint(f"{source_key}_export", {
            "format": format_type,
            "records": written,
            "output_file": str(output_file),
            "exported_at": time.time(),
        })

        return {
            "source_key": source_key,
            "format": format_type,
            "records": written,
            "output_file": str(output_file),
        }

    def export_batch(self, source_keys: list[str], format_type: str = "jsonl") -> dict[str, Any]:
        """批量导出多个日志源的数据。"""
        results = []
        total = 0
        for key in source_keys:
            r = self.export_source(key, format_type)
            results.append(r)
            total += r.get("records", 0)
        return {"total": total, "sources": len(results), "details": results}

    def get_export_manifest(self, source_key: str) -> dict[str, Any]:
        """获取指定日志源的导出清单。"""
        cp = self._read_checkpoint(f"{source_key}_export")
        mapped_cp = self._read_checkpoint(f"{source_key}_map")
        parsed_cp = self._read_checkpoint(f"{source_key}_parse")
        collect_cp = self._read_checkpoint(source_key)

        return {
            "source_key": source_key,
            "collect": collect_cp,
            "parse": parsed_cp,
            "mapping": mapped_cp,
            "export": cp,
        }

    # ------------------------------------------------------------------
    # 多源导出批次清单
    #
    # 采集位点模块提交位点后，把"已确认输入范围"交给格式输出；格式输出据此
    # 冻结每个源的 [start_line, end_line) 采集范围、实际读取的源文件和规则
    # 版本，写入批次清单。执行导出时逐源发放成品文件：
    #   - 已交付条目在重试/重复下载时直接复用，不会重新生成（幂等）；
    #   - 中断的条目保持未交付，接续导出仍按原批冻结的范围读取，
    #     不会把批次建立后新采集的记录混进原批；
    #   - 每份成品带校验摘要，可随时与批次清单对账。
    # ------------------------------------------------------------------

    ENTRY_DELIVERED = "已交付"
    ENTRY_PENDING = "待导出"
    ENTRY_FAILED = "导出失败"
    ENTRY_NO_CHECKPOINT = "无确认位点"
    ENTRY_NO_NEW_DATA = "无新增数据"

    # -- 清单读写 --

    def _batches_dir(self) -> Path:
        return self._data_dir("exports") / "batches"

    def _manifest_path(self, batch_id: str) -> Path:
        return self._batches_dir() / f"{batch_id}.json"

    def _batch_artifact_dir(self, batch_id: str) -> Path:
        return self._batches_dir() / batch_id

    def _save_manifest(self, manifest: dict[str, Any]) -> None:
        manifest["updated_at"] = time.time()
        path = self._manifest_path(manifest["batch_id"])
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2, default=str)
        os.replace(tmp, path)

    def _load_manifest(self, batch_id: str) -> dict[str, Any] | None:
        path = self._manifest_path(batch_id)
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _next_batch_id(self) -> str:
        state = self._read_checkpoint("export_batch_seq") or {"seq": 0}
        state["seq"] = int(state.get("seq", 0)) + 1
        self._write_checkpoint("export_batch_seq", state)
        return f"EXB-{time.strftime('%Y%m%d')}-{state['seq']:04d}"

    def _resolve_rule_version(self, source_key: str) -> str:
        """解析该源当前生效的输出配置版本，其次解析规则版本。"""
        for row in store.rows("format_output"):
            if row.get("关联源") == source_key and row.get("status") == "已生效":
                return str(row.get("配置版本") or "v1.0")
        for row in store.rows("log_parser"):
            if row.get("status") == "已生效" and source_key in str(row.get("关联源", "")):
                return str(row.get("规则版本") or "v1.0")
        return "v1.0"

    def _export_cursor(self, source_key: str) -> int:
        state = self._read_checkpoint(f"{source_key}_export_cursor") or {}
        return int(state.get("delivered_upto_line", 0) or 0)

    def _advance_export_cursor(self, source_key: str, end_line: int) -> None:
        self._write_checkpoint(f"{source_key}_export_cursor", {
            "source_key": source_key,
            "delivered_upto_line": int(end_line),
            "updated_at": time.time(),
        })

    # -- 建立批次 --

    def create_export_batch(self, source_keys: list[str] | None = None,
                            format_type: str = "jsonl",
                            trigger: str = "manual") -> dict[str, Any]:
        """根据各源已确认的采集位点建立多源导出批次清单。

        每个可用条目冻结：实际读取的源文件、采集范围 [start_line, end_line)
        和规则版本；无确认位点或无新增数据的源记入清单但不发放文件。
        """
        from app.services.collect_checkpoint import CollectCheckpointService

        ckpt_svc = CollectCheckpointService()
        if source_keys is None:
            source_keys = [s["源编号"] for s in self._collect_sources()]
        confirmed = ckpt_svc.confirmed_ranges(source_keys)

        entries: list[dict[str, Any]] = []
        for key in source_keys:
            source_file = self._data_dir("logs") / f"{key}_mapped.jsonl"
            entry: dict[str, Any] = {
                "source_key": key,
                "source_file": str(source_file),
                "rule_version": self._resolve_rule_version(key),
                "range": None,
                "status": self.ENTRY_PENDING,
                "artifact": None,
                "records": 0,
                "checksum": None,
                "delivered_at": None,
                "attempts": 0,
                "error": None,
                "last_error": None,
                "partial_records": 0,
            }
            conf = confirmed.get(key)
            if conf is None:
                entry["status"] = self.ENTRY_NO_CHECKPOINT
                entry["error"] = "采集位点未提交，无已确认输入范围"
            else:
                start_line = self._export_cursor(key)
                end_line = int(conf.get("end_line", 0))
                entry["range"] = {
                    "start_line": start_line,
                    "end_line": end_line,
                    "end_offset": int(conf.get("end_offset", 0)),
                    "confirmed_at": conf.get("confirmed_at"),
                }
                if end_line <= start_line:
                    entry["status"] = self.ENTRY_NO_NEW_DATA
                    entry["error"] = "已确认位点不超出上次交付范围"
            entries.append(entry)

        manifest: dict[str, Any] = {
            "batch_id": self._next_batch_id(),
            "created_at": time.time(),
            "format": format_type,
            "trigger": trigger,
            "status": "待导出",
            "entries": entries,
            "totals": {},
            "download_log": [],
        }
        self._refresh_batch_status(manifest)
        self._save_manifest(manifest)
        return manifest

    # -- 执行 / 续传（幂等） --

    def run_export_batch(self, batch_id: str,
                         fail_after: dict[str, int] | None = None,
                         action: str = "download") -> dict[str, Any]:
        """执行批次导出：发放与可用条目对应的成品文件。

        同一批下载请求重复送达时是安全的：已交付条目直接复用，不会重新
        生成；待导出/导出失败条目按原批冻结的范围接续导出。fail_after 仅
        用于故障注入演练（{source_key: 写入N行后中断}）。
        """
        manifest = self._load_manifest(batch_id)
        if manifest is None:
            return {"error": f"导出批次 {batch_id} 不存在"}

        fail_after = fail_after or {}
        reused: list[str] = []
        delivered: list[str] = []
        failed: list[str] = []
        skipped: list[str] = []
        interrupted = False

        for entry in manifest["entries"]:
            key = entry["source_key"]
            if interrupted:
                skipped.append(key)
                continue
            if entry["status"] == self.ENTRY_DELIVERED:
                # 保留先前已确认成品的下载依据，重复请求不重新生成
                reused.append(key)
                continue
            if entry["status"] not in (self.ENTRY_PENDING, self.ENTRY_FAILED):
                skipped.append(key)
                continue

            entry["attempts"] = int(entry.get("attempts", 0)) + 1
            try:
                result = self._export_batch_entry(
                    batch_id, entry, manifest.get("format", "jsonl"),
                    fail_after.get(key))
            except ExportInterrupted as exc:
                entry["status"] = self.ENTRY_FAILED
                entry["error"] = str(exc)
                entry["last_error"] = str(exc)
                entry["partial_records"] = exc.partial
                failed.append(key)
                interrupted = True  # 中断：其余条目保持待导出，留待接续
                self._refresh_batch_status(manifest)
                self._save_manifest(manifest)
                continue

            entry.update(result)
            entry["error"] = None
            entry["partial_records"] = 0
            delivered.append(key)
            rng = entry.get("range") or {}
            self._advance_export_cursor(key, int(rng.get("end_line", 0)))
            self._refresh_batch_status(manifest)
            self._save_manifest(manifest)  # 每条落账，崩溃后可按清单接续

        manifest.setdefault("download_log", []).append({
            "at": time.time(),
            "action": action,
            "reused": reused,
            "delivered": delivered,
            "failed": failed,
            "skipped": skipped,
        })
        self._refresh_batch_status(manifest)
        self._save_manifest(manifest)
        return manifest

    def resume_export_batch(self, batch_id: str) -> dict[str, Any]:
        """接续缺件：与重复下载同一入口，只补未交付的条目。"""
        return self.run_export_batch(batch_id, action="resume")

    def _export_batch_entry(self, batch_id: str, entry: dict[str, Any],
                            format_type: str,
                            fail_after_lines: int | None = None) -> dict[str, Any]:
        """按清单冻结的范围读取源文件并发放单份成品（原子落盘）。"""
        key = entry["source_key"]
        rng = entry["range"] or {}
        start_line = int(rng.get("start_line", 0))
        end_line = int(rng.get("end_line", 0))
        records = self._read_source_slice(entry["source_file"], start_line, end_line)
        expected = end_line - start_line
        if expected > 0 and len(records) != expected:
            # 源文件缺失或映射滞后：宁可判失败待接续，也不发放短斤缺两的成品
            raise ExportInterrupted(
                f"源数据与已确认范围不符：期望 {expected} 行，实读 {len(records)} 行",
                partial=len(records))

        ext = {"jsonl": "jsonl", "json": "json", "csv": "csv", "gz": "jsonl.gz"}.get(format_type)
        if ext is None:
            raise ExportInterrupted(f"不支持的输出格式: {format_type}", partial=0)
        artifact = self._batch_artifact_dir(batch_id) / f"{key}.{ext}"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        tmp = artifact.parent / (artifact.name + ".tmp")

        written = self._write_artifact(tmp, records, format_type, fail_after_lines)
        os.replace(tmp, artifact)  # 成品原子生效，中断不会留下半成品

        return {
            "status": self.ENTRY_DELIVERED,
            "artifact": str(artifact),
            "records": written,
            "checksum": self._file_checksum(artifact),
            "delivered_at": time.time(),
        }

    def _read_source_slice(self, source_file: str, start_line: int,
                           end_line: int) -> list[dict[str, Any]]:
        """读取源文件中 (start_line, end_line] 行号区间的记录。

        映射文件是追加写的，重跑解析会留下重复行号；按行号去重并只取
        冻结区间，保证批次建立后新采集的记录不会混进原批。
        """
        path = Path(source_file)
        records = self._read_jsonl(path) if path.exists() else []
        seen: set[int] = set()
        sliced: list[dict[str, Any]] = []
        for rec in records:
            line_no = rec.get("line_no")
            if line_no is None:
                continue
            line_no = int(line_no)
            if start_line < line_no <= end_line and line_no not in seen:
                seen.add(line_no)
                sliced.append(rec)
        sliced.sort(key=lambda r: int(r["line_no"]))
        return sliced

    def _write_artifact(self, tmp: Path, records: list[dict[str, Any]],
                        format_type: str,
                        fail_after_lines: int | None = None) -> int:
        """把记录写入临时文件；fail_after_lines 用于演练部分落盘后中断。"""
        written = 0

        def maybe_interrupt() -> None:
            if fail_after_lines is not None and written >= fail_after_lines:
                raise ExportInterrupted(f"写入 {written} 行后导出中断", partial=written)

        if format_type == "jsonl":
            with open(tmp, "w", encoding="utf-8") as f:
                for rec in records:
                    maybe_interrupt()
                    f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
                    written += 1
        elif format_type == "json":
            maybe_interrupt()
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2, default=str)
            written = len(records)
        elif format_type == "gz":
            maybe_interrupt()
            with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=6) as f:
                for rec in records:
                    f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
                    written += 1
        elif format_type == "csv":
            maybe_interrupt()
            keys = ["line_no", "source_key"]
            if records:
                std_keys = list(records[0].get("standardized", {}).keys())
                keys.extend(k for k in std_keys if k not in keys)
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(",".join(keys) + "\n")
                for rec in records:
                    std = rec.get("standardized", {})
                    row = {"line_no": rec.get("line_no"), "source_key": rec.get("source_key")}
                    row.update(std)
                    f.write(",".join(str(row.get(k, "")) for k in keys) + "\n")
                    written += 1
        else:
            raise ExportInterrupted(f"不支持的输出格式: {format_type}", partial=0)
        return written

    @staticmethod
    def _file_checksum(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]

    def _refresh_batch_status(self, manifest: dict[str, Any]) -> None:
        entries = manifest["entries"]
        exportable = [e for e in entries if e["status"] in (
            self.ENTRY_PENDING, self.ENTRY_DELIVERED, self.ENTRY_FAILED)]
        delivered = [e for e in entries if e["status"] == self.ENTRY_DELIVERED]
        failed = [e for e in entries if e["status"] == self.ENTRY_FAILED]
        pending = [e for e in entries if e["status"] == self.ENTRY_PENDING]
        unavailable = [e for e in entries if e["status"] in (
            self.ENTRY_NO_CHECKPOINT, self.ENTRY_NO_NEW_DATA)]
        if exportable and len(delivered) == len(exportable):
            manifest["status"] = "已完成"
        elif delivered or failed:
            manifest["status"] = "部分完成"
        else:
            manifest["status"] = "待导出"
        manifest["totals"] = {
            "entries": len(entries),
            "exportable": len(exportable),
            "delivered": len(delivered),
            "failed": len(failed),
            "pending": len(pending),
            "unavailable": len(unavailable),
            "records": sum(int(e.get("records", 0)) for e in delivered),
        }

    # -- 查询与对账 --

    def get_export_batch(self, batch_id: str) -> dict[str, Any]:
        """读取批次清单（含每份成品的下载依据）。"""
        manifest = self._load_manifest(batch_id)
        if manifest is None:
            return {"error": f"导出批次 {batch_id} 不存在"}
        return manifest

    def list_export_batches(self) -> list[dict[str, Any]]:
        """列出全部多源导出批次清单（按建立时间排序）。"""
        batches_dir = self._batches_dir()
        manifests = []
        if batches_dir.exists():
            for path in sorted(batches_dir.glob("EXB-*.json")):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        manifests.append(json.load(f))
                except (OSError, json.JSONDecodeError):
                    continue
        manifests.sort(key=lambda m: m.get("created_at", 0))
        return manifests

    def verify_export_batch(self, batch_id: str) -> dict[str, Any]:
        """对账：逐条核对已交付成品是否仍与原批清单对应。"""
        manifest = self._load_manifest(batch_id)
        if manifest is None:
            return {"error": f"导出批次 {batch_id} 不存在"}

        checks: list[dict[str, Any]] = []
        for entry in manifest["entries"]:
            key = entry["source_key"]
            base = {"source_key": key, "status": entry["status"]}
            if entry["status"] != self.ENTRY_DELIVERED:
                checks.append({**base, "reconciled": None,
                               "note": "未交付，无需对账" if entry["status"] in (
                                   self.ENTRY_NO_CHECKPOINT, self.ENTRY_NO_NEW_DATA)
                                   else "未交付，待接续"})
                continue
            artifact = Path(entry.get("artifact") or "")
            if not artifact.exists():
                checks.append({**base, "reconciled": False, "note": "成品文件缺失"})
                continue
            checksum = self._file_checksum(artifact)
            records = self._read_artifact_records(artifact, manifest.get("format", "jsonl"))
            rng = entry.get("range") or {}
            start_line = int(rng.get("start_line", 0))
            end_line = int(rng.get("end_line", 0))
            line_nos = [int(r["line_no"]) for r in records if r.get("line_no") is not None]
            in_range = all(start_line < ln <= end_line for ln in line_nos)
            ok = (checksum == entry.get("checksum")
                  and len(records) == int(entry.get("records", 0))
                  and in_range)
            checks.append({
                **base,
                "reconciled": ok,
                "checksum_match": checksum == entry.get("checksum"),
                "records": len(records),
                "range": {"start_line": start_line, "end_line": end_line},
                "in_range": in_range,
                "rule_version": entry.get("rule_version"),
            })
        reconciled = all(c.get("reconciled") in (True, None) for c in checks)
        return {
            "batch_id": batch_id,
            "status": manifest.get("status"),
            "reconciled": reconciled,
            "checks": checks,
        }

    def _read_artifact_records(self, artifact: Path, format_type: str) -> list[dict[str, Any]]:
        try:
            if format_type == "gz":
                with gzip.open(artifact, "rt", encoding="utf-8") as f:
                    return [json.loads(line) for line in f if line.strip()]
            if format_type == "json":
                with open(artifact, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return data if isinstance(data, list) else []
            if format_type == "csv":
                with open(artifact, "r", encoding="utf-8") as f:
                    lines = [line for line in f.read().splitlines() if line.strip()]
                header = lines[0].split(",") if lines else []
                records = []
                for line in lines[1:]:
                    values = line.split(",")
                    rec = dict(zip(header, values))
                    if rec.get("line_no") not in (None, ""):
                        rec["line_no"] = int(rec["line_no"])
                    records.append(rec)
                return records
            return self._read_jsonl(artifact)
        except (OSError, ValueError, json.JSONDecodeError):
            return []