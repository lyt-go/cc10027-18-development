"""多源导出批次清单集成测试。

演练场景：三源各写一段数据，A 源提交新位点时建立导出批次；导出中 A 的
位点继续前进、C 源部分数据落盘后中断；同一批下载请求重复送达；最后补做
C 并对账每份已交付文件是否仍与原批对应。
"""
import json
import sys
from pathlib import Path

# 确保 backend 在 sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.services.base import DATA_ROOT
from app.services.log_source import LogSourceService
from app.services.collect_checkpoint import CollectCheckpointService
from app.services.format_output import FormatOutputService
from app.store import store

SEGMENT_LINES = 6


def _ensure_parser_effective() -> None:
    """解析需要至少一条已生效规则（无规则时默认正则匹配不到模拟行）。"""
    for p in store.rows("log_parser"):
        if p.get("status") == "已生效":
            return
    for p in store.rows("log_parser"):
        if p.get("status") in ("草稿", "待校验"):
            p["status"] = "已生效"
            p["pending"] = False
            return


def _reset_source_files(source_key: str) -> None:
    """清理指定源的历史数据与位点，保证用例可重复执行。"""
    logs = DATA_ROOT / "logs"
    for suffix in ("", "_parsed", "_mapped", "_parse_errors"):
        f = logs / f"{source_key}{suffix}.jsonl"
        if f.exists():
            f.unlink()
    cps = DATA_ROOT / "checkpoints"
    for suffix in ("", "_confirmed", "_export_cursor", "_export", "_parse", "_map"):
        f = cps / f"{source_key}{suffix}.json"
        if f.exists():
            f.unlink()


def _create_source(name: str) -> dict:
    rows = store.rows("log_source")
    new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    source = {
        "id": new_id,
        "源编号": f"LOG_-{new_id:04d}",
        "源名称": name,
        "日志类型": "batch_test",
        "接入方式": "api",
        "路径模版": "/var/log/batch_test",
        "字符编码": "UTF-8",
        "轮转规则": "daily",
        "源状态": "已接入",
        "status": "已接入",
        "pending": False,
        "abnormal": False,
    }
    rows.append(source)
    return source


def _create_output_config(source_key: str, version: str) -> dict:
    rows = store.rows("format_output")
    new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    config = {
        "id": new_id,
        "配置编号": f"FORM-{new_id:04d}",
        "关联源": source_key,
        "输出格式": "jsonl",
        "输出路径": f"data/exports/{source_key}",
        "压缩方式": "none",
        "分片大小": "1024",
        "命名规则": "by_source",
        "配置版本": version,
        "status": "已生效",
        "pending": False,
        "abnormal": False,
    }
    rows.append(config)
    return config


def _read_artifact_records(artifact: str) -> list[dict]:
    path = Path(artifact)
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_multi_source_batch_export_scenario():
    """主场景：建立批次 -> 导出中位点变更+中断 -> 幂等重下 -> 补做 -> 对账。"""
    src_svc = LogSourceService()
    ckpt_svc = CollectCheckpointService()
    out_svc = FormatOutputService()
    _ensure_parser_effective()

    # -- 三源各写一段数据 --
    keys: dict[str, str] = {}
    for label, version in (("A", "v3.1"), ("B", "v2.0"), ("C", "v1.4")):
        source = _create_source(f"批次演练源{label}")
        key = source["源编号"]
        keys[label] = key
        _reset_source_files(key)
        _create_output_config(key, version)
        result = src_svc.collect_from_source(source["id"], batch_size=SEGMENT_LINES)
        assert result.get("written") == SEGMENT_LINES, f"{label} 源应写入一段数据: {result}"
    key_a, key_b, key_c = keys["A"], keys["B"], keys["C"]
    id_a = next(s["id"] for s in store.rows("log_source") if s["源编号"] == key_a)

    # -- B、C 先提交位点；A 提交新位点时建立导出批次 --
    ckpt_svc.commit_checkpoint(key_b)
    ckpt_svc.commit_checkpoint(key_c)
    commit_r = ckpt_svc.commit_checkpoint(
        key_a, establish_batch=True, batch_sources=[key_a, key_b, key_c])
    assert "auto_batch" in commit_r, f"提交新位点应建立导出批次: {list(commit_r.keys())}"

    batch = commit_r["auto_batch"]
    batch_id = batch["batch_id"]
    assert batch["status"] == "待导出"
    entries = {e["source_key"]: e for e in batch["entries"]}
    assert set(entries) == {key_a, key_b, key_c}
    for key, version in ((key_a, "v3.1"), (key_b, "v2.0"), (key_c, "v1.4")):
        entry = entries[key]
        assert entry["status"] == "待导出"
        assert entry["range"]["start_line"] == 0
        assert entry["range"]["end_line"] == SEGMENT_LINES, "范围应冻结在已确认位点"
        assert entry["rule_version"] == version, "成品应关联实际读取的规则版本"
        assert entry["source_file"].endswith(f"{key}_mapped.jsonl"), "应记录实际读取的源文件"

    # -- 导出中：A 的位点继续前进（新采集一段并再次提交） --
    r2 = src_svc.collect_from_source(id_a, batch_size=SEGMENT_LINES)
    assert r2["end_line"] == SEGMENT_LINES * 2
    ckpt_svc.commit_checkpoint(key_a)
    assert ckpt_svc.get_confirmed_range(key_a)["end_line"] == SEGMENT_LINES * 2

    # -- 第一次下载：C 部分数据落盘后导出失败 --
    manifest = out_svc.run_export_batch(batch_id, fail_after={key_c: 2})
    assert manifest["status"] == "部分完成"
    m1 = {e["source_key"]: e for e in manifest["entries"]}
    assert m1[key_a]["status"] == "已交付"
    assert m1[key_b]["status"] == "已交付"
    assert m1[key_c]["status"] == "导出失败"
    assert m1[key_c]["partial_records"] == 2, "C 应有部分数据落盘记录"
    assert m1[key_c]["artifact"] is None, "中断的 C 不应发放成品"
    c_artifact = out_svc._batch_artifact_dir(batch_id) / f"{key_c}.jsonl"
    assert not c_artifact.exists(), "半成品不能当成品发放"

    # A 的成品只含原批冻结范围，不能混入位点前进后新采集的记录
    a_records = _read_artifact_records(m1[key_a]["artifact"])
    assert len(a_records) == SEGMENT_LINES
    assert max(int(r["line_no"]) for r in a_records) <= SEGMENT_LINES

    # 留存 A/B 成品下载依据，供幂等核对
    basis = {
        k: (m1[k]["checksum"], m1[k]["delivered_at"], m1[k]["attempts"],
            Path(m1[k]["artifact"]).read_bytes())
        for k in (key_a, key_b)
    }

    # -- 同一批下载请求再送一次：已交付复用，C 按原批范围补做 --
    manifest2 = out_svc.resume_export_batch(batch_id)
    assert manifest2["status"] == "已完成"
    log2 = manifest2["download_log"][-1]
    assert set(log2["reused"]) == {key_a, key_b}, "已确认成品应直接复用"
    assert log2["delivered"] == [key_c], "只有缺件 C 需要补做"

    m2 = {e["source_key"]: e for e in manifest2["entries"]}
    for key in (key_a, key_b):
        checksum, delivered_at, attempts, content = basis[key]
        entry = m2[key]
        assert entry["checksum"] == checksum, "重试不得改变已确认成品的摘要"
        assert entry["delivered_at"] == delivered_at, "重试不得改变已确认成品的交付时间"
        assert entry["attempts"] == attempts, "重试不得重新导出已交付条目"
        assert Path(entry["artifact"]).read_bytes() == content, "重试不得改写已交付成品"

    c_entry = m2[key_c]
    assert c_entry["status"] == "已交付"
    assert c_entry["range"]["end_line"] == SEGMENT_LINES, "补做仍按原批冻结范围"
    c_records = _read_artifact_records(c_entry["artifact"])
    assert sorted(int(r["line_no"]) for r in c_records) == list(range(1, SEGMENT_LINES + 1))

    # -- 下载请求第三次送达：全部复用，无任何变化 --
    manifest3 = out_svc.run_export_batch(batch_id)
    log3 = manifest3["download_log"][-1]
    assert len(log3["reused"]) == 3 and not log3["delivered"] and not log3["failed"]
    assert manifest3["totals"]["delivered"] == 3

    # -- 对账：每份已交付文件都能跟原批对应 --
    report = out_svc.verify_export_batch(batch_id)
    assert report["reconciled"] is True, f"对账应通过: {report}"
    for check in report["checks"]:
        assert check["reconciled"] is True
        assert check["checksum_match"] is True
        assert check["in_range"] is True, "成品记录不得超出原批采集范围"

    # -- 补做 C 后的清单：每份成品关联源、位点范围和规则版本 --
    final = out_svc.get_export_batch(batch_id)
    assert final["status"] == "已完成"
    assert final["totals"]["delivered"] == 3
    for entry in final["entries"]:
        assert entry["status"] == "已交付"
        assert entry["source_key"] and entry["source_file"]
        assert entry["rule_version"]
        assert entry["range"]["start_line"] == 0
        assert entry["range"]["end_line"] == SEGMENT_LINES
        assert entry["checksum"] and entry["artifact"] and entry["delivered_at"]

    # -- A 后续采集的记录只能进入新批次，不会混进原批 --
    batch2 = out_svc.create_export_batch([key_a, key_b, key_c])
    b2 = {e["source_key"]: e for e in batch2["entries"]}
    assert b2[key_a]["status"] == "待导出"
    assert b2[key_a]["range"]["start_line"] == SEGMENT_LINES
    assert b2[key_a]["range"]["end_line"] == SEGMENT_LINES * 2
    assert b2[key_b]["status"] == "无新增数据"
    assert b2[key_c]["status"] == "无新增数据"


def test_batch_skips_source_without_confirmed_checkpoint():
    """未提交位点的源记入清单但不发放文件（只发放与可用条目对应的文件）。"""
    src_svc = LogSourceService()
    out_svc = FormatOutputService()
    _ensure_parser_effective()

    source = _create_source("批次演练源D")
    key = source["源编号"]
    _reset_source_files(key)
    result = src_svc.collect_from_source(source["id"], batch_size=3)
    assert result.get("written") == 3

    manifest = out_svc.create_export_batch([key])
    entry = manifest["entries"][0]
    assert entry["status"] == "无确认位点"

    manifest = out_svc.run_export_batch(manifest["batch_id"])
    entry = manifest["entries"][0]
    assert entry["status"] == "无确认位点"
    assert entry["artifact"] is None
    artifact = out_svc._batch_artifact_dir(manifest["batch_id"]) / f"{key}.jsonl"
    assert not artifact.exists(), "无确认位点的源不应发放成品"


def test_list_and_get_batches():
    """批次清单可列表、可按编号读取；未知编号返回错误。"""
    out_svc = FormatOutputService()
    manifest = out_svc.create_export_batch([])
    batch_id = manifest["batch_id"]

    batches = out_svc.list_export_batches()
    assert any(b["batch_id"] == batch_id for b in batches)

    got = out_svc.get_export_batch(batch_id)
    assert got["batch_id"] == batch_id
    assert "entries" in got and "totals" in got

    missing = out_svc.get_export_batch("EXB-19000101-9999")
    assert "error" in missing


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v", "--tb=short", "-s"]))
