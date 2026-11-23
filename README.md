# 日志采集与事件追踪平台

面向应用日志收集、事件埋点追踪、流式采集与结构化归集的多源日志采集平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 日志源管理 | `log_source` | 日志源 | 源编号、源名称、日志类型 |
| 日志解析 | `log_parser` | 解析规则 | 规则编号、关联源、正则表达式 |
| 事件埋点 | `event_tracker` | 埋点事件 | 事件编号、事件名称、埋点页面 |
| 流式缓冲 | `stream_buffer` | 缓冲配置 | 配置编号、关联源、缓冲区大小 |
| 字段映射 | `field_mapping` | 映射规则 | 规则编号、源字段、目标字段 |
| 采集代理 | `agent_heartbeat` | 采集代理 | 代理编号、部署主机、代理版本 |
| 事件校验 | `event_validate` | 校验规则 | 规则编号、关联事件、字段检查 |
| 日志轮转 | `log_rotate` | 轮转策略 | 策略编号、关联源、轮转间隔 |
| 日志富化 | `enrich_join` | 富化规则 | 规则编号、目标字段、维表来源 |
| 告警规则 | `alert_rule` | 告警规则 | 规则编号、关联源、触发条件 |
| 日志分区 | `log_partition` | 分区规则 | 规则编号、关联源、分区键 |
| 采集位点 | `collect_checkpoint` | 位点记录 | 记录编号、关联源、文件偏移 |
| 窗口去重 | `dedup_window` | 去重窗口 | 窗口编号、关联源、去重键 |
| 聚合计算 | `aggregate_olap` | 聚合规则 | 规则编号、关联源、聚合维度 |
| 格式输出 | `format_output` | 输出配置 | 配置编号、关联源、输出格式 |
| 回溯采集 | `backfill_range` | 回溯任务 | 任务编号、关联源、时间范围 |
| 采样策略 | `sampling_rule` | 采样规则 | 规则编号、关联源、采样方式 |
| 质量诊断 | `log_quality` | 诊断报告 | 报告编号、关联源、解析成功率 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 多源导出批次清单

批量导出不再只汇总已写行数：`collect_checkpoint` 提交位点时记录**已确认输入范围**
（`<源>_confirmed` 位点快照），`format_output` 据此建立批次清单并发放成品文件。

- 每个批次条目冻结：实际读取的源文件、采集范围 `[start_line, end_line)`、规则版本；
  无确认位点 / 无新增数据的源记入清单但不发放文件。
- 成品按源原子落盘到 `data/exports/batches/<批次号>/`，带 SHA 校验摘要；
  清单持久化在 `data/exports/batches/<批次号>.json`。
- 下载/续传幂等：已交付条目直接复用（不重导、不改写），中断条目按原批冻结范围
  接续补做；批次建立后新采集的记录只会进入下一批，不会混进原批。
- `verify` 逐条对账：成品文件、校验摘要、记录数与冻结范围须全部对应。

接口（前缀 `/api/export-batches`，注册在通用 `/{module}` 路由之前）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/export-batches` | 建立批次清单并发放可用条目成品 |
| GET | `/api/export-batches` | 列出全部批次清单 |
| GET | `/api/export-batches/{batch_id}` | 读取批次清单（含每份成品下载依据） |
| POST | `/api/export-batches/{batch_id}/download` | 幂等下载：复用已交付、接续缺件 |
| POST | `/api/export-batches/{batch_id}/resume` | 中断接续（与 download 同一幂等入口） |
| GET | `/api/export-batches/{batch_id}/verify` | 对账已交付成品与原批是否对应 |
| GET | `/api/export-batches/{batch_id}/files/{source_key}` | 下载批次中指定源的成品文件 |

另有管道端点 `POST /api/pipelines/export-batch`（建立 -> 发放 -> 对账）。
场景测试见 `backend/test_export_batch.py`：三源各写一段数据，导出中改变 A 的
位点并中断 C，重复送达下载请求后补做 C，逐条核对成品与原批的对应关系。

领域：数据工程
子方向：数据采集、爬虫与RPA
