# 实时流数据湖构建与治理平台

面向实时流数据接入、Delta湖表构建、流批一体转换、分区合并和数据治理的流式数据湖平台

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

健康检查：`curl http://127.0.0.1:8011/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5175/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8011`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流数据源 | `stream_source` | 流数据源 | 来源标识、接入协议、主题名称 |
| 湖表定义 | `lake_table` | 湖表定义 | 表标识、表名称、主键列 |
| 流式摄入 | `stream_ingest` | 摄入管道 | 管道标识、来源流、目标湖表 |
| 提交版本 | `delta_commit` | 版本提交 | 提交标识、目标湖表、提交类型 |
| 分区合并 | `partition_merge` | 合并任务 | 任务标识、目标湖表、源分区 |
| 流Schema演进 | `stream_schema` | Schema变更 | 变更标识、目标湖表、变更类型 |
| 快照管理 | `snapshot_mgr` | 表快照 | 快照标识、目标湖表、快照时间 |
| 流质量校验 | `stream_quality` | 质量检查 | 检查标识、目标管道、校验字段 |
| 历史清理 | `vacuum_job` | 清理作业 | 作业标识、目标湖表、保留版本 |
| 批流合并 | `batch_merge` | 合并管道 | 管道标识、流来源、批来源 |
| 湖表血缘 | `lake_lineage` | 血缘链路 | 链路标识、上游表、下游表 |
| 保留策略 | `retention_policy` | 保留规则 | 规则标识、目标湖表、保留天数 |
| 压缩任务 | `compaction_task` | 压缩任务 | 任务标识、目标湖表、压缩算法 |
| 数据回填 | `backfill_pipeline` | 回填任务 | 任务标识、目标湖表、回填范围 |
| 流处理指标 | `stream_metric` | 处理指标 | 指标标识、关联管道、指标类型 |
| 格式迁移 | `iceberg_migrate` | 迁移计划 | 计划标识、源格式、目标格式 |
| 表目录同步 | `table_catalog` | 同步记录 | 记录标识、来源Catalog、目标Catalog |
| 湖仓审计 | `lake_audit` | 审计报告 | 报告标识、审计范围、审计时间 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 回填交付边界

数据回填的进度、完成行数和“可用回填数据范围”都来自逐分区实际结果，
纯计算逻辑集中在 `backend/app/backfill.py`，回填工作台与合并入口共用同一套口径：

- 存在失败 / 无产出分区时进度不会显示 100%，失败分区只保留拒绝依据
  （`basis=partition_result`），不计入完成行数；
- 交付支持两种口径，且都拒绝无产出分区：
  - 按分区交付：完整成功的分区立即交付，下一批读取范围就是这些分区；
  - 整批确认：同批分区全部成功后才统一交付，否则成功分区也留在回填侧等待；
- 两种口径同时出现时按优先级统一（整批确认 > 按分区交付），显式传入的口径
  覆盖已存配置并标记 `mode_conflict`；
- 回填状态只能 排队中→回填中→已完成/回填失败 逐级流转；失败恢复回到回填中，
  已写入分区按台账保留，重复提交只记一次；可用范围为空的旧回填依据不会覆盖真实流记录。

相关接口：

| 接口 | 说明 |
| --- | --- |
| `GET /api/backfill/{id}/detail` | 回填详情：进度、可用范围、逐分区依据 |
| `POST /api/backfill/{id}/execute` | 执行一批分区回填 |
| `POST /api/backfill/{id}/recover` | 失败恢复（保留已写入分区） |
| `POST /api/backfill/{id}/delivery-mode` | 设置交付口径 |
| `GET /api/backfill/{id}/boundary` | 合并入口使用的交付边界与合并待办 |
| `POST /api/backfill/merge/{merge_id}/evaluate` | 合并入口预览接收 / 拒绝范围 |

`POST /api/chains/schema-evolve` 支持可选的 `delivery_mode` 参数，摘要会带上
回填进度、可用范围、交付分区、留在回填侧的分区和合并待办。

领域：数据工程
子方向：数仓与数据湖
