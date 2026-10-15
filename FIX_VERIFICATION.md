# Codex Review 缺失功能修复验证

**修复日期**: 2026-10-15
**修复文件**: 4 个核心文件
**修复类别**: 多数据库路由/安全控制、弹性与可观测性、响应模型清理

---

## 一、修复清单

### 模块 1: 多数据库路由 + 安全控制（设计有承诺，实际未启用）

| 问题 | 原始位置 | 修复 |
|------|----------|------|
| orchestrator 只拿单个 executor，忽略了 server.py 为每个 DB 创建的 `sql_executors` dict | `server.py:197` 传 `sql_executors[_settings.database.name]`；`orchestrator.py` 存单 `self.sql_executor` | `server.py` 传 dict；`orchestrator.__init__` 收 `dict[str, SQLExecutor]`；`execute_query` 用 `sql_executors.get(database_name)` 按请求目标 DB 路由 |
| SQLValidator 的 `blocked_tables`/`blocked_columns`/`allow_explain` 硬编码 `None, None, False` | `server.py:154-158` | `SecurityConfig` 新增三个配置字段（带逗号分隔解析器）；`server.py` 从 config 读取 |

**修改文件**: `settings.py`、`server.py`、`orchestrator.py`

### 模块 2: 弹性与可观测性（停留在设计层面）

| 问题 | 原始位置 | 修复 |
|------|----------|------|
| `MultiRateLimiter` 在 server.py 创建后完全没被用 | `server.py:187-189` | 传进 orchestrator；`execute_query` 中 DB 执行前 `async with self.rate_limiter.for_queries(timeout=30.0):` 包裹 |
| 重试循环无退避，只是裸 `continue` | `orchestrator.py:_generate_sql_with_retry` | 加 `await asyncio.sleep(base_delay * (backoff_factor ** attempt))`，从 `ResilienceConfig.retry_delay/backoff_factor` 读取 |
| `MetricsCollector` 创建后零调用 | `server.py:136-143` | 传入 orchestrator；`execute_query` 在入口、成功、失败、SQL-only 各路径埋 `increment_query_request` / `observe_db_query_duration` / `increment_llm_call` / `increment_sql_rejected` / `query_duration.observe` |
| 追踪未贯穿，orchestrator 自己 `uuid.uuid4()` | `orchestrator.py:130` | 用 `async with request_context()` 包裹整个 `execute_query`，下游组件通过 contextvars 自动继承 request_id |

**修改文件**: `orchestrator.py`、`server.py`

### 模块 3: 响应/模型缺陷

| 问题 | 原始位置 | 修复 |
|------|----------|------|
| `QueryResponse` 有**两个同名 `to_dict()`** —— 后定义的覆盖前者，丢掉了 `tokens_used=0` 特殊处理 | `query.py:160` 和 `query.py:214` 同时定义 | 删掉第二个；保留第一个（带 tokens_used fallback + `exclude_none=True`） |

**修改文件**: `query.py`

---

## 二、快速验证步骤

```bash
cd pg-mcp

# 1. 安装依赖
uv sync

# 2. 跑现有测试（应该全部通过或失败数不增加）
uv run pytest tests/unit/ -v

# 3. 手动验证修复点
uv run python -c "
import ast, sys

# 验证 orchestrator 的改动
src = open('src/pg_mcp/services/orchestrator.py').read()
checks = [
    ('executors dict 路由', 'sql_executors: dict[str, SQLExecutor]'),
    ('request_context tracing', 'request_context()'),
    ('rate_limiter 接入', 'rate_limiter.for_queries'),
    ('metrics 埋点', 'increment_query_request'),
    ('指数退避 sleep', 'asyncio.sleep'),
    ('指数退避公式', 'backoff_factor ** attempt'),
]
for name, needle in checks:
    status = '✅' if needle in src else '❌'
    print(f'{status} {name}')

# 验证 query.py 只有一个 to_dict
src_q = open('src/pg_mcp/models/query.py').read()
count = src_q.count('def to_dict')
print(f'{"✅" if count == 1 else "❌"} to_dict 数量 = {count}')

# 验证 settings.py 新字段
src_s = open('src/pg_mcp/config/settings.py').read()
for field in ['blocked_tables', 'blocked_columns', 'allow_explain']:
    status = '✅' if f'{field}: list[str]' in src_s or f'{field}: bool' in src_s else '❌'
    print(f'{status} SecurityConfig.{field}')

# 验证 server.py 传 dict
src_v = open('src/pg_mcp/server.py').read()
print(f'{"✅" if "sql_executors=sql_executors," in src_v else "❌"} server.py 传 dict')
print(f'{"✅" if "rate_limiter=_rate_limiter," in src_v else "❌"} server.py 传 rate_limiter')
print(f'{"✅" if "metrics=_metrics," in src_v else "❌"} server.py 传 metrics')
"
```

---

## 三、改动范围汇总

```
修改 4 个文件:
  src/pg_mcp/services/orchestrator.py   — 核心枢纽，改动最多（约 80+ 行新增）
  src/pg_mcp/server.py                  — 改 orchestrator 构造传参
  src/pg_mcp/models/query.py            — 合并重复 to_dict
  src/pg_mcp/config/settings.py          — SecurityConfig 加 3 个新字段

新增依赖: 无（全部使用已有模块）
删除代码: 约 15 行（第二个 to_dict 方法）
语法检查: 全部通过
```

---

## 四、测试覆盖建议

| 测试类 | 覆盖的修复点 |
|--------|--------------|
| `TestMultiDatabaseRouting` | 构造多 pool 多 executor 的 orchestrator，验证请求正确路由到目标 DB 的 executor |
| `TestRateLimiterIntegration` | 用 fake rate_limiter 执行多请求，验证 `for_queries` 被调用且超时抛出 `RateLimitExceededError` |
| `TestRetryWithBackoff` | mock sleep 记录调用次数和延迟序列，验证公式 `delay = base * factor^attempt` |
| `TestMetricsCollection` | 验证成功/失败路径的 Counter/Histogram 都被 increment/observe |
| `TestTracingContext` | 验证 request_context 内生成的 request_id 能在下游组件（executor、logger）获取 |
| `TestSecurityPolicies` | 构造带 blocked_tables 的 SQLValidator，验证对被禁表名的 SQL 正确抛出 SecurityViolationError |
| `TestQueryResponseSingleToDict` | 验证 QueryResponse 只有一个 to_dict，且 tokens_used=None 时返回 `{"tokens_used": 0}` |
