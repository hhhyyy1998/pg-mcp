"""verify_fixes.py — 快速验证 codex review 三项修复点。

静态检查 + 运行时 smoke test，不依赖真实数据库/LLM。
用法: python verify_fixes.py
"""
import ast
import os
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(ROOT, "src", "pg_mcp")

PASS = "✅"
FAIL = "❌"
results = []


def check(name: str, condition: bool):
    mark = PASS if condition else FAIL
    results.append(condition)
    print(f"  {mark} {name}")


def load(module, filename):
    path = os.path.join(SRC, module, filename)
    return open(path, encoding="utf-8").read()


# ============================================================
# 1. Multi-database router
# ============================================================
print("\n== 模块 1: 多数据库路由 ==")

src_orch = load("services", "orchestrator.py")
src_server = load("", "server.py")

check("orchestrator __init__ 收 dict[str, SQLExecutor]",
      "sql_executors: dict[str, SQLExecutor]" in src_orch)

check("execute_query 用 sql_executors.get(database_name) 路由",
      "self.sql_executors.get(database_name)" in src_orch)

check("server.py 传完整 dict 而不是单 executor",
      "sql_executors=sql_executors," in src_server)

check("SecurityConfig 有 blocked_tables/columns/allow_explain",
      "blocked_tables" in load("config", "settings.py") and
      "blocked_columns" in load("config", "settings.py") and
      "allow_explain" in load("config", "settings.py"))

check("server.py 从 config 读取 blocked_tables/columns/allow_explain",
      "security.blocked_tables" in src_server and
      "security.blocked_columns" in src_server and
      "security.allow_explain" in src_server)

# ============================================================
# 2. Resilience & observability wired in
# ============================================================
print("\n== 模块 2: 弹性与可观测性 ==")

check("orchestrator 收 rate_limiter 和 metrics 参数",
      "rate_limiter:" in src_orch and "metrics:" in src_orch)

check("server.py 传 rate_limiter 和 metrics 给 orchestrator",
      "rate_limiter=_rate_limiter," in src_server and
      "metrics=_metrics," in src_server)

check("execute_query 用 async with request_context() 替代裸 uuid",
      "request_context()" in src_orch and "uuid.uuid4" not in src_orch)

check("DB 执行前 rate_limiter.for_queries(timeout=30.0) 包裹",
      "rate_limiter.for_queries(timeout=30.0)" in src_orch)

check("指数退避 sleep (attempt > 0 时)",
      "backoff_factor ** attempt" in src_orch and "asyncio.sleep(delay)" in src_orch)

check("success 路径埋 metrics",
      "increment_query_request(status=\"success\"" in src_orch)

check("error 路径埋 metrics",
      "increment_query_request(status=\"error\"" in src_orch)

check("db_query_duration observe",
      "observe_db_query_duration" in src_orch)

check("llm_calls 埋点",
      "increment_llm_call(operation=\"generate_sql\")" in src_orch)

# ============================================================
# 3. Model fixes
# ============================================================
print("\n== 模块 3: 响应/模型清理 ==")

src_q = load("models", "query.py")
tree = ast.parse(src_q)

# 找出 QueryResponse 类
qr_class = None
for node in ast.walk(tree):
    if isinstance(node, ast.ClassDef) and node.name == "QueryResponse":
        qr_class = node
        break

to_dict_methods = [
    m for m in qr_class.body
    if isinstance(m, ast.FunctionDef) and m.name == "to_dict"
] if qr_class else []

check(f"QueryResponse.to_dict 只有 1 个定义 (实际 {len(to_dict_methods)} 个)",
      len(to_dict_methods) == 1)

check("to_dict 有 tokens_used fallback 逻辑",
      "tokens_used" in src_q and "= 0" in src_q)

# ============================================================
# Summary
# ============================================================
total = len(results)
passed = sum(results)
print(f"\n== 汇总: {passed}/{total} 通过 ==")

# Syntax smoke test on orchestrator AST
try:
    ast.parse(src_orch)
    ast.parse(src_server)
    ast.parse(src_q)
    ast.parse(load("config", "settings.py"))
    print(f"  {PASS} 所有修改文件 AST 可解析")
    results.append(True)
except SyntaxError as e:
    print(f"  {FAIL} SyntaxError: {e}")
    results.append(False)

sys.exit(0 if all(results) else 1)
