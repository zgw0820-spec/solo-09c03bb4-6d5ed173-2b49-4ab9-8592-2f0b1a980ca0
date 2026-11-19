"""会话终止（带再校验）与 VACUUM 后台作业执行器。"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from psycopg.types.json import Jsonb

from . import db, sql_queries as q
from .config import settings

DISK_NOTICE = (
    "relation_size_bytes 是当前磁盘占用，前后差值不等于且不可称为"
    "“已释放磁盘字节”；普通 VACUUM 只回收可见空间给本库复用，"
    "一般不向操作系统归还空间。"
)


# --- 会话终止 ---------------------------------------------------------------

async def terminate_idle_session(
    req_pid: int,
    req_backend_start: datetime,
    req_xact_start: datetime,
    schema_name: str,
    table_name: str,
) -> dict[str, Any]:
    """再校验通过才 pg_terminate_backend，否则拒绝并留审计。

    校验条件（全部满足才终止）:
      1. PID 仍存在且属于当前数据库（同库）;
      2. backend_start 与诊断时一致（原会话，未被替换）;
      3. xact_start 与诊断时一致（原事务，未重开）;
      4. state 仍为 'idle in transaction'。
    """
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            q.RECHECK_SESSION, (req_pid, req_backend_start, req_xact_start)
        )
        observed = await cur.fetchone()
        observed_d = dict(observed) if observed else None

        if observed is None:
            reason = (
                "拒绝终止: 未找到同库且 PID/backend_start/xact_start "
                "完全一致的会话（会话已退出或已被替换，或事务已重开）"
            )
            await conn.execute(
                q.AUDIT_TERMINATION,
                (req_pid, req_backend_start, req_xact_start,
                 "rejected", False, Jsonb(db.to_jsonable(observed_d or {})), reason),
            )
            return {
                "terminated": False,
                "decision": "rejected",
                "target_pid": req_pid,
                "reason": reason,
                "observed_session": observed_d,
            }

        if observed_d["state"] != "idle in transaction":
            reason = (
                f"拒绝终止: 会话身份虽一致，但状态已变为 "
                f"{observed_d['state']!r}，不再是 idle in transaction"
            )
            await conn.execute(
                q.AUDIT_TERMINATION,
                (req_pid, req_backend_start, req_xact_start,
                 "rejected", False, Jsonb(db.to_jsonable(observed_d)), reason),
            )
            return {
                "terminated": False,
                "decision": "rejected",
                "target_pid": req_pid,
                "reason": reason,
                "observed_session": observed_d,
            }

        # 全部条件满足: 终止原会话
        cur2 = await conn.execute("SELECT pg_terminate_backend(%s)", (req_pid,))
        ok = (await cur2.fetchone())["pg_terminate_backend"]
        detail = (
            "再校验通过（同库、backend_start/xact_start 一致、仍为 idle in "
            f"transaction），调用 pg_terminate_backend 返回 {ok}"
        )
        await conn.execute(
            q.AUDIT_TERMINATION,
            (req_pid, req_backend_start, req_xact_start,
             "terminated", bool(ok), Jsonb(db.to_jsonable(observed_d)), detail),
        )
        return {
            "terminated": bool(ok),
            "decision": "terminated" if ok else "rejected",
            "target_pid": req_pid,
            "reason": detail if ok else "pg_terminate_backend 返回 false",
            "observed_session": observed_d,
        }


# --- VACUUM 作业 ------------------------------------------------------------

async def create_vacuum_job(
    schema_name: str, table_name: str
) -> tuple[int | None, dict[str, Any] | None]:
    """登记 running 作业。同表已有 running 作业时返回 (None, 冲突行)。"""
    pool = await db.open_pool()
    before_row = await db.fetch_table_stats(schema_name, table_name)
    before = db.stats_snapshot(before_row)
    try:
        async with pool.connection() as conn:
            cur = await conn.execute(
                q.INSERT_JOB,
                (schema_name, table_name,
                 settings.vacuum_statement_timeout_ms, Jsonb(db.to_jsonable(before))),
            )
            job_id = (await cur.fetchone())["id"]
    except psycopg.errors.UniqueViolation:
        # 部分唯一索引: 同表已有 running 作业
        async with pool.connection() as conn:
            cur = await conn.execute(
                sql.SQL(
                    "SELECT id, started_at FROM {sch}.{tbl} "
                    "WHERE table_schema = %s AND table_name = %s "
                    "AND status = 'running'"
                ).format(
                    sch=sql.Identifier(settings.job_schema),
                    tbl=sql.Identifier(settings.job_table),
                ),
                (schema_name, table_name),
            )
            conflict = db.to_jsonable(dict(await cur.fetchone()))
        return None, conflict
    return job_id, before


async def run_vacuum_job(
    job_id: int, schema_name: str, table_name: str
) -> None:
    """后台执行: 独立连接 + autocommit（VACUUM 不能在事务块中运行）。

    互斥保险:
      第一道（持久化）: vacuum_jobs 的部分唯一索引，同表第二个请求在登记
        阶段即被拒（409），不会走到这里；
      第二道（会话级）: 按表派生 key 的阻塞式 pg_advisory_lock，
        进程崩溃/重启时会话锁由 PostgreSQL 自动释放。
    """
    started = datetime.now(timezone.utc)
    notices: list[str] = []

    def notice_handler(notice):  # psycopg3: 单参数 Notice
        notices.append(f"{notice.severity}: {notice.message_primary}")

    try:
        async with await psycopg.AsyncConnection.connect(
            settings.database_url,
            autocommit=True,
            row_factory=dict_row,
        ) as conn:
            conn.add_notice_handler(notice_handler)

            lock_key = _advisory_lock_key(schema_name, table_name)
            # 阻塞式会话锁: 部分唯一索引已保证同表不会有第二个 running 作业，
            # 这里是跨进程的第二道互斥保险。进程退出时会话锁自动释放。
            await conn.execute(
                "SELECT pg_advisory_lock(%s)", (lock_key,)
            )
            try:
                if settings.vacuum_statement_timeout_ms > 0:
                    # SET 不支持绑定参数，用 Literal 安全内联整数
                    await conn.execute(
                        sql.SQL("SET statement_timeout = {}").format(
                            sql.Literal(settings.vacuum_statement_timeout_ms)
                        )
                    )
                vacuum = sql.SQL("VACUUM (VERBOSE, ANALYZE) {sch}.{tbl}").format(
                    sch=sql.Identifier(schema_name),
                    tbl=sql.Identifier(table_name),
                )
                await conn.execute(vacuum)
            finally:
                await conn.execute(
                    "SELECT pg_advisory_unlock(%s)", (lock_key,)
                )

            after_row = await db.fetch_table_stats(schema_name, table_name)
        after = db.stats_snapshot(after_row)
        finished = datetime.now(timezone.utc)
        result = {
            "duration_ms": round((finished - started).total_seconds() * 1000, 3),
            "before": await _get_before_snapshot(job_id),
            "after": after,
            "vacuum_verbose_notices": notices,
            "disk_notice": DISK_NOTICE,
            "completed_at": finished.isoformat(),
        }
        await _finish(job_id, "succeeded", result, None)
    except Exception as exc:  # 失败也要落库，绝不静默
        await _finish(job_id, "failed", None, f"{type(exc).__name__}: {exc}")


async def _get_before_snapshot(job_id: int) -> dict[str, Any]:
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(q.GET_JOB, (job_id,))
        row = await cur.fetchone()
    return dict(row["before_snapshot"])


async def _finish(
    job_id: int, status: str, result: dict[str, Any] | None, error: str | None
) -> None:
    pool = await db.open_pool()
    async with pool.connection() as conn:
        await conn.execute(
            q.FINISH_JOB,
            (status,
             Jsonb(db.to_jsonable(result)) if result is not None else None,
             error, job_id),
        )


def _advisory_lock_key(schema_name: str, table_name: str) -> int:
    # 稳定的 64bit 哈希，同表同 key；取正 int8 范围
    digest = f"{schema_name}.{table_name}".encode()
    import hashlib

    return int.from_bytes(hashlib.blake2b(digest, digest_size=8).digest(), "big") \
        - (1 << 63)


async def verify_interrupted_job(job: dict[str, Any]) -> str | None:
    """对 interrupted 作业核实: 对比统计基线判断 VACUUM 是否实际完成过。"""
    try:
        current = await db.fetch_table_stats(
            job["table_schema"], job["table_name"]
        )
    except LookupError as exc:
        return f"无法核实: {exc}"
    before = job["before_snapshot"]
    cur_vac = current["vacuum_count"]
    base_vac = before.get("vacuum_count", 0) or 0
    cur_dead = current["n_dead_tup_estimate"]
    base_dead = before.get("n_dead_tup_estimate", 0) or 0
    if cur_vac > base_vac:
        return (
            f"核实: 该表 vacuum_count 已由 {base_vac} 增至 {cur_vac}，"
            "VACUUM 在中断前后实际执行过（可能由本次作业或 autovacuum 完成）；"
            f"当前死元组估算 {cur_dead}（基线 {base_dead}）。可直接重试以确保结果。"
        )
    return (
        f"核实: vacuum_count 未增加（仍为 {cur_vac}），没有证据表明本次 "
        f"VACUUM 已完成；当前死元组估算 {cur_dead}（基线 {base_dead}），请重试。"
    )
