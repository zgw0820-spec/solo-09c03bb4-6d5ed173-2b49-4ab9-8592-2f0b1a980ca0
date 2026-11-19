"""连接池、初始化与统计读取。"""
from __future__ import annotations

import json
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from . import sql_queries as q
from .config import settings

pool: AsyncConnectionPool | None = None


async def open_pool() -> AsyncConnectionPool:
    global pool
    if pool is None:
        pool = AsyncConnectionPool(
            conninfo=settings.database_url,
            min_size=settings.pool_min_size,
            max_size=settings.pool_max_size,
            kwargs={"autocommit": True, "row_factory": dict_row},
            open=False,
        )
        await pool.open()
    return pool


async def close_pool() -> None:
    global pool
    if pool is not None:
        await pool.close()
        pool = None


async def init_schema() -> None:
    """建维护表，并把上次进程遗留的 running 作业标记为 interrupted。"""
    assert pool is not None
    async with pool.connection() as conn:
        await conn.execute(q.CREATE_SCHEMA)
        await conn.execute(q.CREATE_JOB_TABLE)
        await conn.execute(q.CREATE_JOB_UNIQUE)
        await conn.execute(q.CREATE_AUDIT_TABLE)
        await conn.execute(q.SWEEP_INTERRUPTED)


async def relation_exists(schema_name: str, table_name: str) -> bool:
    assert pool is not None
    async with pool.connection() as conn:
        cur = await conn.execute(q.RELATION_EXISTS, (schema_name, table_name))
        return cur.rowcount > 0


async def fetch_table_stats(schema_name: str, table_name: str) -> dict[str, Any]:
    assert pool is not None
    async with pool.connection() as conn:
        cur = await conn.execute(q.TABLE_STATS, (schema_name, table_name))
        row = await cur.fetchone()
    if row is None:
        raise LookupError(f"relation {schema_name}.{table_name} does not exist")
    return dict(row)


async def fetch_blocking_sessions(limit: int = 20) -> list[dict[str, Any]]:
    assert pool is not None
    async with pool.connection() as conn:
        cur = await conn.execute(q.BLOCKING_SNAPSHOTS, (limit,))
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def fetch_prepared_xacts() -> list[dict[str, Any]]:
    assert pool is not None
    async with pool.connection() as conn:
        cur = await conn.execute(q.PREPARED_XACTS)
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def fetch_inactive_slots() -> list[dict[str, Any]]:
    assert pool is not None
    async with pool.connection() as conn:
        cur = await conn.execute(q.REPLICATION_SLOTS)
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


def stats_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    """只保留可 JSON 序列化的统计字段。"""
    keys = (
        "n_dead_tup_estimate",
        "n_live_tup_estimate",
        "vacuum_count",
        "autovacuum_count",
        "last_vacuum",
        "last_autovacuum",
        "relation_size_bytes",
        "total_relation_size_bytes",
    )
    out: dict[str, Any] = {}
    for k in keys:
        v = row.get(k)
        out[k] = v.isoformat() if hasattr(v, "isoformat") else v
    return out


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def to_jsonable(obj: Any) -> Any:
    """递归把 datetime/date 等转成可 JSON 序列化的值。"""
    from datetime import date, datetime, timedelta

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, timedelta):
        return obj.total_seconds()
    if isinstance(obj, dict):
        return {k: to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    return obj
