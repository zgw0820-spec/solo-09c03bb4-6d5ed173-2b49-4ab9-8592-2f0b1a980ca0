"""FastAPI 入口: 诊断、受控终止会话、VACUUM 作业（单飞 + 可核实/重试）。"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, status
from psycopg import sql

from . import db, maintenance
from .config import settings
from .schemas import (
    DiagnosticsResponse,
    HealthResponse,
    JobResponse,
    TableRef,
    TerminateRequest,
    TerminateResponse,
    VacuumRequest,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.open_pool()
    # 建表 + 把上次进程遗留的 running 作业标记为 interrupted
    await db.init_schema()
    yield
    await db.close_pool()


app = FastAPI(
    title="PostgreSQL 16 设备事件表 VACUUM 维护 API",
    version="1.0.0",
    description=(
        "展示设备事件表死元组【估算】与可能阻碍回收的旧快照会话；"
        "凭诊断返回的 PID/backend_start/事务起点，仅可终止同库且仍为 "
        "idle in transaction 的原会话；随后执行 VACUUM 并回读结果。\n\n"
        "- 同表并发维护单飞（部分唯一索引 + advisory lock）。\n"
        "- 请求超时或服务重启后，未确认的清理不会被报为完成，"
        "可经作业接口核实或重试。\n"
        "- 所有 n_dead_tup/尺寸字段均为估值或当前占用，"
        "不代表“已释放磁盘字节”。"
    ),
    lifespan=lifespan,
)


async def _check_relation(schema_name: str, table_name: str) -> None:
    if not await db.relation_exists(schema_name, table_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"relation {schema_name}.{table_name} does not exist",
        )


@app.get("/health", response_model=HealthResponse, tags=["meta"])
async def health() -> HealthResponse:
    try:
        pool = await db.open_pool()
        async with pool.connection() as conn:
            cur = await conn.execute("SELECT current_database() AS db")
            row = await cur.fetchone()
        return HealthResponse(status="ok", database=row["db"])
    except Exception as exc:  # 连通性失败要显式报错
        return HealthResponse(status="error", detail=str(exc))


@app.get(
    "/diagnostics",
    response_model=DiagnosticsResponse,
    tags=["diagnostics"],
    summary="死元组估算 + 可能阻碍回收的旧快照会话",
)
async def diagnostics(schema_name: str = "public",
                      table_name: str = "device_events") -> DiagnosticsResponse:
    await _check_relation(schema_name, table_name)
    stats = await db.fetch_table_stats(schema_name, table_name)
    sessions = await db.fetch_blocking_sessions()
    prepared = await db.fetch_prepared_xacts()
    slots = await db.fetch_inactive_slots()
    return DiagnosticsResponse(
        generated_at=datetime.now(timezone.utc),
        table=stats,
        blocking_sessions=sessions,
        prepared_transactions=prepared,
        inactive_replication_slots=slots,
    )


@app.post(
    "/sessions/terminate",
    response_model=TerminateResponse,
    tags=["sessions"],
    summary="终止 idle in transaction 会话（身份/状态变化即拒绝）",
)
async def terminate_session(req: TerminateRequest) -> TerminateResponse:
    await _check_relation(req.schema_name, req.table_name)
    result = await maintenance.terminate_idle_session(
        req.pid, req.backend_start, req.xact_start,
        req.schema_name, req.table_name,
    )
    return TerminateResponse(**result)


@app.post(
    "/maintenance/vacuum",
    tags=["maintenance"],
    status_code=status.HTTP_202_ACCEPTED,
    summary="对目标表执行 VACUUM (VERBOSE, ANALYZE)，同表单飞",
)
async def start_vacuum(req: VacuumRequest) -> dict:
    await _check_relation(req.schema_name, req.table_name)
    job_id, conflict = await maintenance.create_vacuum_job(
        req.schema_name, req.table_name
    )
    if job_id is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "该表已有正在执行的维护作业，拒绝重复执行",
                "running_job": conflict,
            },
        )

    async def _runner() -> None:
        await maintenance.run_vacuum_job(
            job_id, req.schema_name, req.table_name
        )

    asyncio.create_task(_runner())

    if req.wait:
        deadline = asyncio.get_event_loop().time() + settings.sync_wait_seconds
        while True:
            job = await _load_job(job_id)
            if job["status"] != "running":
                return {"job": _serialize_job(job)}
            if asyncio.get_event_loop().time() >= deadline:
                return {
                    "job": _serialize_job(job),
                    "message": "等待超时，作业仍在后台执行；"
                               "未确认完成，不会报为成功，请轮询作业接口",
                }
            await asyncio.sleep(0.25)

    return {
        "job_id": job_id,
        "status": "running",
        "message": "作业已受理；VACUUM 在后台执行，凭 job_id 轮询核实",
    }


async def _load_job(job_id: int) -> dict:
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            sql.SQL("SELECT * FROM {sch}.{tbl} WHERE id = %s").format(
                sch=sql.Identifier(settings.job_schema),
                tbl=sql.Identifier(settings.job_table),
            ),
            (job_id,),
        )
        row = await cur.fetchone()
    if row is None:
        raise HTTPException(404, f"job {job_id} not found")
    return dict(row)


def _serialize_job(row: dict) -> dict:
    out = dict(row)
    if out.get("status") == "interrupted":
        out["verification_hint"] = "调用 GET /maintenance/jobs/{id} 触发核实"
    if out.get("status") == "succeeded":
        out["disk_notice"] = maintenance.DISK_NOTICE
    return out


@app.get(
    "/maintenance/jobs/{job_id}",
    response_model=JobResponse,
    tags=["maintenance"],
    summary="查询作业；interrupted 作业会附带基于统计基线的核实结论",
)
async def get_job(job_id: int) -> JobResponse:
    job = await _load_job(job_id)
    out = _serialize_job(job)
    if job["status"] == "interrupted":
        out["verification_hint"] = await maintenance.verify_interrupted_job(job)
    return JobResponse(**out)


@app.get(
    "/maintenance/jobs",
    tags=["maintenance"],
    summary="列出某表最近的维护作业",
)
async def list_jobs(schema_name: str = "public",
                    table_name: str = "device_events",
                    limit: int = 20) -> dict:
    await _check_relation(schema_name, table_name)
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            sql.SQL(
                "SELECT * FROM {sch}.{tbl} "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY id DESC LIMIT %s"
            ).format(
                sch=sql.Identifier(settings.job_schema),
                tbl=sql.Identifier(settings.job_table),
            ),
            (schema_name, table_name, min(limit, 100)),
        )
        rows = await cur.fetchall()
    return {"jobs": [_serialize_job(dict(r)) for r in rows]}


@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": app.title,
        "docs": "/docs",
        "endpoints": [
            "/health",
            "/diagnostics?schema_name=public&table_name=device_events",
            "/sessions/terminate",
            "/maintenance/vacuum",
            "/maintenance/jobs/{job_id}",
            "/maintenance/jobs",
        ],
    }
