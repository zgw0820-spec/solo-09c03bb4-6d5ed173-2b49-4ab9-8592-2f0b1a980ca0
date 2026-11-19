"""Pydantic 模型 —— 字段描述同时生成 OpenAPI/Swagger 文档。"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, Field


class TableRef(BaseModel):
    schema_name: str = Field(default="public", description="目标表所在 schema")
    table_name: str = Field(..., description="目标表名（如 device_events）")


class SnapshotSession(BaseModel):
    pid: int
    role_name: str | None = None
    application_name: str | None = None
    client_addr: str | None = None
    state: str | None = None
    backend_start: datetime
    xact_start: datetime | None = None
    state_change: datetime | None = None
    xact_age: timedelta | None = Field(
        None, description="事务已持续时长（序列化为秒）"
    )
    backend_age: timedelta | None = None
    backend_xid: str | None = None
    backend_xmin: str | None = None
    current_query: str | None = None


class TableStats(BaseModel):
    db_name: str
    table_schema: str
    table_name: str
    relation: str
    n_dead_tup_estimate: int = Field(
        ..., description="死元组【估算】值，来自规划器统计，非精确计数"
    )
    n_live_tup_estimate: int = Field(..., description="活元组【估算】值")
    dead_tuple_ratio_estimate: float | None = Field(
        None, description="死元组占比【估算】，统计未刷新时可能为 null"
    )
    vacuum_count: int
    autovacuum_count: int
    last_vacuum: datetime | None = None
    last_autovacuum: datetime | None = None
    last_analyze: datetime | None = None
    last_autoanalyze: datetime | None = None
    relation_size_bytes: int = Field(
        ..., description="表（不含索引）当前占用字节；非本次清理释放量"
    )
    total_relation_size_bytes: int = Field(
        ..., description="表+索引+TOAST 当前占用字节；非本次清理释放量"
    )


class PreparedXact(BaseModel):
    gid: str
    owner: str
    database: str
    prepared: datetime
    age: timedelta | None = None


class ReplicationSlotInfo(BaseModel):
    slot_name: str
    slot_type: str
    active: bool
    xmin: str | None = None
    catalog_xmin: str | None = None
    retained_wal_bytes: int | None = None


class DiagnosticsResponse(BaseModel):
    generated_at: datetime
    table: TableStats
    blocking_sessions: list[SnapshotSession] = Field(
        ...,
        description="持有旧快照、可能阻碍死元组回收的会话；"
        "终止操作必须回传其中的 pid/backend_start/xact_start",
    )
    prepared_transactions: list[PreparedXact]
    inactive_replication_slots: list[ReplicationSlotInfo]
    estimates_notice: str = Field(
        "n_dead_tup_estimate 等为规划器估算值，不是精确死元组计数，"
        "更不代表可释放的磁盘字节；普通 VACUUM 通常不向操作系统归还磁盘空间。"
    )


class TerminateRequest(BaseModel):
    pid: int = Field(..., description="诊断结果中给出的 PID")
    backend_start: datetime = Field(
        ..., description="诊断结果中给出的 backend_start，必须原样回传"
    )
    xact_start: datetime = Field(
        ..., description="诊断结果中给出的事务起点 xact_start，必须原样回传"
    )
    schema_name: str = Field(default="public")
    table_name: str = Field(..., description="目标表名，用于审计关联")


class TerminateResponse(BaseModel):
    terminated: bool
    decision: Literal["terminated", "rejected"]
    target_pid: int
    reason: str
    observed_session: dict[str, Any] | None = Field(
        None, description="再校验时观察到的会话实际状态（审计留痕）"
    )


class VacuumRequest(BaseModel):
    schema_name: str = Field(default="public")
    table_name: str
    wait: bool = Field(
        False,
        description="true 时同步等待本次 VACUUM 结束（受 SYNC_WAIT_SECONDS 限制）；"
        "false 立即返回作业 id，凭作业接口轮询",
    )


class StatsSnapshot(BaseModel):
    n_dead_tup_estimate: int
    n_live_tup_estimate: int
    vacuum_count: int
    autovacuum_count: int
    last_vacuum: datetime | None = None
    last_autovacuum: datetime | None = None
    relation_size_bytes: int
    total_relation_size_bytes: int


class JobResponse(BaseModel):
    id: int
    table_schema: str
    table_name: str
    status: Literal["running", "succeeded", "failed", "interrupted"]
    started_at: datetime
    finished_at: datetime | None = None
    statement_timeout_ms: int
    before_snapshot: dict[str, Any]
    result: dict[str, Any] | None = None
    error_message: str | None = None
    verification_hint: str | None = Field(
        None,
        description="interrupted 作业的核实结论: 重启后对比统计基线判断 VACUUM 是否实际完成",
    )
    disk_notice: str | None = Field(
        None,
        description="relation_size_bytes 为当前占用，差值不等于'已释放磁盘字节'",
    )


class HealthResponse(BaseModel):
    status: Literal["ok", "error"]
    database: str | None = None
    detail: str | None = None
