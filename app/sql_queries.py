"""全部 SQL 集中在此，动态标识符用 psycopg.sql 安全拼接。

注意: n_dead_tup / n_live_tup 是 *planner 的估算值*，不是精确计数，
也不能被解读为"可释放磁盘字节"。
"""
from __future__ import annotations

from psycopg import sql

from .config import settings


def _q(ident: str) -> sql.Composed:
    return sql.Identifier(ident)


# --- 维护作业表 / 审计表 -----------------------------------------------------

CREATE_SCHEMA = sql.SQL("CREATE SCHEMA IF NOT EXISTS {sch}").format(
    sch=_q(settings.job_schema)
)

CREATE_JOB_TABLE = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {sch}.{tbl} (
        id              bigserial PRIMARY KEY,
        table_schema    text        NOT NULL,
        table_name      text        NOT NULL,
        status          text        NOT NULL CHECK (status IN
                            ('running','succeeded','failed','interrupted')),
        started_at      timestamptz NOT NULL DEFAULT now(),
        finished_at     timestamptz,
        statement_timeout_ms integer NOT NULL,
        -- VACUUM 启动前的统计基线，用于重启后核实是否真的跑过
        before_snapshot jsonb       NOT NULL,
        result          jsonb,
        error_message   text
    )
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

# 同一张表同一时刻只允许一个 running 作业（跨进程/跨重启生效）
CREATE_JOB_UNIQUE = sql.SQL(
    """
    CREATE UNIQUE INDEX IF NOT EXISTS {idx}
        ON {sch}.{tbl} (table_schema, table_name)
        WHERE status = 'running'
    """
).format(
    idx=_q(f"{settings.job_table}_one_running_per_table"),
    sch=_q(settings.job_schema),
    tbl=_q(settings.job_table),
)

CREATE_AUDIT_TABLE = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {sch}.{tbl} (
        id              bigserial PRIMARY KEY,
        occurred_at     timestamptz NOT NULL DEFAULT now(),
        target_pid      integer     NOT NULL,
        claimed_backend_start timestamptz NOT NULL,
        claimed_xact_start    timestamptz NOT NULL,
        decision        text        NOT NULL,
        terminated      boolean     NOT NULL,
        observed_state  jsonb       NOT NULL,
        detail          text
    )
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.audit_table))

# 服务重启/崩溃恢复: 把没有结局的作业标为 interrupted
SWEEP_INTERRUPTED = sql.SQL(
    """
    UPDATE {sch}.{tbl}
       SET status = 'interrupted',
           finished_at = now(),
           error_message = '进程退出或服务重启，VACUUM 结果未经确认；可凭此作业核实或重试'
     WHERE status = 'running'
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

INSERT_JOB = sql.SQL(
    """
    INSERT INTO {sch}.{tbl}
        (table_schema, table_name, status, statement_timeout_ms, before_snapshot)
    VALUES (%s, %s, 'running', %s, %s)
    RETURNING id
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

FINISH_JOB = sql.SQL(
    """
    UPDATE {sch}.{tbl}
       SET status = %s, finished_at = now(), result = %s, error_message = %s
     WHERE id = %s AND status = 'running'
    RETURNING id
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

GET_JOB = sql.SQL(
    "SELECT * FROM {sch}.{tbl} WHERE id = %s"
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

LIST_JOBS = sql.SQL(
    """
    SELECT * FROM {sch}.{tbl}
     WHERE table_schema = %s AND table_name = %s
     ORDER BY id DESC LIMIT %s
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

# --- 业务表诊断 --------------------------------------------------------------

# 表存在性
RELATION_EXISTS = sql.SQL(
    """
    SELECT relpersistence
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = %s AND c.relname = %s
    """
)

# 死元组等统计: 估算值
TABLE_STATS = sql.SQL(
    """
    SELECT current_database() AS db_name,
           n.nspname AS table_schema,
           c.relname AS table_name,
           c.oid::regclass::text AS relation,
           pg_stat_get_dead_tuples(c.oid)  AS n_dead_tup_estimate,
           pg_stat_get_live_tuples(c.oid)  AS n_live_tup_estimate,
           pg_stat_get_vacuum_count(c.oid) AS vacuum_count,
           pg_stat_get_autovacuum_count(c.oid)      AS autovacuum_count,
           stat.last_vacuum,
           stat.last_autovacuum,
           pg_relation_size(c.oid)                 AS relation_size_bytes,
           pg_total_relation_size(c.oid)           AS total_relation_size_bytes,
           GREATEST(pg_stat_get_dead_tuples(c.oid), 0)
               / NULLIF(pg_stat_get_live_tuples(c.oid)
                        + pg_stat_get_dead_tuples(c.oid), 0)
               AS dead_tuple_ratio_estimate,
           stat.last_analyze,
           stat.last_autoanalyze
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      LEFT JOIN pg_stat_user_tables stat
             ON stat.relid = c.oid
     WHERE n.nspname = %s AND c.relname = %s
    """
)

# 可能阻碍死元组回收的旧快照会话:
# 判据是"事务仍开启"（xact_start 非空且非本维护进程）。
# 注意 READ COMMITTED 下语句结束后快照会注销、backend_xmin 可能为 NULL，
# 但只要事务还开着，下一条语句即可重新持有旧 xmin 视角的快照，
# 因此 idle in transaction 会话仍是最常见的回收阻碍，均列出并标明状态。
BLOCKING_SNAPSHOTS = sql.SQL(
    """
    SELECT pid,
           usename              AS role_name,
           application_name,
           client_addr,
           state,
           backend_start,
           xact_start,
           state_change,
           now() - xact_start   AS xact_age,
           now() - backend_start AS backend_age,
           backend_xid,
           backend_xmin,
           left(query, 200)     AS current_query
      FROM pg_stat_activity
     WHERE datname = current_database()
       AND pid <> pg_backend_pid()
       AND xact_start IS NOT NULL
       AND state <> 'idle'
     ORDER BY xact_start, backend_start
     LIMIT %s
    """
)

# 其它同样可能让 VACUUM 无法回收的因素，一并呈现
PREPARED_XACTS = sql.SQL(
    """
    SELECT transaction AS gid, owner, database,
           prepared, now() - prepared AS age
      FROM pg_prepared_xacts
     WHERE database = current_database()
     ORDER BY prepared
    """
)

REPLICATION_SLOTS = sql.SQL(
    """
    SELECT slot_name, slot_type, active, xmin, catalog_xmin,
           pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_wal_bytes
      FROM pg_replication_slots
     WHERE NOT active
     ORDER BY slot_name
    """
)

# --- 会话终止前的再校验 ------------------------------------------------------

# 只能结束: 同库 + PID + backend_start + xact_start 全部吻合 +
# 仍是 idle in transaction 的原会话，任一变化即拒绝。
RECHECK_SESSION = sql.SQL(
    """
    SELECT pid, datname, state, backend_start, xact_start,
           usename AS role_name, application_name, client_addr,
           left(query, 200) AS current_query
      FROM pg_stat_activity
     WHERE pid = %s
       AND datname = current_database()
       AND backend_start = %s
       AND xact_start = %s
    """
)

AUDIT_TERMINATION = sql.SQL(
    """
    INSERT INTO {sch}.{tbl}
        (target_pid, claimed_backend_start, claimed_xact_start,
         decision, terminated, observed_state, detail)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.audit_table))
