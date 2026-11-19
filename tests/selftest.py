"""端到端自测（对真实 PostgreSQL 16 实例，经 ASGI 测试客户端调用 API）。

运行方式（见 tests/run_selftest.sh）:
    DATABASE_URL='postgresql://postgres@/devicedb?host=<socket>&port=55432' \
        python -I tests/selftest.py
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime

import psycopg
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import app  # noqa: E402
from app.config import settings  # noqa: E402

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
results: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    results.append((name, bool(cond), detail))
    print(f"  [{PASS if cond else FAIL}] {name}" + (f" — {detail}" if detail else ""))


def admin_conn() -> psycopg.Connection:
    return psycopg.connect(settings.database_url, autocommit=True)


def wait_status(client: TestClient, job_id: int, timeout: float = 20.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/maintenance/jobs/{job_id}")
        body = r.json()
        if body["status"] != "running":
            return body
        time.sleep(0.2)
    raise TimeoutError(f"job {job_id} still running after {timeout}s")


def main() -> int:
    print("DATABASE_URL =", settings.database_url)

    with TestClient(app) as client:
        # 应用 lifespan 已建好 maintenance schema/表，先清理历史数据
        with admin_conn() as adm:
            adm.execute("DELETE FROM maintenance.vacuum_jobs")
            adm.execute("DELETE FROM maintenance.termination_audit")
            # 关闭测试表 autovacuum，避免死元组被后台自动清理
            adm.execute(
                "ALTER TABLE public.device_events SET ("
                "autovacuum_enabled=false)"
            )
            adm.execute("DROP TABLE IF EXISTS public.big_t")
            adm.execute(
                "CREATE TABLE public.big_t (id int, pad text) "
                "WITH (fillfactor=50, autovacuum_enabled=false)"
            )
            adm.execute(
                "INSERT INTO public.big_t SELECT g, repeat('x', 200) "
                "FROM generate_series(1, 200000) g"
            )
            # 重新制造 device_events 死元组。PG16 的 n_dead_tup 由统计
            # 子系统异步投递，删除后需轮询等待（也正好验证它是估值）。
            def current_dead() -> int:
                return adm.execute(
                    "SELECT n_dead_tup FROM pg_stat_user_tables "
                    "WHERE relname='device_events'"
                ).fetchone()[0] or 0

            if current_dead() == 0:
                adm.execute(
                    "INSERT INTO public.device_events (device_id, event_type) "
                    "SELECT g, 'probe' FROM generate_series(1, 20000) g"
                )
                adm.execute(
                    "DELETE FROM public.device_events WHERE id IN ("
                    "SELECT id FROM public.device_events "
                    "WHERE event_type='probe' LIMIT 10000)"
                )
            deadline = time.time() + 10
            dead = 0
            while time.time() < deadline:
                dead = current_dead()
                if dead > 0:
                    break
                time.sleep(0.25)
            assert dead > 0, f"fixture dead tuples missing: {dead}"

        # 1. 健康检查
        print("\n== 1. 健康检查 ==")
        r = client.get("/health")
        check("health 200 且连通", r.status_code == 200 and r.json()["status"] == "ok")

        # 2. 诊断: 死元组估算 + 阻碍会话
        print("\n== 2. 诊断接口 ==")
        r = client.get("/diagnostics", params={"table_name": "device_events"})
        check("diagnostics 200", r.status_code == 200, str(r.status_code))
        diag = r.json()
        check(
            "死元组为估算字段且为非负整数",
            isinstance(diag["table"]["n_dead_tup_estimate"], int)
            and diag["table"]["n_dead_tup_estimate"] > 0,
            f"n_dead_tup_estimate={diag['table']['n_dead_tup_estimate']}",
        )
        check(
            "响应明确声明估值不等于磁盘释放字节",
            "估算" in diag["estimates_notice"] and "磁盘" in diag["estimates_notice"],
        )
        check(
            "字段命名不含 released/freed bytes",
            all("released" not in k and "freed" not in k
                for k in json.dumps(diag["table"]).lower().split('"')),
        )
        check("不存在表返回 404",
              client.get("/diagnostics", params={"table_name": "nope"}).status_code == 404)

        # 3. 制造 idle in transaction 旧快照
        print("\n== 3. 旧快照 idle in transaction 会话 ==")
        blocker = admin_conn()
        blocker.execute("BEGIN")
        blocker.execute("SELECT count(*) FROM public.device_events")
        time.sleep(0.1)
        r = client.get("/diagnostics", params={"table_name": "device_events"})
        sessions = r.json()["blocking_sessions"]
        mine = next((s for s in sessions
                     if s["state"] == "idle in transaction"), None)
        check("诊断列出 idle in transaction 阻碍会话", mine is not None,
              f"{len(sessions)} 个持快照会话")
        assert mine is not None
        check("会话含 pid/backend_start/xact_start",
              all(mine.get(k) for k in ("pid", "backend_start", "xact_start")))
        bpid = mine["pid"]
        bstart = mine["backend_start"]
        xstart = mine["xact_start"]

        # 4. 篡改身份 -> 拒绝
        print("\n== 4. 终止再校验 ==")
        r = client.post("/sessions/terminate", json={
            "table_name": "device_events", "pid": bpid,
            "backend_start": bstart,
            "xact_start": datetime.fromisoformat(xstart).replace(year=2001)
            .isoformat(),
        })
        check("伪造 xact_start 被拒绝(409 语义/200 rejected)",
              r.status_code == 200 and r.json()["decision"] == "rejected"
              and r.json()["terminated"] is False, r.json().get("reason", ""))

        # 不存在的 PID -> 拒绝
        r = client.post("/sessions/terminate", json={
            "table_name": "device_events", "pid": 999999,
            "backend_start": bstart, "xact_start": xstart,
        })
        check("不存在的 PID 被拒绝",
              r.json()["decision"] == "rejected")

        # 状态变为 active -> 拒绝（用后台线程跑 pg_sleep 维持 active）
        import threading

        def hold_active(conn):
            conn.execute("SELECT pg_sleep(5)")

        t = threading.Thread(target=hold_active, args=(blocker,))
        t.start()
        time.sleep(0.5)  # 事务内查询执行中，服务端 state = active
        r = client.post("/sessions/terminate", json={
            "table_name": "device_events", "pid": bpid,
            "backend_start": bstart, "xact_start": xstart,
        })
        check("会话已变 active 时拒绝终止",
              r.json()["decision"] == "rejected"
              and "idle in transaction" in r.json()["reason"],
              r.json()["reason"])
        t.join()

        # 会话替换（同 PID 不可能，但可模拟: 旧会话结束后 PID 复用语义 ——
        # 直接验证 backend_start 不符: 先 ROLLBACK 终止，再开新会话拿到新 backend）
        blocker.rollback()
        blocker.close()
        blocker2 = admin_conn()
        blocker2.execute("BEGIN")
        blocker2.execute("SELECT 1")
        time.sleep(0.1)
        r = client.get("/diagnostics", params={"table_name": "device_events"})
        mine2 = next(s for s in r.json()["blocking_sessions"]
                     if s["state"] == "idle in transaction")
        r = client.post("/sessions/terminate", json={
            "table_name": "device_events", "pid": mine2["pid"],
            # 用旧会话的 backend_start（已失效/不匹配）
            "backend_start": bstart, "xact_start": mine2["xact_start"],
        })
        check("会话替换后旧 backend_start 被拒绝",
              r.json()["decision"] == "rejected", r.json()["reason"])

        # 正确凭证 -> 终止成功
        r = client.post("/sessions/terminate", json={
            "table_name": "device_events", "pid": mine2["pid"],
            "backend_start": mine2["backend_start"],
            "xact_start": mine2["xact_start"],
        })
        check("凭证完全一致且仍 idle in transaction -> 终止",
              r.status_code == 200 and r.json()["terminated"] is True,
              r.json()["reason"])
        time.sleep(0.2)
        with admin_conn() as adm2:
            alive = adm2.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE pid=%s",
                (mine2["pid"],),
            ).fetchone()[0]
        check("目标会话确实已消失", alive == 0)
        blocker2.close()

        # 审计留痕
        with admin_conn() as adm3:
            n_audit = adm3.execute(
                "SELECT count(*) FROM maintenance.termination_audit"
            ).fetchone()[0]
            n_term = adm3.execute(
                "SELECT count(*) FROM maintenance.termination_audit "
                "WHERE decision='terminated'"
            ).fetchone()[0]
        check("终止尝试全部写入审计表", n_audit >= 4 and n_term == 1,
              f"audit={n_audit}, terminated={n_term}")

        # 5. VACUUM 成功路径与统计回读
        print("\n== 5. VACUUM 作业 ==")
        with admin_conn() as adm4:
            before_dead = adm4.execute(
                "SELECT n_dead_tup FROM pg_stat_user_tables "
                "WHERE relname='device_events'"
            ).fetchone()[0]
            before_vc = adm4.execute(
                "SELECT vacuum_count FROM pg_stat_user_tables "
                "WHERE relname='device_events'"
            ).fetchone()[0]
        r = client.post("/maintenance/vacuum",
                        json={"table_name": "device_events", "wait": True})
        check("VACUUM 请求被受理(202)", r.status_code == 202)
        body = r.json()
        job_id = body["job"].get("id") if "job" in body else body["job_id"]
        if body.get("job", {}).get("status") == "running":
            job = wait_status(client, job_id)
        else:
            job = body["job"]
        check("作业 succeeded", job["status"] == "succeeded",
              job.get("error_message", ""))
        check("结果含 before/after 统计与 VERBOSE 通知",
              "before" in job["result"] and "after" in job["result"]
              and len(job["result"]["vacuum_verbose_notices"]) > 0)
        check("结果含磁盘声明纠正",
              "disk_notice" in job["result"])
        check("死元组估算下降",
              job["result"]["after"]["n_dead_tup_estimate"] < before_dead,
              f"{before_dead} -> {job['result']['after']['n_dead_tup_estimate']}")
        with admin_conn() as adm5:
            after_vc = adm5.execute(
                "SELECT vacuum_count FROM pg_stat_user_tables "
                "WHERE relname='device_events'"
            ).fetchone()[0]
        check("vacuum_count 增加 1", after_vc == before_vc + 1,
              f"{before_vc} -> {after_vc}")

        # 6. 同表并发单飞:
        # 持有与 VACUUM 执行器相同的 advisory lock，让作业确定停在 running；
        # advisory lock 不阻塞目录/统计查询，不会造成测试自死锁。
        print("\n== 6. 同表并发单飞 ==")
        from app.maintenance import _advisory_lock_key

        key = _advisory_lock_key("public", "big_t")
        locker = admin_conn()
        locker.execute("SELECT pg_advisory_xact_lock(%s)", (key,))
        try:
            r1 = client.post("/maintenance/vacuum", json={"table_name": "big_t"})
            check("第一个 big_t 作业受理 202", r1.status_code == 202)
            # 等到作业确实处于 running（执行器在等 advisory lock）
            deadline = time.time() + 10
            j = client.get(f"/maintenance/jobs/{r1.json()['job_id']}").json()
            while time.time() < deadline and j["status"] != "running":
                time.sleep(0.1)
                j = client.get(f"/maintenance/jobs/{r1.json()['job_id']}").json()
            check("第一个作业处于 running（等待 advisory lock）",
                  j["status"] == "running")
            r2 = client.post("/maintenance/vacuum", json={"table_name": "big_t"})
            check("并发第二个同表作业 409", r2.status_code == 409
                  and r2.json()["detail"]["running_job"]["id"] == r1.json()["job_id"])
            # 不同表 key 不同，不互斥
            r3 = client.post("/maintenance/vacuum",
                             json={"table_name": "device_events"})
            check("不同表作业不互斥(202)", r3.status_code == 202)
        finally:
            locker.rollback()  # 释放 advisory xact lock
            locker.close()
        j_big = wait_status(client, r1.json()["job_id"])
        j_dev = wait_status(client, r3.json()["job_id"])
        check("两个作业最终 succeeded",
              j_big["status"] == "succeeded" and j_dev["status"] == "succeeded")

        # 7. wait=true 超时不报完成
        print("\n== 7. 同步等待超时语义 ==")
        r = client.post("/maintenance/vacuum",
                        json={"table_name": "big_t", "wait": True})
        # big_t 已较干净，通常很快完成；这里验证响应里不会把 running 说成成功
        b = r.json()
        if "job" in b and b["job"]["status"] == "running":
            check("超时响应明确标注未确认完成",
                  "未确认" in b.get("message", ""))
            wait_status(client, b["job"]["id"])
        else:
            check("快速完成时返回终态作业", b["job"]["status"] == "succeeded")

        # 8. 作业列表
        r = client.get("/maintenance/jobs",
                       params={"table_name": "device_events"})
        check("作业列表可查且按 id 倒序",
              r.status_code == 200
              and [j["id"] for j in r.json()["jobs"]]
              == sorted((j["id"] for j in r.json()["jobs"]), reverse=True))

    # 9. 服务重启: 未确认作业 -> interrupted + 可核实
    print("\n== 9. 服务重启核实/重试 ==")
    # 手工构造一个 running 作业（模拟进程在 VACUUM 途中被杀）
    with admin_conn() as adm6:
        adm6.execute(
            "INSERT INTO maintenance.vacuum_jobs "
            "(table_schema, table_name, status, statement_timeout_ms, before_snapshot) "
            "VALUES ('public','big_t','running',0, "
            "'{\"n_dead_tup_estimate\": 1, \"vacuum_count\": 0}'::jsonb)"
        )
        orphan_id = adm6.execute(
            "SELECT max(id) FROM maintenance.vacuum_jobs"
        ).fetchone()[0]

    with TestClient(app) as client2:  # 新应用实例 == 服务重启
        r = client2.get(f"/maintenance/jobs/{orphan_id}")
        body = r.json()
        check("重启后孤儿作业被标记 interrupted",
              body["status"] == "interrupted")
        check("interrupted 附核实结论且不报为完成",
              bool(body.get("verification_hint"))
              and "完成" in body["verification_hint"])
        check("错误信息提示可重试", "重试" in (body.get("error_message") or ""))

        # 重试可成功
        r = client2.post("/maintenance/vacuum",
                         json={"table_name": "big_t", "wait": True})
        check("interrupted 后允许重试且 202", r.status_code == 202)
        b = r.json()
        jid = b["job"].get("id") if "job" in b else b["job_id"]
        job = wait_status(client2, jid) if b.get("job", {}).get("status") == "running" else b["job"]
        check("重试作业 succeeded", job["status"] == "succeeded")

    # 10. VACUUM 失败落库（不存在的表在登记前就 404；用权限/超时场景：
    #     statement_timeout 触发 -> failed 且不静默）
    print("\n== 10. 失败路径 ==")
    settings.vacuum_statement_timeout_ms = 1  # 1ms 必然超时
    try:
        with TestClient(app) as client3:
            r = client3.post("/maintenance/vacuum",
                             json={"table_name": "big_t", "wait": True})
            b = r.json()
            jid = b["job"].get("id") if "job" in b else b["job_id"]
            job = wait_status(client3, jid) if b.get("job", {}).get("status") == "running" else b["job"]
            check("超时 VACUUM 标记 failed 并记录错误",
                  job["status"] == "failed"
                  and "statement timeout" in (job.get("error_message") or ""),
                  job.get("error_message", ""))
            # failed 后同表可再次发起
            settings.vacuum_statement_timeout_ms = 0
            r2 = client3.post("/maintenance/vacuum",
                              json={"table_name": "big_t", "wait": True})
            check("failed 后同表可重新发起", r2.status_code == 202)
            b2 = r2.json()
            jid2 = b2["job"].get("id") if "job" in b2 else b2["job_id"]
            job2 = wait_status(client3, jid2) if b2.get("job", {}).get("status") == "running" else b2["job"]
            check("重新发起的作业 succeeded", job2["status"] == "succeeded")
    finally:
        settings.vacuum_statement_timeout_ms = 0

    # 汇总
    print("\n" + "=" * 60)
    failed = [n for n, ok, _ in results if not ok]
    for n, ok, detail in results:
        print(f"  [{PASS if ok else FAIL}] {n}" + (f" — {detail}" if not ok and detail else ""))
    print(f"\n合计 {len(results)} 项, 通过 {len(results)-len(failed)}, 失败 {len(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
