-- 设备事件表（演示/自测用）。
-- 插入后删除一批行并 ANALYZE，使死元组估算立即可见。
CREATE TABLE IF NOT EXISTS public.device_events (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    device_id   integer NOT NULL,
    event_type  text NOT NULL,
    payload     jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO public.device_events (device_id, event_type, payload)
SELECT (g % 50) + 1,
       (ARRAY['boot', 'telemetry', 'alarm', 'shutdown'])[(g % 4) + 1],
       jsonb_build_object('seq', g)
  FROM generate_series(1, 100000) AS g;

-- 删除约 4 万行制造死元组（不立刻 VACUUM，留给维护 API 处理）
DELETE FROM public.device_events
 WHERE id IN (SELECT id FROM public.device_events
              WHERE event_type = 'telemetry' LIMIT 40000);

-- 演示夹具: 关闭该表 autovacuum，确保死元组保留到人工维护；
-- 生产环境不应全局关闭 autovacuum。
ALTER TABLE public.device_events SET (autovacuum_enabled = false);

ANALYZE public.device_events;
