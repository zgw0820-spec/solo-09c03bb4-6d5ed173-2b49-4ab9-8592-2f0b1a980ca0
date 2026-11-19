#!/usr/bin/env bash
# 对本地免安装 PostgreSQL 16 实例跑端到端自测。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOCKET="${PG_SOCKET:-/workspace/.tools/run}"
PORT="${PG_PORT:-55432}"

export DATABASE_URL="postgresql://postgres@/devicedb?host=${SOCKET}&port=${PORT}"
export PYTHONPATH="${ROOT}"

# 注意: 这里运行的是仓库内自有代码，不读取任何下载归档，
# 因此不使用 -I（-I 会同时忽略 user site 中的 psycopg/fastapi）。
exec python3 tests/selftest.py "$@"
