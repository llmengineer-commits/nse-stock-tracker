#!/usr/bin/env bash
# scripts/check_pipeline.sh
#
# Quick sanity check — confirms all services are up and ticks are flowing.
# Run this any time you want to verify the pipeline is healthy.
#
# Usage: bash scripts/check_pipeline.sh

echo "══════════════════════════════════════"
echo "  NSE Stock Tracker — Pipeline Check"
echo "══════════════════════════════════════"
echo ""

# ── Service health ──────────────────────────────────────────────────────────

check_service() {
  local name=$1
  local url=$2
  local expected=$3

  response=$(curl -sf "$url" 2>/dev/null || echo "FAIL")
  if echo "$response" | grep -q "$expected"; then
    echo "  ✅  $name"
  else
    echo "  ❌  $name  (expected '$expected', got: $response)"
  fi
}

echo "Services:"
check_service "Redpanda"         "http://localhost:9644/v1/cluster/health_overview"  "is_healthy"
check_service "ClickHouse"       "http://localhost:8123/ping"                        "Ok."
check_service "Flink"            "http://localhost:8081/overview"                    "taskmanagers"
check_service "Redpanda Console" "http://localhost:8080"                             "html"
check_service "Kestra"           "http://localhost:8082/api/v1/flows"                "namespace"

echo ""

# ── Tick count ──────────────────────────────────────────────────────────────

echo "ClickHouse — tick count today:"
docker exec nse_clickhouse clickhouse-client \
  --user nse_user \
  --password "${CLICKHOUSE_PASSWORD:-nse_secret_2026}" \
  --database nse \
  --query "
    SELECT
      toDate(inserted_at)     AS day,
      count()                 AS total_ticks,
      countDistinct(ticker)   AS unique_tickers,
      min(price)              AS min_price,
      max(price)              AS max_price
    FROM raw_ticks
    WHERE toDate(inserted_at) >= today() - 1
    GROUP BY day
    ORDER BY day DESC
    FORMAT PrettyCompact
  " 2>/dev/null || echo "  (ClickHouse not reachable or no data yet)"

echo ""

# ── Flink jobs ──────────────────────────────────────────────────────────────

echo "Flink running jobs:"
curl -sf http://localhost:8081/jobs/overview 2>/dev/null \
  | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
if not jobs:
    print('  (no running jobs — run: bash scripts/submit_flink_job.sh)')
for j in jobs:
    state = j.get('state', '?')
    name  = j.get('name', '?')
    icon  = '✅' if state == 'RUNNING' else '⚠️'
    print(f'  {icon}  {name}  [{state}]')
" 2>/dev/null || echo "  (Flink not reachable)"

echo ""
echo "UI Links:"
echo "  Redpanda Console → http://localhost:8080"
echo "  Flink Dashboard  → http://localhost:8081"
echo "  Kestra UI        → http://localhost:8082"
echo ""
