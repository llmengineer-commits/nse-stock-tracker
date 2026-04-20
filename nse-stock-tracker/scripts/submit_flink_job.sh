#!/usr/bin/env bash
# scripts/submit_flink_job.sh
#
# Submits the NSE Flink streaming job to the running JobManager.
# Run this once after `docker compose up` — Flink will keep the job running
# and restart it automatically if the TaskManager restarts.
#
# Usage: bash scripts/submit_flink_job.sh

set -e

echo "Waiting for Flink JobManager to be ready..."
until curl -sf http://localhost:8081/overview > /dev/null 2>&1; do
  sleep 2
done
echo "Flink is up."

echo "Submitting NSE Flink job..."
docker exec nse_flink_jobmanager \
  flink run \
    --python /opt/flink/jobs/nse_flink_job.py \
    --pyFiles /opt/flink/jobs/ \
    -d

echo ""
echo "Job submitted. Monitor it at http://localhost:8081"
echo "Logs: docker logs nse_flink_taskmanager -f"
