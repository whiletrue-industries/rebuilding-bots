#!/bin/sh
set -eu

# ─────────────────────────────────────────────────────────────────────────
# Cache subdirs. /srv/cache is an EFS mount in staging/prod (see
# infra/envs/*/main.tf), which shadows the subdirs the Dockerfile
# pre-creates. Re-create them here so KVFile can open its sqlite files.
# This is a no-op locally where /srv/cache is just a container-local dir.
# ─────────────────────────────────────────────────────────────────────────
mkdir -p /srv/cache/metadata /srv/cache/embedding

# ─────────────────────────────────────────────────────────────────────────
# Cold-start flow (when SYNC_ON_STARTUP=1, i.e. ECS staging/prod):
#
#   1. Kick off `botnim sync --reindex` in the background. This populates
#      Elasticsearch from source files and takes ~5 minutes on cold start.
#   2. Immediately exec uvicorn in the foreground so the ALB health check
#      (/health) passes. If we ran sync synchronously, the ALB would mark
#      the target unhealthy and kill the task long before sync finished.
#
# During the sync window, /health returns 200 and /retrieve returns 500
# with "no such index". Once sync completes, /retrieve starts serving
# real data. Users are expected to retry through the assistants loop.
#
# Note: init-clean-es (a separate ECS container, not this script) wipes
# stale ES data before the ES sidecar boots, so we don't need recovery
# logic here.
# ─────────────────────────────────────────────────────────────────────────

run_sync_async() {
  echo "[api_server.sh] background sync: waiting for local Elasticsearch..."
  for i in $(seq 1 120); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:9200" || echo 000)
    case "$code" in
      200|401)
        echo "[api_server.sh] background sync: Elasticsearch reachable after ${i}s (HTTP $code)"
        break
        ;;
    esac
    sleep 2
  done

  : "${ENVIRONMENT:?ENVIRONMENT must be set when SYNC_ON_STARTUP=1}"
  echo "[api_server.sh] background sync: starting 'botnim sync ${ENVIRONMENT} all --backend es --reindex'"
  if AIRTABLE_API_KEY=dummy botnim sync "${ENVIRONMENT}" all --backend es --reindex; then
    echo "[api_server.sh] background sync: complete"
  else
    echo "[api_server.sh] background sync: FAILED"
  fi
}

if [ "${SYNC_ON_STARTUP:-0}" = "1" ]; then
  run_sync_async > /proc/1/fd/1 2>&1 &
  echo "[api_server.sh] background sync started (pid $!)"
fi

exec uvicorn server:app --host 0.0.0.0 --port 8000
