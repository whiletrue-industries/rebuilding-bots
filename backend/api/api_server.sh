#!/bin/bash
set -eu

# ─────────────────────────────────────────────────────────────────────────
# Cache subdirs. /srv/cache is an EFS mount in staging/prod (see
# infra/envs/*/main.tf), which shadows the subdirs the Dockerfile
# pre-creates. Re-create them here so KVFile can open its sqlite files.
# This is a no-op locally where /srv/cache is just a container-local dir.
# ─────────────────────────────────────────────────────────────────────────
mkdir -p /srv/cache/metadata /srv/cache/embedding

# ─────────────────────────────────────────────────────────────────────────
# EFS seed: /srv/specs/unified/extraction is an EFS access point in
# staging/prod. On a fresh EFS (first deploy or after a wipe) it's empty;
# botnim sync then finds no extraction CSVs and the bot serves nothing for
# knesset PDF contexts. Seed from the image-baked copy at /srv/specs-seed
# when the mount is empty. This is a no-op locally (both paths live in the
# container) and a no-op after the first successful refresh writes real
# content to EFS.
# ─────────────────────────────────────────────────────────────────────────
seed_extraction_if_empty() {
  local mount_dir=/srv/specs/unified/extraction
  local seed_dir=/srv/specs-seed/unified/extraction
  if [ ! -d "$seed_dir" ]; then
    return 0
  fi
  mkdir -p "$mount_dir"
  if [ -z "$(ls -A "$mount_dir" 2>/dev/null)" ]; then
    echo "[seed_extraction_if_empty] $mount_dir is empty; seeding from $seed_dir"
    cp -R "$seed_dir"/. "$mount_dir"/
    echo "[seed_extraction_if_empty] done; $(ls "$mount_dir" | wc -l) files seeded"
  else
    echo "[seed_extraction_if_empty] $mount_dir already populated; skip"
  fi
}

seed_extraction_if_empty

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

# Resolve ES password from the env-specific var the task definition sets:
#   staging → ES_PASSWORD_STAGING, prod → ES_PASSWORD_PROD.
# Printed from a helper rather than expanded inline so the password is
# never in the script's own stdout under `set -x`.
resolve_es_password() {
  local var_name
  var_name="ES_PASSWORD_$(echo "${ENVIRONMENT:-}" | tr '[:lower:]' '[:upper:]')"
  eval "printf '%s' \"\${$var_name:-}\""
}

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
    # Post-sync hook: register the S3 snapshot repo (idempotent) and take
    # a named "post-cold-sync" snapshot so we have an offline backup of
    # the freshly-rebuilt indices. A failure here is loud-but-not-fatal:
    # the API keeps serving, and the 6-h scheduled loop will retry.
    if register_snapshot_repo; then
      take_snapshot "post-cold-sync" || echo "[api_server.sh] cold-sync snapshot FAILED (continuing; scheduled loop will retry)"
    else
      echo "[api_server.sh] snapshot repo registration FAILED; scheduled loop will retry"
    fi
  else
    echo "[api_server.sh] background sync: FAILED"
  fi
}

# ─────────────────────────────────────────────────────────────────────────
# S3 snapshot hook (supports post-sync + every-6h scheduled snapshots).
#
# Elasticsearch 8.x ships repository-s3 as a built-in module. The SDK
# inside ES reads credentials from the Fargate task role via the ECS
# container credentials endpoint — no static keys needed. The task role
# is granted the minimum S3 actions in infra/envs/*/backups.tf.
#
# Snapshot naming convention so they're distinguishable in `aws s3 ls`:
#   post-cold-sync-<UTC-timestamp>  — taken at end of cold sync
#   scheduled-<UTC-timestamp>       — taken every 6 h while uvicorn runs
#
# Retention is handled by the existing S3 lifecycle policy on the bucket
# (Glacier at 30 d, expire at 90 d). No pruning inside ES needed.
# ─────────────────────────────────────────────────────────────────────────

# One-shot repo registration. Idempotent — ES accepts repeated PUTs with
# the same settings. Returns 0 only if ES responded 200/201.
register_snapshot_repo() {
  local bucket="botnim-api-es-backups-${ENVIRONMENT}"
  local region="${AWS_REGION:-${AWS_DEFAULT_REGION:-il-central-1}}"
  local pw
  pw=$(resolve_es_password)
  if [ -z "$pw" ]; then
    echo "[api_server.sh] snapshot: ES_PASSWORD_$(echo "$ENVIRONMENT" | tr '[:lower:]' '[:upper:]') is empty; cannot register repo"
    return 1
  fi

  # ES 8.11 bundles an AWS SDK that, for regions launched after its cut-off
  # (il-central-1 went GA 2023-08), picks the wrong regional S3 endpoint
  # and hits IllegalLocationConstraintException. Passing the endpoint
  # explicitly bypasses the SDK's regional-discovery path. `path_style_access`
  # is required for virtual-hosted-style incompatibilities on the newer
  # regions; it costs nothing on older ones.
  local endpoint="s3.${region}.amazonaws.com"
  echo "[api_server.sh] snapshot: registering S3 repo s3://${bucket}/snapshots (region=${region}, endpoint=${endpoint})"
  local code body
  body=$(mktemp)
  code=$(curl -s -o "$body" -w '%{http_code}' \
    -u "elastic:$pw" \
    -X PUT "http://localhost:9200/_snapshot/s3_repo" \
    -H 'Content-Type: application/json' \
    -d "{\"type\":\"s3\",\"settings\":{\"bucket\":\"$bucket\",\"region\":\"$region\",\"endpoint\":\"$endpoint\",\"path_style_access\":true,\"base_path\":\"snapshots\"}}") || code=000

  if [ "$code" = "200" ] || [ "$code" = "201" ]; then
    echo "[api_server.sh] snapshot: repo registered (HTTP $code)"
    rm -f "$body"
    return 0
  fi
  echo "[api_server.sh] snapshot: repo registration FAILED (HTTP $code)"
  cat "$body" | head -c 500 | sed 's/^/[api_server.sh]   /'
  rm -f "$body"
  return 1
}

# Take one snapshot with a prefixed, timestamped name. Waits for
# completion so callers see the real outcome (SUCCESS vs failure).
take_snapshot() {
  local prefix="$1"  # "post-cold-sync" or "scheduled"
  # ES requires snapshot names to be all lowercase, so timestamp uses
  # lowercase separators (e.g. 20260417t120934z) instead of ISO's T/Z.
  local name="${prefix}-$(date -u +%Y%m%dt%H%M%Sz)"
  local pw
  pw=$(resolve_es_password)
  if [ -z "$pw" ]; then
    echo "[api_server.sh] snapshot: password unavailable; skipping $name"
    return 1
  fi

  echo "[api_server.sh] snapshot: taking $name"
  local code body
  body=$(mktemp)
  code=$(curl -s -o "$body" -w '%{http_code}' \
    -u "elastic:$pw" \
    -X PUT "http://localhost:9200/_snapshot/s3_repo/${name}?wait_for_completion=true" \
    -H 'Content-Type: application/json' \
    -d '{"indices":"botnim__*","include_global_state":false}') || code=000

  if [ "$code" = "200" ]; then
    local state
    state=$(python3 -c "import json; d=json.load(open('$body')); print(d.get('snapshot',{}).get('state',''))" 2>/dev/null || echo "?")
    echo "[api_server.sh] snapshot: $name → state=$state (HTTP 200)"
    rm -f "$body"
    [ "$state" = "SUCCESS" ] && return 0 || return 1
  fi
  echo "[api_server.sh] snapshot: $name FAILED (HTTP $code)"
  cat "$body" | head -c 500 | sed 's/^/[api_server.sh]   /'
  rm -f "$body"
  return 1
}

# Every-6-h scheduled loop. Runs for the lifetime of the task; ECS reaps
# it when the task stops. Logs each iteration so we can spot gaps.
run_snapshot_cron() {
  local interval=21600  # 6 h
  # Sleep first so we don't collide with the cold-sync snapshot.
  while true; do
    sleep "$interval"
    if register_snapshot_repo; then
      take_snapshot "scheduled" || echo "[api_server.sh] scheduled snapshot missed this cycle; retrying in 6 h"
    fi
  done
}

if [ "${SYNC_ON_STARTUP:-0}" = "1" ]; then
  run_sync_async > /proc/1/fd/1 2>&1 &
  echo "[api_server.sh] background sync started (pid $!)"

  run_snapshot_cron > /proc/1/fd/1 2>&1 &
  echo "[api_server.sh] snapshot cron started (pid $!, interval 6 h)"
fi

exec uvicorn server:app --host 0.0.0.0 --port 8000
