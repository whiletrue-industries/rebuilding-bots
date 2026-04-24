#!/usr/bin/env bash
# refresh_precheck.sh — dev-local probe of BudgetKey knesset feeds.
#
# Exit 0 if every feed has >0 rows AND a readable datapackage revision.
# Exit 1 otherwise. Prints a per-feed summary to stderr.
#
# Usage: scripts/refresh_precheck.sh
set -euo pipefail

FEEDS=(
  "knesset_committee_decisions"
  "knesset_legal_advisor"
  "knesset_legal_advisor_letters"
  "ethics_committee_decisions"
)
BASE="https://next.obudget.org/datapackages/knesset"

any_empty=0
for feed in "${FEEDS[@]}"; do
  dp_url="$BASE/$feed/datapackage.json"
  index_url="$BASE/$feed/index.csv"

  dp_json="$(curl -sf "$dp_url" || true)"
  if [ -z "$dp_json" ]; then
    printf '%-40s  NO DATAPACKAGE\n' "$feed" >&2
    any_empty=1
    continue
  fi

  rows="$(printf '%s' "$dp_json" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("count_of_rows", 0))')"
  rev="$(printf '%s' "$dp_json" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("revision", ""))')"

  if [ "$rows" = "0" ]; then
    printf '%-40s  EMPTY (revision=%s)\n' "$feed" "$rev" >&2
    any_empty=1
  else
    printf '%-40s  %s rows (revision=%s)\n' "$feed" "$rows" "$rev" >&2
  fi
done

if [ "$any_empty" -ne 0 ]; then
  echo "at least one feed is empty — refresh will raise EmptyUpstreamIndex" >&2
  exit 1
fi
echo "all feeds populated" >&2
