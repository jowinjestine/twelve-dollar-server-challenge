#!/usr/bin/env bash
# farm/collect.sh [RUN_ID]   (default: the latest farm run on bench-farm)
# Downloads each job's result.txt and prints one line per job, sorted by variant, users, rep.
set -euo pipefail
REPO=${REPO:-jowinjestine/twelve-dollar-server-challenge}
RUN=${1:-$(gh run list -R "$REPO" -w farm -b bench-farm -L 1 --json databaseId -q '.[0].databaseId')}
gh run view "$RUN" -R "$REPO" --json status,conclusion,createdAt -q '"run '"$RUN"': \(.status) \(.conclusion) \(.createdAt)"'
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
gh run download "$RUN" -R "$REPO" -D "$T" -p 'farm-*' >/dev/null 2>&1 || true
for f in "$T"/farm-*/out/result.txt; do
  [ -f "$f" ] || continue
  rep=$(grep -o 'rep=[0-9]*' "$f" | head -1); env=$(grep -o 'env=\[[^]]*\]' "$f" | head -1)
  echo "$(grep '^RESULT' "$f" | sed 's/^RESULT //') $rep $env"
done | sort -k1,1 -k2,2V | column -t || true
