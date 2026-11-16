#!/usr/bin/env bash
# farm/collect.sh [RUN_ID]   (default: the latest farm run on bench-farm)
# Downloads each job's result.txt and prints one line per job, sorted by variant, users, rep.
set -euo pipefail
REPO=${REPO:-jowinjestine/twelve-dollar-server-challenge}
RUN=${1:-$(gh run list -R "$REPO" -w farm -b bench-farm -L 1 --json databaseId -q '.[0].databaseId')}
gh run view "$RUN" -R "$REPO" --json status,conclusion,createdAt -q '"run '"$RUN"': \(.status) \(.conclusion) \(.createdAt)"'
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
gh run download "$RUN" -R "$REPO" -D "$T" -p 'farm-*' >/dev/null 2>&1 || true
# one line per variant run: name users verdict ... order= model= rep= env=
cat "$T"/farm-*/result-*.txt 2>/dev/null | sed 's/^RESULT //' | sort -k1,1 | column -t || true
echo "== paired (same runner): whole-run p99 ms per job, variants side by side"
for d in "$T"/farm-*/; do
  echo "$(basename "$d") $(cat "$d"result-*.txt 2>/dev/null | sed 's/^RESULT //' | awk '{for(i=1;i<=NF;i++){if($i~/^whole_p99=/)p=$i; if($i~/^cpumodel=/)c=$i}; printf "%s:%s:%s ", $1, $3, substr(p,11)} END {print c}')"
done | sort
