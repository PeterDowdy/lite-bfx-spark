#!/usr/bin/env bash
# Run all integration tests sequentially inside Docker containers.
# Usage: ./scripts/run-tests.sh [service...]
#   With no arguments, runs all default services.
#   Pass service names to run a subset, e.g.: ./scripts/run-tests.sh spark-test spark402

set -uo pipefail

DEFAULT_SERVICES=(spark-test spark402 spark411 databricks)
SERVICES=("${@:-${DEFAULT_SERVICES[@]}}")

PASS=()
FAIL=()

for svc in "${SERVICES[@]}"; do
  echo ""
  echo "========================================"
  echo "  Running tests: $svc"
  echo "========================================"
  docker compose run --rm "$svc" mvn test 2>&1 \
    | grep --line-buffered -E '(Tests run|BUILD|ERROR|FAILURE)'
  rc=${PIPESTATUS[0]}
  if [ "$rc" -eq 0 ]; then
    PASS+=("$svc")
  else
    FAIL+=("$svc")
  fi
done

echo ""
echo "========================================"
echo "  Summary"
echo "========================================"
for svc in "${PASS[@]:-}"; do echo "  PASS  $svc"; done
for svc in "${FAIL[@]:-}"; do echo "  FAIL  $svc"; done

if [ ${#FAIL[@]} -gt 0 ]; then
  exit 1
fi
