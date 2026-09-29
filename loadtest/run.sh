#!/usr/bin/env bash
# Runs a k6 scenario in Docker against Kosh on the host.
#   loadtest/run.sh steady.js -e RATE=100 -e DURATION=60s
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
script="$1"
shift
docker run --rm -i \
  --add-host=host.docker.internal:host-gateway \
  -v "$here:/scripts" \
  grafana/k6:0.54.0 run --quiet "$@" "/scripts/$script"
