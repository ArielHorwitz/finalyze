#! /bin/bash
# Serve the built web app over a fresh demo, for the end-to-end tests.
set -e
cd "$(dirname "$0")/../.."
dir=${FINALYZE_E2E_DIR:-/tmp/finalyze-web-e2e}
uv run python -m finalyze.demo "$dir" --end-date "${FINALYZE_E2E_END_DATE:-2025-06-15}" > /dev/null
exec uv run finalyze -C "$dir" --port "${FINALYZE_E2E_PORT:-8766}"
