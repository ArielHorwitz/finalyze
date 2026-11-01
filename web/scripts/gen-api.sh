#! /bin/bash
# Regenerate src/lib/api-schema.d.ts (TypeScript types of the HTTP API) from
# the FastAPI schema. Run it after changing finalyze/service/models.py or the API.
set -e
cd "$(dirname "$0")/.."
schema=$(mktemp --suffix=.json)
trap 'rm -f "$schema"' EXIT
uv run python -c 'import json; from finalyze.api import create_app; print(json.dumps(create_app(static_dir=None).openapi()))' > "$schema"
# Fields with defaults are optional in requests
npx openapi-typescript "$schema" --default-non-nullable false -o src/lib/api-schema.d.ts
