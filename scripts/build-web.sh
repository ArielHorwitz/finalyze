#! /bin/bash
# Build the web app into finalyze/static, where `finalyze` serves it.
set -e
cd "$(dirname "$0")/../web"
if [[ ! -d node_modules ]]; then
    npm ci
fi
npm run build
