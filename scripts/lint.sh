#! /bin/bash
set -e

FORMAT_SCRIPT=$(dirname "$0")/format.sh

printerr() { printf '\x1b[1;31m%s\x1b[m\n' "$@" ; }
printok() { printf '\x1b[1;32m%s\x1b[m\n' "$@" ; }
try_command() {
    set -e
    local name=$1
    shift
    if "$@" ; then
        printok " ✔ $name"
        return 0
    else
        printerr " ✘ $name"
        return 1
    fi
}

try_command "Lint" uv run ruff check
try_command "Format" "$FORMAT_SCRIPT" --check

WEB_DIR=$(dirname "$0")/../web
if [[ -d "$WEB_DIR/node_modules" ]]; then
    try_command "Web check" npm --prefix "$WEB_DIR" run --silent check
fi
