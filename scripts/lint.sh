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

try_command "Lint" uvx ruff check
try_command "Format" "$FORMAT_SCRIPT" --check
