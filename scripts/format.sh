#! /bin/bash
set -e

if [[ "$*" == *"--check"* ]]; then
    ruff check --select I
    ruff format --check
else
    ruff check --select I --fix
    ruff format
fi

