#! /bin/bash
set -e

if [[ "$*" == *"--check"* ]]; then
    uv run ruff check --select I
    uv run ruff format --check
else
    uv run ruff check --select I --fix
    uv run ruff format
fi

