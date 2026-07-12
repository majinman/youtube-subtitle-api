#!/bin/bash
set -euo pipefail

exec uvicorn main:app --host "${HOST:-127.0.0.1}" --port "${PORT:-8000}"
