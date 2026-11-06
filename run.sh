#!/usr/bin/env bash
# Corre todo el pipeline: ./run.sh   (en Windows: python run.py)
set -euo pipefail
cd "$(dirname "$0")"
python3 run.py "$@"
