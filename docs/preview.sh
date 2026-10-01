#!/bin/bash
set -euo pipefail

PORT="${1:-8080}"
THIS_DIR="$(cd "$(dirname "$(readlink "$0" || echo "$0")")"; pwd -P)"

cd "$THIS_DIR"
npm run dev -- --port "${PORT}"
