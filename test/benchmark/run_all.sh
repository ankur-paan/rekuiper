#!/usr/bin/env bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

echo "========================================================================="
echo "  rekuiper 500,000 Event Head-to-Head Competitive Benchmark Suite       "
echo "========================================================================="

# 1. Ensure Python 3
if ! command -v python3 &> /dev/null; then
    echo "python3 is required to run the benchmark suite."
    exit 1
fi

# 2. Run master benchmark
python3 run_all.py
