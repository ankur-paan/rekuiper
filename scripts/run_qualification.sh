#!/usr/bin/env bash
# run_qualification.sh - Rekuiper Reliability Qualification & Soak Runner
set -euo pipefail

MODE="${1:-short}"
DURATION="${2:-10}"
RATE="${3:-100}"
OUTPUT="${4:-target/qualification_results.json}"
BROKER="${5:-${REKUIPER_TEST_MQTT:-}}"

echo "============================================================"
echo " rekuiper Reliability Qualification Runner"
echo " Mode:     ${MODE}"
echo " Duration: ${DURATION}s"
echo " Rate:     ${RATE} msg/s"
echo " Output:   ${OUTPUT}"
if [ -n "${BROKER}" ]; then
    echo " Broker:   ${BROKER}"
    export REKUIPER_TEST_MQTT="${BROKER}"
fi
echo "============================================================"

if [ "${MODE}" = "soak" ]; then
    echo "[NOTICE] Long-running soak qualification selected (${DURATION}s)."
    echo "[NOTICE] For 24h-72h production soak gate, ensure continuous disk monitoring."
fi

export REKUIPER_QUAL_MODE="${MODE}"
export REKUIPER_QUAL_DURATION_SECS="${DURATION}"
export REKUIPER_QUAL_RATE="${RATE}"
export REKUIPER_QUAL_OUTPUT="${OUTPUT}"

cargo build -p kuiperd
cargo test -p rekuiper-server --test qualification_harness -- --nocapture
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo ""
    echo "[FAILED] Qualification harness failed with exit code ${EXIT_CODE}"
    if [ -f "target/qualification_logs/kuiperd_child.log" ]; then
        echo "Preserved child logs: target/qualification_logs/kuiperd_child.log"
    fi
    exit ${EXIT_CODE}
fi

echo ""
echo "[SUCCESS] Qualification harness run finished."
if [ -f "target/qualification_logs/kuiperd_child.log" ]; then
    echo "Preserved child logs: target/qualification_logs/kuiperd_child.log"
fi
if [ -f "${OUTPUT}" ]; then
    echo "Results written to: ${OUTPUT}"
    cat "${OUTPUT}"
fi
