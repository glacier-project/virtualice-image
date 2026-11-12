#!/usr/bin/env bash
# Fresh ICE Teach (Unity) + Frost gadget run (ice-frost-ext, published by its KafkaBridge) on the
# local virtualice-image stack.
#
#   ice_teach/demo.sh            Unity with graphics
#   ice_teach/demo.sh headless   Unity -batchmode -nographics
#
# Creates the twin topic (prepare_kafka.py), restarts Unity, waits until it has its Kafka partitions
# (and FrostTwin is ready, in drive mode), then runs ice-frost-ext GadgetMain in real time
# (build it first: cd ice-frost-ext && lfc src/GadgetMain.lf, with its .venv active).
# Paths: config.sh.  Frost log: $FROST_LOG (default ice-frost-ext/gadget_unity.log).
set -euo pipefail
source "$(dirname "$0")/config.sh"
FROST_LOG=${FROST_LOG:-$FROST_EXT/gadget_unity.log}
[ -f "$FROST_EXT/src-gen/GadgetMain/GadgetMain.py" ] || { echo "build ice-frost-ext first (lfc src/GadgetMain.lf)" >&2; exit 1; }
if pgrep -f "GadgetMain.py" >/dev/null; then echo "a GadgetMain run is already active" >&2; exit 1; fi

source "$VIRTUALICE_ROOT/.envrc"
"$PY" "$ICE_TEACH_ROOT/prepare_kafka.py"
start_unity "${1:-graphics}"
if [ "$(cat "$ICE_TEACH_DATA/FrostTwin.enabled" 2>/dev/null)" = drive ]; then
    for _ in $(seq 60); do grep -a "FROST TWIN: ready" "$LOG" >/dev/null && break; sleep 1; done
    grep -a "FROST TWIN: ready" "$LOG" >/dev/null || { echo "FrostTwin not ready, see $LOG" >&2; exit 1; }
fi
echo "Unity connected, starting Frost (log: $FROST_LOG)"

cd "$FROST_EXT"
KAFKA_BOOTSTRAP_SERVERS=localhost:9093 exec "$PY" src-gen/GadgetMain/GadgetMain.py --fast false >"$FROST_LOG" 2>&1
