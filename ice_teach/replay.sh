#!/usr/bin/env bash
# Replay a recorded Kafka capture of the real line into a fresh ICE Teach run.
#
#   ice_teach/replay.sh <capture.jsonl> [headless] [player.py options, e.g. --infinite]
#
# Restarts Unity (local stack), waits until it has its Kafka partitions, starts replay_rpc.py (Unity's
# RPC reads answered with the last replayed value; log $RPC_LOG), then ice_player publishes the topics
# Unity reads, with the recorded timing. Captures: ~/ICE_DIR/kafka-record/record.
set -euo pipefail
source "$(dirname "$0")/config.sh"
CAPTURE=$(realpath "$1"); shift
UI=graphics; if [ "${1:-}" = headless ]; then UI=headless; shift; fi
TOPICS=(ice_data_cell6_state ice_data_conveyor_fifos ice_data_conveyor_control ice_data_conveyor_state
        ice_data_rbkairos_a_control ice_data_rbkairos_b_control ice_data_rbkairos_a_pose ice_data_rbkairos_b_pose
        ice_data_rbkairos_a_arm ice_data_rbkairos_b_arm ice_data_cell4_control ice_data_cell4_state ice_data_cell4_mission
        ice_data_cell2_control ice_data_cell2_state ice_data_cell5_control ice_data_cell5_state)

source "$VIRTUALICE_ROOT/.envrc"
start_unity "$UI"
RPC_LOG=${RPC_LOG:-$ICE_TEACH_ROOT/replay_rpc.log}
"$PY" "$ICE_TEACH_ROOT/replay_rpc.py" >"$RPC_LOG" 2>&1 &
RPC=$!
trap 'kill $RPC 2>/dev/null; [ $UI = graphics ] || taskkill.exe /IM "ICE Teach.exe" /F >/dev/null 2>&1 || true' EXIT  # a headless Unity has no window to close
for _ in $(seq 30); do grep -q answering "$RPC_LOG" && break; kill -0 $RPC || { cat "$RPC_LOG" >&2; exit 1; }; sleep 1; done
echo "Unity connected, replaying $CAPTURE (Unity log: $LOG, RPC log: $RPC_LOG)"
"$PY" "$VIRTUALICE_ROOT/ice_player/player.py" --path "$CAPTURE" --topics "${TOPICS[@]}" --simulate-delays "$@"
