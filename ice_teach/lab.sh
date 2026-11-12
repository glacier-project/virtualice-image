#!/usr/bin/env bash
# ICE Teach (Unity) as digital shadow of the real ICE line (needs the lab network / VPN).
#
#   ice_teach/lab.sh            Unity with graphics
#   ice_teach/lab.sh headless   Unity -batchmode -nographics
#
# Unity is started with -lab (FrostTwin/Endpoints.cs): Kafka 10.197.21.10:30529 from the end of the
# topics in a consumer group of its own (the lab's digital shadow keeps all its partitions), RabbitMQ
# RPC on 10.197.21.10 read only: every RPC write is refused. FrostTwin drive mode does nothing here
# (no twin topic on the real line): pallets follow the line's FIFO messages as in the stock build.
set -euo pipefail
source "$(dirname "$0")/config.sh"
for hp in 10.197.21.10:30529; do  # Kafka only: RabbitMQ is not checked
    timeout 3 bash -c "</dev/tcp/${hp%:*}/${hp#*:}" 2>/dev/null || { echo "$hp unreachable: lab network / VPN?" >&2; exit 1; }
done
start_unity "${1:-graphics}" -lab
echo "Unity on the real line, log: $LOG"
