#!/usr/bin/env bash
# Install FrostTwin into the ICE Teach build:  ice_teach/frost_twin/install.sh [drive|trace|off]
#
#   drive  pallets follow the ice-frost conveyor model (topic ice_data_conveyor_twin)
#   trace  original pallet logic, positions logged to ICE Teach_Data/FrostTwin.trace.csv
#   off    original pallet logic (the hook stays, FrostTwin does nothing)
#
# Always starts again from Assembly-CSharp.dll.orig: the byte patches of patch_ice_teach.py use
# fixed offsets, valid only before Mono.Cecil rewrites the assembly.
set -euo pipefail
MODE=${1:-drive}
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/../config.sh"
DATA=$ICE_TEACH_DATA
MANAGED="$DATA/Managed"
DOTNET=~/.dotnet/dotnet
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1

if tasklist.exe | grep -i "ICE Teach.exe" >/dev/null; then echo "close ICE Teach first" >&2; exit 1; fi

cp "$MANAGED/Assembly-CSharp.dll.orig" "$MANAGED/Assembly-CSharp.dll"
python3 "$ICE_TEACH_ROOT/patch_ice_teach.py" "$ICE_TEACH_DIR" | grep -v "already patched" || true

"$DOTNET" build -c Release -o "$HERE/out" "$HERE/FrostTwin" | grep -E "error|Build succeeded"
"$DOTNET" build -c Release -o "$HERE/injector-out" "$HERE/Injector" | grep -E "error|Build succeeded"
cp "$HERE/out/FrostTwin.dll" "$MANAGED/"
python3 - "$DATA/ScriptingAssemblies.json" <<'EOF'
import json, sys
p = sys.argv[1]; j = json.load(open(p))
if "FrostTwin.dll" not in j["names"]:
    j["names"].append("FrostTwin.dll"); j["types"].append(16)  # 16 = user assembly, like Assembly-CSharp
    json.dump(j, open(p, "w"))
EOF
"$DOTNET" "$HERE/injector-out/Injector.dll" "$MANAGED"

case "$MODE" in
    drive|trace) printf '%s\n' "$MODE" > "$DATA/FrostTwin.enabled" ;;
    off) rm -f "$DATA/FrostTwin.enabled" ;;
    *) echo "mode: drive|trace|off" >&2; exit 1 ;;
esac
echo "FrostTwin installed, mode: $MODE"
