# Paths shared by the ice_teach scripts (sourced). Override them in the environment or in .envrc.
ICE_TEACH_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
VIRTUALICE_ROOT=$(dirname "$ICE_TEACH_ROOT")
# Unity build folder (contains "ICE Teach.exe" and "ICE Teach_Data")
ICE_TEACH_DIR=${ICE_TEACH_DIR:-$HOME/ICE_DIR/purple/VICE_main_builds (2)/VICE_main_builds}
# the Frost line of this repository: ice-frost-ext (GadgetMain + KafkaBridge on the ice-frost submodule)
FROST_EXT=$VIRTUALICE_ROOT/ice-frost-ext
PY=${PY:-$FROST_EXT/.venv/bin/python}
ICE_TEACH_DATA="$ICE_TEACH_DIR/ICE Teach_Data"
export ICE_TEACH_DIR  # read by frost_twin/FrostTwin/FrostTwin.csproj

if [ ! -f "$ICE_TEACH_DIR/ICE Teach.exe" ]; then
    echo "ICE Teach build not found in '$ICE_TEACH_DIR': set ICE_TEACH_DIR" >&2
    return 1 2>/dev/null || exit 1
fi

# start_unity <graphics|headless> [ICE Teach args, e.g. -lab]: closes a running ICE Teach, starts a new
# one and returns once its Kafka consumer has its partitions (the scripts publish only after that:
# every run is a new consumer group starting at the end of the topics, see FrostTwin/Endpoints.cs).
# Unity log: $LOG (%TEMP%\iceteach.log).
unity_running() { tasklist.exe | grep -i "ICE Teach.exe" >/dev/null; }  # not grep -q: SIGPIPE + pipefail
start_unity() {
    local mode=(-desktop) exe_win temp_win log_win
    [ "$1" = headless ] && mode=(-batchmode -nographics); shift
    exe_win=$(wslpath -w "$ICE_TEACH_DIR/ICE Teach.exe")
    temp_win=$(cd /mnt/c && cmd.exe /c "echo %TEMP%" 2>/dev/null | tr -d '\r')
    log_win="$temp_win\\iceteach.log"
    LOG=$(wslpath -u "$log_win")
    # graceful close first; a -batchmode Unity has no window and ignores it, so it is forced after 10 s
    if unity_running; then
        taskkill.exe /IM "ICE Teach.exe" >/dev/null 2>&1 || true
        for _ in $(seq 10); do unity_running || break; sleep 1; done
        for _ in $(seq 10); do unity_running || break; taskkill.exe /IM "ICE Teach.exe" /F >/dev/null 2>&1 || true; sleep 1; done
        if unity_running; then echo "cannot close ICE Teach.exe" >&2; return 1; fi
    fi
    rm -f "$LOG"
    (cd /mnt/c && cmd.exe /c start "" "$exe_win" "${mode[@]}" "$@" -logFile "$log_win" >/dev/null 2>&1) &
    for _ in $(seq 120); do grep -a "ICE TEACH: Kafka assigned\|KAFKA Errore" "$LOG" >/dev/null 2>&1 && break; sleep 1; done
    grep -a "ICE TEACH: Kafka assigned" "$LOG" >/dev/null 2>&1 || { echo "Unity got no Kafka partitions, see $LOG" >&2; return 1; }
    grep -a "ICE TEACH: Kafka\|ICE TEACH: RabbitMQ\|RabbitMQ riuscita" "$LOG" | tr -d '\r'
}
