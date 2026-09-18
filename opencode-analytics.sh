#!/bin/sh

set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PID_FILE="${OPENCODE_ANALYTICS_PID_FILE:-$ROOT/.opencode-analytics.pid}"
INTERVAL="${OPENCODE_ANALYTICS_INTERVAL:-30}"

sync_once() {
    cd "$ROOT"
    python3 ingest.py --database "${OPENCODE_DB:-$HOME/.local/share/opencode/opencode.db}" --output "${DATA_DIR:-$ROOT/data/parquet}" --upload --iceberg
}

watch_loop() {
    while :; do
        sync_once || printf '%s\n' 'sync failed; retrying' >&2
        sleep "$INTERVAL"
    done
}

case "${1:-}" in
    sync)
        sync_once
        ;;
    enable)
        if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
            printf 'collector already running (pid %s)\n' "$(cat "$PID_FILE")"
            exit 0
        fi
        (watch_loop >>"${OPENCODE_ANALYTICS_LOG:-$ROOT/opencode-analytics.log}" 2>&1 & printf '%s' "$!" >"$PID_FILE")
        printf 'collector enabled (pid %s)\n' "$(cat "$PID_FILE")"
        ;;
    disable)
        if [ -f "$PID_FILE" ]; then
            kill "$(cat "$PID_FILE")" 2>/dev/null || true
            rm -f "$PID_FILE"
        fi
        printf '%s\n' 'collector disabled'
        ;;
    status)
        if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
            printf 'enabled (pid %s)\n' "$(cat "$PID_FILE")"
        else
            printf '%s\n' 'disabled'
        fi
        ;;
    *)
        printf '%s\n' 'Usage: ./opencode-analytics.sh {sync|enable|disable|status}' >&2
        exit 1
        ;;
esac
