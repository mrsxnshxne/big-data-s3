#!/bin/sh

set -eu

case "${1:-}" in
    upload)
        exec "$(dirname "$0")/opencode-analytics.sh" sync
        ;;
    enable|disable|status)
        exec "$(dirname "$0")/opencode-analytics.sh" "$1"
        ;;
    *)
        printf '%s\n' 'Usage: ./opencode-s3-logs.sh {upload|enable|disable|status}' >&2
        exit 1
        ;;
esac
