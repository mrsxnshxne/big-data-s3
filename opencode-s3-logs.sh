#!/bin/sh

set -eu

COMMAND="${1:-}"
BUCKET="${S3_BUCKET:-rustfs-test}"
ENDPOINT="${S3_ENDPOINT:-http://rustfs:9000}"
LOG_DIR="${OPENCODE_LOG_DIR:-$HOME/.local/share/opencode/log}"
PREFIX="${S3_LOG_PREFIX:-opencode}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-rustfsadmin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-rustfsadmin}"

export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

run_aws() {
    if command -v aws >/dev/null 2>&1; then
        aws "$@"
    elif command -v docker >/dev/null 2>&1; then
        docker compose run --rm --no-deps aws-cli "$@"
    else
        printf 'AWS CLI is required. Install awscli or Docker.\n' >&2
        exit 1
    fi
}

usage() {
    printf '%s\n' \
        'Usage:' \
        '  ./opencode-s3-logs.sh upload [log-directory]' \
        '  ./opencode-s3-logs.sh list [prefix]' \
        '  ./opencode-s3-logs.sh read <object-key>'
}

ensure_bucket() {
    if ! run_aws --endpoint-url "$ENDPOINT" s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
        run_aws --endpoint-url "$ENDPOINT" s3api create-bucket --bucket "$BUCKET" >/dev/null
    fi
}

upload_logs() {
    directory="${1:-$LOG_DIR}"

    if [ ! -d "$directory" ]; then
        printf 'Log directory not found: %s\n' "$directory" >&2
        exit 1
    fi

    ensure_bucket
    find "$directory" -type f -name '*.log' -print | while IFS= read -r file; do
        relative=${file#"$directory"/}
        key="$PREFIX/$relative"
        run_aws --endpoint-url "$ENDPOINT" s3 cp "$file" "s3://$BUCKET/$key" --only-show-errors
        printf 'Uploaded: %s\n' "$key"
    done
}

case "$COMMAND" in
    upload)
        upload_logs "${2:-$LOG_DIR}"
        ;;
    list)
        prefix="${2:-$PREFIX}"
        run_aws --endpoint-url "$ENDPOINT" s3 ls "s3://$BUCKET/$prefix/"
        ;;
    read)
        if [ -z "${2:-}" ]; then
            usage >&2
            exit 1
        fi
        run_aws --endpoint-url "$ENDPOINT" s3 cp "s3://$BUCKET/$2" -
        ;;
    *)
        usage >&2
        exit 1
        ;;
esac
