#!/bin/sh

set -eu

BUCKET="${S3_BUCKET:-rustfs-test}"
OBJECT="${S3_OBJECT:-test.txt}"
S3_ENDPOINT="${S3_ENDPOINT:-http://rustfs:9000}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-rustfsadmin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-rustfsadmin}"
export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
TMP_DIR=$(mktemp -d)
SOURCE_FILE="$TMP_DIR/source"
RESULT_FILE="$TMP_DIR/result"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

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

printf 'RustFS S3 read/write test\n'

i=0
until run_aws --endpoint-url "$S3_ENDPOINT" s3api list-buckets >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -ge 30 ]; then
        printf 'failed\n'
        printf 'RustFS is not ready after 30 seconds.\n' >&2
        printf 'Endpoint: %s\nAWS CLI response:\n' "$S3_ENDPOINT" >&2
        run_aws --endpoint-url "$S3_ENDPOINT" s3api list-buckets >&2 || true
        exit 1
    fi
    sleep 1
done
printf 'ready\n'

printf 'Creating bucket "%s"... ' "$BUCKET"
if run_aws --endpoint-url "$S3_ENDPOINT" s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
    printf 'already exists\n'
else
    run_aws --endpoint-url "$S3_ENDPOINT" s3api create-bucket --bucket "$BUCKET" >/dev/null
    printf 'created\n'
fi

printf 'Writing object "%s"... ' "$OBJECT"
printf 'RustFS S3 test payload\n' > "$SOURCE_FILE"
run_aws --endpoint-url "$S3_ENDPOINT" s3 cp "$SOURCE_FILE" "s3://$BUCKET/$OBJECT" >/dev/null
printf 'ok\n'

printf 'Reading object "%s"... ' "$OBJECT"
run_aws --endpoint-url "$S3_ENDPOINT" s3 cp "s3://$BUCKET/$OBJECT" "$RESULT_FILE" >/dev/null
cmp "$SOURCE_FILE" "$RESULT_FILE"
printf 'ok\n'

printf 'S3 read/write test passed.\n'
