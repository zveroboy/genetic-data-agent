#!/usr/bin/env bash
#
# Idempotent bootstrap for the two allowlisted ingestion sources.
#
# Seeds exactly:
#   s3://genomic-data/samples/demo_user.vcf
#   s3://genomic-data/samples/na12878_hg001.vcf.gz
#
# These are the only keys `ts-api-agent/src/application/dataset-catalog.ts` will ever ingest.
# An object is skipped only when its stored `sha256` user metadata equals the SHA-256 of the
# local file, so a truncated or replaced object is re-uploaded instead of trusted. The same
# metadata entry is what `publishDataset` and the Rust uploader use for content verification.
#
# The ClinVar reference bootstrap is deliberately NOT part of this script; it lives in
# `scripts/download_to_s3.sh`.
set -euo pipefail

S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
if [ "$S3_ENDPOINT" = "http://minio:9000" ] && ! curl -s --connect-timeout 1 http://minio:9000 >/dev/null 2>&1; then
  S3_ENDPOINT="http://localhost:9000"
fi

SOURCE_BUCKET="${S3_SOURCE_BUCKET:-genomic-data}"
ARTIFACT_BUCKET="${S3_ARTIFACT_BUCKET:-genomic-artifacts}"

export AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY:-admin}"
export AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY:-password123}"
export AWS_DEFAULT_REGION="${S3_REGION:-us-east-1}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NA12878_URL="https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/NA12878_HG001/latest/GRCh38/HG001_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"

if ! command -v aws >/dev/null 2>&1; then
  echo "✖ the aws CLI is required to seed MinIO; install it (brew install awscli) and re-run" >&2
  exit 1
fi

aws_s3() {
  aws --endpoint-url "$S3_ENDPOINT" "$@"
}

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

# Stored `sha256` metadata of an object, or an empty string when the object or the entry is
# absent. Never treated as an error: absence simply means "upload it".
remote_checksum() {
  local bucket="$1" key="$2" value
  value="$(aws_s3 s3api head-object --bucket "$bucket" --key "$key" \
    --query 'Metadata.sha256' --output text 2>/dev/null || true)"
  if [ "$value" = "None" ]; then
    value=""
  fi
  printf '%s' "$value"
}

ensure_bucket() {
  if ! aws_s3 s3api head-bucket --bucket "$1" >/dev/null 2>&1; then
    aws_s3 s3 mb "s3://$1" >/dev/null
    echo "  created bucket s3://$1"
  fi
}

# seed_object <local-file> <key>
seed_object() {
  local local_file="$1" key="$2" local_sha remote_sha
  local_sha="$(sha256_of "$local_file")"
  remote_sha="$(remote_checksum "$SOURCE_BUCKET" "$key")"

  if [ "$remote_sha" = "$local_sha" ]; then
    echo "  ✔ s3://${SOURCE_BUCKET}/${key} already matches sha256 ${local_sha}; skipping"
    return
  fi

  if [ -n "$remote_sha" ]; then
    echo "  ! s3://${SOURCE_BUCKET}/${key} has sha256 ${remote_sha}, expected ${local_sha}; re-uploading"
  fi

  aws_s3 s3 cp "$local_file" "s3://${SOURCE_BUCKET}/${key}" --metadata "sha256=${local_sha}" >/dev/null
  echo "  ✔ uploaded s3://${SOURCE_BUCKET}/${key} (sha256 ${local_sha})"
}

echo "Seeding allowlisted ingestion sources into ${S3_ENDPOINT}"
ensure_bucket "$SOURCE_BUCKET"
ensure_bucket "$ARTIFACT_BUCKET"

echo "[1/2] demo-small -> s3://${SOURCE_BUCKET}/samples/demo_user.vcf"
DEMO_LOCAL="${REPO_ROOT}/tests/fixtures/demo_user.vcf"
if [ ! -f "$DEMO_LOCAL" ]; then
  echo "✖ missing ${DEMO_LOCAL}; the demo VCF is tracked in this repository" >&2
  exit 1
fi
seed_object "$DEMO_LOCAL" "samples/demo_user.vcf"

echo "[2/2] na12878-full -> s3://${SOURCE_BUCKET}/samples/na12878_hg001.vcf.gz"
NA12878_LOCAL="${NA12878_LOCAL_FILE:-${REPO_ROOT}/data/na12878_hg001.vcf.gz}"
if [ ! -f "$NA12878_LOCAL" ]; then
  echo "  downloading the GIAB NA12878/HG001 benchmark VCF (this is large and cached in data/)"
  mkdir -p "$(dirname "$NA12878_LOCAL")"
  curl -sSL --fail -o "${NA12878_LOCAL}.part" "$NA12878_URL"
  mv "${NA12878_LOCAL}.part" "$NA12878_LOCAL"
fi
seed_object "$NA12878_LOCAL" "samples/na12878_hg001.vcf.gz"

echo "Done. Exactly the two allowlisted source objects are present."
