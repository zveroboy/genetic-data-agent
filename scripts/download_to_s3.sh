#!/usr/bin/env bash
set -eo pipefail

# Configuration (MinIO / DigitalOcean Spaces / AWS S3)
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
if [ "$S3_ENDPOINT" = "http://minio:9000" ] && ! curl -s --connect-timeout 1 http://minio:9000 > /dev/null 2>&1; then
  S3_ENDPOINT="http://localhost:9000"
fi

S3_BUCKET="${S3_BUCKET:-genomic-data}"
ACCESS_KEY="${S3_ACCESS_KEY:-admin}"
SECRET_KEY="${S3_SECRET_KEY:-password123}"

echo "🚀 Direct Stream Ingestion from NCBI FTP to S3 Bucket '${S3_BUCKET}'..."
echo "   S3 Endpoint: ${S3_ENDPOINT}"

# Check for aws-cli
if command -v aws &> /dev/null; then
  export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
  export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"

  # Create bucket if not exists
  aws --endpoint-url "$S3_ENDPOINT" s3 mb "s3://${S3_BUCKET}" 2>/dev/null || true

  echo "[1/2] Checking NCBI ClinVar VCF in S3 bucket '${S3_BUCKET}'..."
  if aws --endpoint-url "$S3_ENDPOINT" s3 ls "s3://${S3_BUCKET}/clinvar.vcf.gz" >/dev/null 2>&1; then
    echo "✔ File 's3://${S3_BUCKET}/clinvar.vcf.gz' already exists in MinIO/S3. Skipping download."
  else
    echo "   Downloading & streaming NCBI ClinVar VCF directly into S3..."
    curl -sSL --fail "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz" | aws --endpoint-url "$S3_ENDPOINT" s3 cp - "s3://${S3_BUCKET}/clinvar.vcf.gz"
    echo "✔ Streamed NCBI ClinVar VCF to s3://${S3_BUCKET}/clinvar.vcf.gz"
  fi

  echo "[2/2] Checking NIST GIAB NA12878 VCF in S3 bucket '${S3_BUCKET}'..."
  if aws --endpoint-url "$S3_ENDPOINT" s3 ls "s3://${S3_BUCKET}/na12878_hg001.vcf.gz" >/dev/null 2>&1; then
    echo "✔ File 's3://${S3_BUCKET}/na12878_hg001.vcf.gz' already exists in MinIO/S3. Skipping download."
  else
    echo "   Downloading & streaming NIST GIAB NA12878 VCF directly into S3..."
    curl -sSL --fail "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/NA12878_HG001/latest/GRCh38/HG001_GRCh38_1_22_v4.2.1_benchmark.vcf.gz" | aws --endpoint-url "$S3_ENDPOINT" s3 cp - "s3://${S3_BUCKET}/na12878_hg001.vcf.gz"
    echo "✔ Streamed NIST NA12878 VCF to s3://${S3_BUCKET}/na12878_hg001.vcf.gz"
  fi

  echo "🎉 Direct S3 Streaming Check Completed with 0 Local Disk Usage!"
else
  echo "Note: aws CLI tool not installed. Fallback: Downloading to local data/ directory first..."
  ./scripts/download_na12878.sh
fi
