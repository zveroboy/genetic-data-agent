#!/usr/bin/env bash
set -e

# Configuration (MinIO / DigitalOcean Spaces / AWS S3)
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
S3_BUCKET="${S3_BUCKET:-genomic-data}"
ACCESS_KEY="${S3_ACCESS_KEY:-admin}"
SECRET_KEY="${S3_SECRET_KEY:-password123}"

echo "🚀 Direct Stream Ingestion from NCBI FTP to S3 Bucket '${S3_BUCKET}'..."
echo "   S3 Endpoint: ${S3_ENDPOINT}"

# Check for aws-cli or minio-mc
if command -v aws &> /dev/null; then
  export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
  export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"

  # Create bucket if not exists
  aws --endpoint-url "$S3_ENDPOINT" s3 mb "s3://${S3_BUCKET}" 2>/dev/null || true

  echo "[1/2] Streaming NCBI ClinVar VCF directly into S3..."
  curl -L "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz" | \
    aws --endpoint-url "$S3_ENDPOINT" s3 cp - "s3://${S3_BUCKET}/clinvar.vcf.gz"
  echo "✔ Streamed NCBI ClinVar VCF to s3://${S3_BUCKET}/clinvar.vcf.gz"

  echo "[2/2] Streaming NIST GIAB NA12878 VCF directly into S3..."
  curl -L "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/NA12878_HG001/latest/GRCh38/HG001_GRCh38_1_22_v4.2.1_benchmark.vcf.gz" | \
    aws --endpoint-url "$S3_ENDPOINT" s3 cp - "s3://${S3_BUCKET}/na12878_hg001.vcf.gz"
  echo "✔ Streamed NIST NA12878 VCF to s3://${S3_BUCKET}/na12878_hg001.vcf.gz"

  echo "🎉 Direct S3 Streaming Upload Completed with 0 Local Disk Usage!"
else
  echo "Note: aws CLI tool not installed. Fallback: Downloading to local data/ directory first..."
  ./scripts/download_na12878.sh
fi
