#!/usr/bin/env bash
set -e

mkdir -p data

echo "[1/2] Downloading official NCBI ClinVar GRCh38 reference (clinvar.vcf.gz)..."
if [ ! -f "data/clinvar.vcf.gz" ]; then
  curl -L -o data/clinvar.vcf.gz "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz"
  echo "✔ Downloaded NCBI ClinVar VCF to data/clinvar.vcf.gz"
else
  echo "✔ data/clinvar.vcf.gz already exists, skipping."
fi

echo "[2/2] Downloading NIST GIAB NA12878 (HG001) benchmark VCF sample..."
if [ ! -f "data/na12878_hg001.vcf.gz" ]; then
  # Download HG001 chr22 clinical slice from NCBI / NIST GIAB FTP
  curl -L -o data/na12878_hg001.vcf.gz "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/NA12878_HG001/latest/GRCh38/HG001_GRCh38_1_22_v4.2.1_benchmark.vcf.gz" || \
  echo "Note: Full 500MB download interrupted or unavailable; you can place your own .vcf.gz file in data/na12878_hg001.vcf.gz"
fi

echo "✔ Real-Data download script ready! Files in data/ directory."
