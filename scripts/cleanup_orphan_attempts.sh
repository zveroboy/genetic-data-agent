#!/usr/bin/env bash
#
# Reports — and, only when asked, removes — attempt prefixes no manifest points at.
#
# Every ingestion attempt uploads its Parquet objects to
# `datasets/{datasetId}/versions/{artifactVersion}/attempt-{n}/variants/…` *before* anything is
# published, and the manifest is written last. That ordering is what makes a failed or retried
# attempt harmless: nothing reads an attempt prefix unless a manifest names it. It also means
# storage accumulates prefixes that will never be read again — a retried attempt 1, a run that
# failed after upload, a cancelled workflow.
#
# There is deliberately no S3 lifecycle rule for this. A lifecycle expiration can only match on
# prefix and age, and "attempt-1 under this dataset" is indistinguishable by prefix from the
# attempt a live manifest depends on. Deciding requires reading the manifest, so the decision
# lives here, in a command an operator runs, rather than in a bucket rule that silently deletes
# the wrong object at 3am.
#
# Safety, by construction:
#
#   * dry run unless `--delete` is passed;
#   * an attempt prefix is a candidate only when the dataset HAS a manifest and that manifest
#     names a DIFFERENT attempt — a dataset with no manifest at all is left completely alone,
#     because an ingestion may still be in flight;
#   * candidates younger than `--min-age-hours` (default 24) are skipped for the same reason;
#   * nothing outside `datasets/*/versions/*/attempt-*/` is ever considered.
#
# Usage:
#   scripts/cleanup_orphan_attempts.sh                    # report only
#   scripts/cleanup_orphan_attempts.sh --min-age-hours 6  # report, narrower age filter
#   scripts/cleanup_orphan_attempts.sh --delete           # remove the reported prefixes
set -euo pipefail

S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
ARTIFACT_BUCKET="${S3_ARTIFACT_BUCKET:-genomic-artifacts}"
export AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY:-admin}"
export AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY:-password123}"
export AWS_DEFAULT_REGION="${S3_REGION:-us-east-1}"

DELETE=0
MIN_AGE_HOURS=24

while [ $# -gt 0 ]; do
  case "$1" in
    --delete) DELETE=1; shift ;;
    --min-age-hours) MIN_AGE_HOURS="$2"; shift 2 ;;
    -h|--help) sed -n '2,32p' "$0"; exit 0 ;;
    *) echo "unknown argument '$1'" >&2; exit 2 ;;
  esac
done

for tool in aws node; do
  command -v "$tool" >/dev/null 2>&1 || { echo "✖ '$tool' is required" >&2; exit 1; }
done

aws_s3() { aws --endpoint-url "$S3_ENDPOINT" "$@"; }

echo "Scanning s3://${ARTIFACT_BUCKET}/datasets/ at ${S3_ENDPOINT}"
echo "  mode: $([ "$DELETE" -eq 1 ] && echo 'DELETE' || echo 'report only (pass --delete to act)')"
echo "  skipping anything modified in the last ${MIN_AGE_HOURS}h"

listing="$(aws_s3 s3api list-objects-v2 --bucket "$ARTIFACT_BUCKET" --prefix 'datasets/' \
  --query 'Contents[].{k:Key,t:LastModified}' --output json 2>/dev/null || echo '[]')"

# All of the reasoning happens here, over the full listing, so the shell never has to parse S3
# keys or timestamps. Output is one candidate prefix per line.
candidates="$(
  MIN_AGE_HOURS="$MIN_AGE_HOURS" node --input-type=module -e '
    let raw = "";
    for await (const chunk of process.stdin) raw += chunk;
    const objects = JSON.parse(raw || "[]") ?? [];
    const cutoff = Date.now() - Number(process.env.MIN_AGE_HOURS) * 3600_000;

    // datasets/{id}/versions/{version}/attempt-{n}/…
    const ATTEMPT = /^datasets\/([^/]+)\/versions\/([^/]+)\/(attempt-[^/]+)\//;
    const newestByPrefix = new Map();
    const datasetsWithManifest = new Set();

    for (const object of objects) {
      const manifest = /^datasets\/([^/]+)\/manifest\.json$/.exec(object.k);
      if (manifest) datasetsWithManifest.add(manifest[1]);
      const attempt = ATTEMPT.exec(object.k);
      if (!attempt) continue;
      const prefix = object.k.slice(0, attempt[0].length);
      const at = Date.parse(object.t);
      newestByPrefix.set(prefix, Math.max(newestByPrefix.get(prefix) ?? 0, at));
    }

    // Which attempt prefix each published dataset actually depends on is read from the manifest
    // itself, never guessed from the key: "the highest attempt number" is not the same thing.
    // The manifest bodies are fetched by the shell (one `aws s3 cp` each) rather than here, so
    // this stays a pure function of its input. Emit the datasets to inspect and the prefixes
    // seen, and let the shell resolve the rest.
    for (const [prefix, at] of [...newestByPrefix].sort()) {
      const [, datasetId] = /^datasets\/([^/]+)\//.exec(prefix);
      if (!datasetsWithManifest.has(datasetId)) continue;   // never touch an unpublished dataset
      if (at > cutoff) continue;                            // too recent: may be in flight
      console.log(`${datasetId}\t${prefix}`);
    }
  ' <<<"$listing"
)"

orphans=0
kept=0
while IFS=$'\t' read -r dataset_id prefix; do
  [ -n "${prefix:-}" ] || continue
  manifest_attempt="$(
    aws_s3 s3 cp "s3://${ARTIFACT_BUCKET}/datasets/${dataset_id}/manifest.json" - 2>/dev/null \
      | node -e 'let s="";process.stdin.on("data",c=>s+=c).on("end",()=>{try{process.stdout.write(String(JSON.parse(s).attemptPrefix ?? ""))}catch{process.stdout.write("")}})'
  )"
  if [ -z "$manifest_attempt" ]; then
    echo "  ? ${prefix} — the dataset's manifest could not be read; leaving it alone"
    kept=$((kept + 1))
    continue
  fi
  if [ "$prefix" = "$manifest_attempt" ]; then
    kept=$((kept + 1))
    continue
  fi

  orphans=$((orphans + 1))
  echo "  orphan: s3://${ARTIFACT_BUCKET}/${prefix} (live attempt is ${manifest_attempt})"
  if [ "$DELETE" -eq 1 ]; then
    aws_s3 s3 rm "s3://${ARTIFACT_BUCKET}/${prefix}" --recursive >/dev/null
    echo "    removed"
  fi
done <<<"$candidates"

echo "Done. ${orphans} orphan attempt prefix(es), ${kept} live or skipped."
if [ "$orphans" -gt 0 ] && [ "$DELETE" -eq 0 ]; then
  echo "Re-run with --delete to remove exactly the prefixes listed above."
fi
