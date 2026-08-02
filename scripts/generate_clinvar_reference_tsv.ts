/**
 * Regenerates the coordinate snapshot's source of truth from the authoritative ClinVar VCF.
 *
 *   node scripts/generate_clinvar_reference_tsv.ts \
 *     --clinvar data/clinvar.vcf.gz \
 *     --out     tests/fixtures/clinvar_coordinates_grch38.tsv \
 *     --extract tests/fixtures/clinvar_source_records.vcf
 *
 * Both outputs are committed. `--out` is what `make reference-snapshot` compiles into
 * `data/reference/<referenceVersion>.duckdb`; `--extract` is the handful of ClinVar records the
 * table was derived from, kept verbatim so the derivation can be re-checked in a unit test
 * without the 193 MB download.
 *
 * The full ClinVar VCF is a git-ignored artifact. When it is missing this script says so and
 * exits non-zero rather than falling back to the extract — regenerating "from source" against a
 * file this repository itself produced would prove nothing.
 *
 * Nothing here decides where a variant is. See `clinvar-source-records.ts` for the rules.
 */
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

import { REFERENCE_BUILD, REFERENCE_VERSION } from '../ts-api-agent/src/domain/datasets.ts';
import {
  REFERENCE_TARGETS,
  collectSourceRecords,
  deriveTable,
  readVcfLines,
  renderCoordinateTsv,
  renderSourceExtract,
  selectCanonicalRecord,
} from '../ts-api-agent/src/infrastructure/database/clinvar-source-records.ts';

const REPO_ROOT = fileURLToPath(new URL('..', import.meta.url));

function option(name: string, fallback: string): string {
  const index = process.argv.indexOf(`--${name}`);
  if (index === -1) return fallback;
  const value = process.argv[index + 1];
  if (value === undefined || value.startsWith('--')) {
    throw new Error(`--${name} needs a path`);
  }
  return path.resolve(process.cwd(), value);
}

async function run(): Promise<void> {
  const clinvarPath = option('clinvar', path.join(REPO_ROOT, 'data/clinvar.vcf.gz'));
  const outPath = option('out', path.join(REPO_ROOT, 'tests/fixtures/clinvar_coordinates_grch38.tsv'));
  const extractPath = option(
    'extract',
    path.join(REPO_ROOT, 'tests/fixtures/clinvar_source_records.vcf'),
  );

  if (!fs.existsSync(clinvarPath)) {
    console.error(
      `✖ ClinVar source '${clinvarPath}' does not exist.\n` +
        '  Download it first (it is git-ignored, ~193 MB):\n' +
        '    curl -o data/clinvar.vcf.gz \\\n' +
        '      https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz',
    );
    process.exitCode = 1;
    return;
  }

  console.log(`[clinvar] scanning ${clinvarPath} for ${REFERENCE_TARGETS.length} rsIDs…`);
  const sourceRecords = await collectSourceRecords(readVcfLines(clinvarPath), REFERENCE_TARGETS);

  for (const target of REFERENCE_TARGETS) {
    const candidates = sourceRecords.get(target.rsid) ?? [];
    const chosen = selectCanonicalRecord(candidates);
    if (chosen === null) {
      console.log(`  ${target.rsid.padEnd(11)} ✖ ${candidates.length} record(s), none usable`);
      continue;
    }
    const rejected = candidates.length - 1;
    console.log(
      `  ${target.rsid.padEnd(11)} ✔ ${chosen.chrom}:${chosen.pos} ${chosen.ref}>${chosen.alt} ` +
        `(VariationID ${chosen.variationId}${rejected > 0 ? `, ${rejected} other record(s) rejected` : ''})`,
    );
  }

  const { rows, dropped } = deriveTable(
    sourceRecords,
    REFERENCE_TARGETS,
    REFERENCE_VERSION,
    REFERENCE_BUILD,
  );

  for (const drop of dropped) {
    console.warn(`  ! ${drop.rsid} dropped: ${drop.reason}`);
  }
  if (rows.length === 0) {
    throw new Error('refusing to write an empty coordinate table');
  }

  fs.writeFileSync(extractPath, renderSourceExtract(sourceRecords), 'utf8');
  fs.writeFileSync(outPath, renderCoordinateTsv(rows), 'utf8');

  console.log(
    `✔ ${rows.length} rows → ${path.relative(REPO_ROOT, outPath)} (${REFERENCE_VERSION} / ${REFERENCE_BUILD})`,
  );
  console.log(`✔ source records → ${path.relative(REPO_ROOT, extractPath)}`);
  console.log('  Rebuild the snapshot with: make reference-snapshot');
}

run().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
