/**
 * Regenerates the coordinate snapshot's source of truth from the authoritative ClinVar VCF.
 *
 *   node scripts/generate_clinvar_reference_tsv.ts \
 *     --clinvar data/clinvar.vcf.gz \
 *     --out     tests/fixtures/clinvar_coordinates_grch38.tsv \
 *     --extract tests/fixtures/clinvar_source_records.vcf
 *
 * Both outputs are committed. `--out` is what `make reference-snapshot` compiles into
 * `data/reference/<referenceVersion>.duckdb`; `--extract` is the ClinVar records the *featured*
 * rows were derived from, kept verbatim so that derivation can be re-checked in a unit test
 * without the 193 MB download.
 *
 * One pass over the source produces both halves of the table (see `collectCoordinateUniverse`):
 * the machine-selected coordinate universe — pathogenic and expert-reviewed, plus every
 * pharmacogenomic record — and the featured rows, which are included whatever ClinVar classifies
 * them as. The pass reads 4.46M records in about 8 seconds on the shipped ClinVar release; the
 * whole script, gunzip included, is well under a minute.
 *
 * The full ClinVar VCF is a git-ignored artifact. When it is missing this script says so and
 * exits non-zero rather than falling back to the extract — regenerating "from source" against a
 * file this repository itself produced would prove nothing.
 *
 * Nothing here decides where a variant is, or which records belong in the table. See
 * `clinvar-source-records.ts` for both sets of rules.
 */
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

import { REFERENCE_BUILD, REFERENCE_VERSION } from '../ts-api-agent/src/domain/datasets.ts';
import {
  FEATURED_TARGETS,
  collectCoordinateUniverse,
  deriveCoordinateTable,
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

  console.log(
    `[clinvar] one pass over ${clinvarPath}: the coordinate universe plus ` +
      `${FEATURED_TARGETS.length} featured rsIDs…`,
  );
  const startedAt = Date.now();
  const universe = await collectCoordinateUniverse(readVcfLines(clinvarPath), FEATURED_TARGETS);
  const sourceRecords = universe.featuredRecords;
  const stats = universe.stats;
  console.log(
    `[clinvar] read ${stats.dataLines.toLocaleString('en-US')} records in ` +
      `${((Date.now() - startedAt) / 1000).toFixed(1)}s ` +
      `(${stats.withoutRsid.toLocaleString('en-US')} carried no RS=)`,
  );

  console.log('[clinvar] featured targets:');
  for (const target of FEATURED_TARGETS) {
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

  const table = deriveCoordinateTable(
    universe,
    FEATURED_TARGETS,
    REFERENCE_VERSION,
    REFERENCE_BUILD,
  );
  const { rows, dropped } = table;

  for (const drop of dropped) {
    console.warn(`  ! ${drop.rsid} dropped: ${drop.reason}`);
  }
  if (rows.length === 0) {
    throw new Error('refusing to write an empty coordinate table');
  }
  // A featured target that failed to derive is fatal, not a warning: the whole point of the union
  // is that nothing which answers today stops answering, and a 14,000-row table is exactly where
  // one missing row goes unnoticed.
  if (dropped.length > 0) {
    throw new Error(
      `refusing to write a table missing ${dropped.length} featured target(s): ` +
        dropped.map((drop) => drop.rsid).join(', '),
    );
  }

  console.log('[clinvar] coordinate universe:');
  console.log(
    `  pathogenic / likely pathogenic, expert panel or practice guideline: ` +
      `${stats.pathogenicExpertReviewed.toLocaleString('en-US')} record(s)`,
  );
  console.log(`  CLNSIG drug_response: ${stats.drugResponse.toLocaleString('en-US')} record(s)`);
  console.log(`  in both sets: ${stats.inBothSets.toLocaleString('en-US')} record(s)`);
  console.log(
    `  skipped: ${stats.skippedNonCanonicalContig} on non-canonical contigs, ` +
      `${stats.skippedEmptyAllele} with an empty or '.' allele, ` +
      `${stats.skippedNoGeneSymbol} with no GENEINFO`,
  );
  console.log(
    `  collapsed: ${stats.duplicateCoordinates} record(s) sharing a coordinate, ` +
      `${table.supersededByFeatured} superseded by a featured row`,
  );
  console.log(
    `  rows: ${table.featuredRowCount} featured + ${table.selectedRowCount} machine-selected`,
  );

  fs.writeFileSync(extractPath, renderSourceExtract(sourceRecords), 'utf8');
  fs.writeFileSync(outPath, renderCoordinateTsv(rows), 'utf8');

  console.log(
    `✔ ${rows.length.toLocaleString('en-US')} rows → ${path.relative(REPO_ROOT, outPath)} ` +
      `(${REFERENCE_VERSION} / ${REFERENCE_BUILD})`,
  );
  console.log(`✔ source records → ${path.relative(REPO_ROOT, extractPath)}`);
  console.log('  Rebuild the snapshot with: make reference-snapshot');
}

run().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
