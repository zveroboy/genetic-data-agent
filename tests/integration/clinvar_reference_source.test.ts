/**
 * The committed ClinVar extract really is what the authoritative 193 MB VCF contains.
 *
 * `ts-api-agent/src/infrastructure/database/clinvar-source-records.test.ts` proves the
 * coordinate table is derived from `tests/fixtures/clinvar_source_records.vcf`. That leaves one
 * link unchecked: whether the extract itself still matches ClinVar. This suite closes it by
 * re-scanning the real download and re-rendering the extract from scratch.
 *
 * ## Why it lives here and not in `npm test`
 *
 * `data/clinvar.vcf.gz` is ~193 MB and git-ignored, so it cannot be a dependency of the unit
 * suite. It is a dependency of *this* suite, alongside the other integration tests that need
 * Docker, MinIO and Temporal — none of which run on a bare clone either.
 *
 * When the download is absent the suite skips, but **loudly**: a banner on stderr and a skip
 * reason naming the missing file and the command that fetches it. A silent skip is the failure
 * mode that let the hand-written table drift in the first place, so it is not an option here.
 *
 * This suite needs no stack: no Docker, no Temporal, no MinIO. It reads one local file.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { describe, it } from 'node:test';

import { REFERENCE_BUILD, REFERENCE_VERSION } from '../../ts-api-agent/src/domain/datasets.ts';
import {
  REFERENCE_TARGETS,
  collectSourceRecords,
  deriveTable,
  readVcfLines,
  renderCoordinateTsv,
  renderSourceExtract,
} from '../../ts-api-agent/src/infrastructure/database/clinvar-source-records.ts';
import { REPO_ROOT } from './support/stack.ts';

/** Overridable so a deployment can point at its own ClinVar release. */
const CLINVAR_VCF = process.env.CLINVAR_VCF ?? path.join(REPO_ROOT, 'data/clinvar.vcf.gz');
const SOURCE_EXTRACT = path.join(REPO_ROOT, 'tests/fixtures/clinvar_source_records.vcf');
const COORDINATES_TSV = path.join(REPO_ROOT, 'tests/fixtures/clinvar_coordinates_grch38.tsv');

const DOWNLOAD_HINT =
  'curl -o data/clinvar.vcf.gz https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz';

const available = fs.existsSync(CLINVAR_VCF);

if (!available) {
  // Loud on purpose. Skipping this suite means nothing in the repository is comparing the
  // committed reference table against ClinVar on this machine.
  const banner = '='.repeat(96);
  process.stderr.write(
    `\n${banner}\n` +
      `!! NOT VERIFYING THE CLINVAR REFERENCE TABLE AGAINST ITS SOURCE\n` +
      `!! '${CLINVAR_VCF}' is missing, so tests/fixtures/clinvar_source_records.vcf is taken on\n` +
      `!! trust. The coordinate table is still checked against that extract by the unit suite.\n` +
      `!! Fetch the source (~193 MB, git-ignored) and re-run to close the loop:\n` +
      `!!   ${DOWNLOAD_HINT}\n` +
      `${banner}\n\n`,
  );
}

const skip = available
  ? false
  : `${CLINVAR_VCF} is absent — fetch it with: ${DOWNLOAD_HINT}`;

describe('the committed ClinVar extract matches the authoritative ClinVar VCF', { skip }, () => {
  it('re-derives the extract and the coordinate table from data/clinvar.vcf.gz', async () => {
    const sourceRecords = await collectSourceRecords(readVcfLines(CLINVAR_VCF), REFERENCE_TARGETS);

    const missing = REFERENCE_TARGETS.filter((target) => !sourceRecords.has(target.rsid));
    assert.deepEqual(
      missing.map((target) => target.rsid),
      [],
      'a declared target is no longer in ClinVar; drop it from REFERENCE_TARGETS rather than ' +
        'leaving a row nothing supports',
    );

    assert.equal(
      renderSourceExtract(sourceRecords),
      fs.readFileSync(SOURCE_EXTRACT, 'utf8'),
      'tests/fixtures/clinvar_source_records.vcf no longer matches ClinVar; regenerate both ' +
        'fixtures with `node scripts/generate_clinvar_reference_tsv.ts` and bump REFERENCE_VERSION',
    );

    const { rows, dropped } = deriveTable(
      sourceRecords,
      REFERENCE_TARGETS,
      REFERENCE_VERSION,
      REFERENCE_BUILD,
    );
    assert.deepEqual(dropped, []);
    assert.equal(
      renderCoordinateTsv(rows),
      fs.readFileSync(COORDINATES_TSV, 'utf8'),
      'the coordinate table does not match what ClinVar says today',
    );
  });

  it('declares GRCh38, the build every row claims', async () => {
    let reference: string | null = null;
    for await (const line of readVcfLines(CLINVAR_VCF)) {
      if (!line.startsWith('#')) break;
      if (line.startsWith('##reference=')) reference = line.slice('##reference='.length);
    }
    assert.equal(
      reference,
      REFERENCE_BUILD,
      'the ClinVar download is not the build the snapshot claims; liftover is out of scope',
    );
  });
});
