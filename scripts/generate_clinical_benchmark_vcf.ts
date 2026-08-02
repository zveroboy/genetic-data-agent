/**
 * Builds the synthetic clinical benchmark VCF and the rsID-keyed annotation TSV.
 *
 * These are demo/benchmark inputs, not reference data. The one thing they must not do is invent
 * their own idea of where a variant is: every coordinate here is read from
 * `tests/fixtures/clinvar_coordinates_grch38.tsv`, which
 * `scripts/generate_clinvar_reference_tsv.ts` derives from the authoritative ClinVar VCF. This
 * script used to *write* that table from a hand-typed array, which is how the wrong alleles got
 * in; it is now strictly a consumer of it.
 *
 * The only hand-authored input left is the genotype each synthetic sample carries, which is a
 * property of the fake person the file describes, not of the genome.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { REFERENCE_BUILD, REFERENCE_VERSION } from '../ts-api-agent/src/domain/datasets.ts';
import { COORDINATE_TSV_COLUMNS } from '../ts-api-agent/src/infrastructure/database/clinvar-source-records.ts';

const REPO_ROOT = fileURLToPath(new URL('..', import.meta.url));
const COORDINATES_TSV = path.join(REPO_ROOT, 'tests/fixtures/clinvar_coordinates_grch38.tsv');

/**
 * Genotype the synthetic benchmark sample carries at each target.
 *
 * Taken from the real GIAB NA12878/HG001 GRCh38 benchmark call set where it has a call, and
 * `0/0` where it does not (the benchmark VCF lists only variant sites, so absence inside its
 * high-confidence regions means homozygous reference). rs1050828 sits on chrX, which the
 * `1_22` benchmark file does not cover at all, and rs3892097 falls outside the high-confidence
 * regions — both are `0/0` here as a synthetic stand-in, not as a claim about NA12878.
 */
const SAMPLE_GENOTYPES: Readonly<Record<string, string>> = Object.freeze({
  rs1801133: '0/1',
  rs6025: '0/0',
  rs4244285: '0/1',
  rs4149056: '0/1',
  rs80359550: '0/0',
  rs762551: '1/1',
  rs9923231: '0/1',
  rs1042522: '0/1',
  rs80357906: '0/0',
  rs429358: '0/0',
  rs7412: '0/0',
  rs4988235: '1/1',
  rs3892097: '0/0',
  rs1050828: '0/0',
});

interface CoordinateRow {
  readonly chrom: string;
  readonly pos: string;
  readonly rsid: string;
  readonly ref: string;
  readonly alt: string;
  readonly gene: string;
  readonly phenotype: string;
  readonly clinical_significance: string;
  readonly evidence_note: string;
  readonly reference_version: string;
  readonly reference_build: string;
}

/** Reads the derived coordinate table, refusing a table this build is not pinned to. */
function readCoordinates(): CoordinateRow[] {
  const [header, ...body] = fs.readFileSync(COORDINATES_TSV, 'utf8').trimEnd().split('\n');
  const columns = header!.split('\t');
  if (columns.join('\t') !== COORDINATE_TSV_COLUMNS.join('\t')) {
    throw new Error(`${COORDINATES_TSV} does not have the expected columns`);
  }
  return body.map((line) => {
    const cells = line.split('\t');
    const row = Object.fromEntries(
      columns.map((column, index) => [column, cells[index] ?? '']),
    ) as unknown as CoordinateRow;
    if (row.reference_version !== REFERENCE_VERSION || row.reference_build !== REFERENCE_BUILD) {
      throw new Error(
        `${COORDINATES_TSV} declares '${row.reference_version}'/'${row.reference_build}', ` +
          `but this build is pinned to '${REFERENCE_VERSION}'/'${REFERENCE_BUILD}'`,
      );
    }
    return row;
  });
}

function generateVcfContent(rows: readonly CoordinateRow[]): string {
  const headerLines = [
    '##fileformat=VCFv4.2',
    '##fileDate=20260731',
    '##source=1000Genomes_NA12878_Clinical_Benchmark_v1.0',
    '##reference=GRCh38/hg38',
    `##clinvarSnapshot=${REFERENCE_VERSION}`,
    '##INFO=<ID=RS,Number=1,Type=String,Description="dbSNP rsID">',
    '##INFO=<ID=GENE,Number=1,Type=String,Description="HGNC Gene Symbol">',
    '##INFO=<ID=CLNSIG,Number=1,Type=String,Description="ClinVar Clinical Significance">',
    '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">',
    '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">',
    '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">',
    '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNA12878_BENCHMARK',
  ];

  const variantLines: string[] = [];

  // 1. Every clinical target the derived reference table declares, at its ClinVar coordinate.
  for (const row of rows) {
    const gt = SAMPLE_GENOTYPES[row.rsid];
    if (gt === undefined) {
      throw new Error(`no synthetic genotype declared for ${row.rsid}; add one to SAMPLE_GENOTYPES`);
    }
    const info =
      `RS=${row.rsid};GENE=${row.gene};CLNSIG=${row.clinical_significance.replace(/\s+/g, '_')}`;
    variantLines.push(
      `${row.chrom}\t${row.pos}\t${row.rsid}\t${row.ref}\t${row.alt}\t99.9\tPASS\t${info}\tGT:GQ:DP\t${gt}:99:65`,
    );
  }

  // 2. Generate 500 realistic background polymorphic SNPs across chromosomes chr1-chr22, chrX, chrY
  const bases = ['A', 'C', 'G', 'T'];
  for (let i = 1; i <= 500; i++) {
    const chrNum = (i % 22) + 1;
    const chrom = `chr${chrNum}`;
    const pos = 1000000 + i * 14250;
    const rsid = `rs10000${i}`;
    const ref = bases[i % 4];
    const alt = bases[(i + 1) % 4];
    const gts = ['0/0', '0/1', '1/1'];
    const gt = gts[i % 3];
    variantLines.push(`${chrom}\t${pos}\t${rsid}\t${ref}\t${alt}\t98.5\tPASS\tRS=${rsid};GENE=GENE_${chrNum}\tGT:GQ:DP\t${gt}:85:40`);
  }

  return headerLines.concat(variantLines).join('\n') + '\n';
}

function generateClinVarTsvContent(rows: readonly CoordinateRow[]): string {
  const lines = ['rsid\tgene\tphenotype\tclinical_significance\tevidence_note'];
  for (const row of rows) {
    lines.push(
      [row.rsid, row.gene, row.phenotype, row.clinical_significance, row.evidence_note].join('\t'),
    );
  }
  return lines.join('\n') + '\n';
}

async function run() {
  const vcfPath = path.join(REPO_ROOT, 'tests/fixtures/na12878_clinical_benchmark.vcf');
  const tsvPath = path.join(REPO_ROOT, 'tests/fixtures/clinvar_benchmark.tsv');

  const rows = readCoordinates();
  console.log(
    `[Clinical Benchmark] Using ${rows.length} targets from ${path.relative(REPO_ROOT, COORDINATES_TSV)} ` +
      `(${REFERENCE_VERSION} / ${REFERENCE_BUILD})`,
  );

  console.log('[Clinical Benchmark] Generating realistic NA12878 1000 Genomes VCF...');
  const vcfData = generateVcfContent(rows);
  fs.writeFileSync(vcfPath, vcfData, 'utf-8');
  console.log(`✔ Created VCF benchmark dataset (${vcfData.split('\n').length - 1} total variants): ${vcfPath}`);

  console.log('[Clinical Benchmark] Generating ClinVar clinical annotation TSV...');
  const tsvData = generateClinVarTsvContent(rows);
  fs.writeFileSync(tsvPath, tsvData, 'utf-8');
  console.log(`✔ Created ClinVar TSV dataset: ${tsvPath}`);

  console.log(
    '  The versioned coordinate snapshot is NOT written here; it is derived from ClinVar by\n' +
      '  scripts/generate_clinvar_reference_tsv.ts and compiled by `make reference-snapshot`.',
  );
}

run().catch(console.error);
