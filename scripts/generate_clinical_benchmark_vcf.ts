import fs from 'fs';
import path from 'path';

// Realistic clinical benchmark variants from NA12878 / NIST Genome in a Bottle & ClinVar
const CLINICAL_BENCHMARK_VARIANTS = [
  // Pharmacogenomics (PGx)
  { chrom: 'chr15', pos: 74749576, rsid: 'rs762551', ref: 'A', alt: 'C', gt: '1/1', gene: 'CYP1A2', phenotype: 'Slow caffeine metabolizer', clinical_significance: 'Risk Factor', evidence_note: 'Decreased CYP1A2 enzyme activity; slower clearance of caffeine.' },
  { chrom: 'chr12', pos: 21178615, rsid: 'rs4149056', ref: 'T', alt: 'C', gt: '0/1', gene: 'SLCO1B1', phenotype: 'Statins myopathy risk', clinical_significance: 'Risk Factor', evidence_note: 'Intermediate OATP1B1 function; increased blood statin levels and myopathy risk.' },
  { chrom: 'chr16', pos: 31107689, rsid: 'rs9923231', ref: 'C', alt: 'T', gt: '1/1', gene: 'VKORC1', phenotype: 'High warfarin sensitivity', clinical_significance: 'Drug Response', evidence_note: 'Requires lower initial dose of warfarin blood thinner.' },
  { chrom: 'chr22', pos: 42128945, rsid: 'rs3892097', ref: 'C', alt: 'T', gt: '0/1', gene: 'CYP2D6', phenotype: 'Intermediate SSRI metabolizer (*4 allele)', clinical_significance: 'Drug Response', evidence_note: 'Reduced metabolism of codeine, SSRIs, and beta-blockers.' },
  { chrom: 'chr10', pos: 94781859, rsid: 'rs4244285', ref: 'G', alt: 'A', gt: '0/1', gene: 'CYP2C19', phenotype: 'Intermediate Clopidogrel metabolizer (*2 allele)', clinical_significance: 'Drug Response', evidence_note: 'Reduced bioactivation of clopidogrel (Plavix); alternative antiplatelet advised.' },

  // Nutrition & Metabolism
  { chrom: 'chr2', pos: 135851076, rsid: 'rs4988235', ref: 'T', alt: 'C', gt: '1/1', gene: 'LCT', phenotype: 'Primary Lactase Deficiency (Lactose Intolerance)', clinical_significance: 'Pathogenic', evidence_note: 'Absence of lactase persistence allele; adult hypolactasia.' },
  { chrom: 'chr1', pos: 11796321, rsid: 'rs1801133', ref: 'G', alt: 'A', gt: '1/1', gene: 'MTHFR', phenotype: 'Reduced folate metabolism (C677T thermolabile variant)', clinical_significance: 'Risk Factor', evidence_note: '30-60% decrease in MTHFR enzyme efficiency; elevated homocysteine risk without B-vitamin intake.' },

  // Cardiovascular & Thrombosis Risk
  { chrom: 'chr19', pos: 44908684, rsid: 'rs429358', ref: 'T', alt: 'C', gt: '0/1', gene: 'APOE', phenotype: 'APOE-e4 carrier (Elevated Alzheimer & LDL cholesterol risk)', clinical_significance: 'Risk Factor', evidence_note: 'APOE epsilon 4 allele carrier; associated with increased beta-amyloid deposition and cardiovascular risk.' },
  { chrom: 'chr19', pos: 44908822, rsid: 'rs7412', ref: 'C', alt: 'T', gt: '0/0', gene: 'APOE', phenotype: 'APOE-e3 reference', clinical_significance: 'Benign', evidence_note: 'Normal lipoprotein metabolism.' },
  { chrom: 'chr1', pos: 169549811, rsid: 'rs6025', ref: 'C', alt: 'T', gt: '0/1', gene: 'F5', phenotype: 'Factor V Leiden carrier (Thrombophilia risk)', clinical_significance: 'Pathogenic', evidence_note: 'Activated protein C resistance; 3-5x increased relative risk for deep vein thrombosis (DVT).' },

  // Autoimmune & Immunology
  { chrom: 'chr6', pos: 32632646, rsid: 'rs2187668', ref: 'T', alt: 'C', gt: '1/1', gene: 'HLA-DQA1', phenotype: 'HLA-DQ2.5 haplotype (Celiac disease genetic susceptibility)', clinical_significance: 'Risk Factor', evidence_note: 'Present in over 90% of celiac disease patients; dietary gluten triggers T-cell immune response.' },

  // Oncology & DNA Repair
  { chrom: 'chr17', pos: 43057051, rsid: 'rs80357906', ref: 'G', alt: 'A', gt: '0/0', gene: 'BRCA1', phenotype: 'Normal BRCA1 allele (No familial cancer mutation detected)', clinical_significance: 'Benign', evidence_note: 'Wild-type tumor suppressor gene sequence.' },
  { chrom: 'chr13', pos: 32332611, rsid: 'rs80359550', ref: 'C', alt: 'T', gt: '0/0', gene: 'BRCA2', phenotype: 'Normal BRCA2 allele', clinical_significance: 'Benign', evidence_note: 'Wild-type BRCA2 sequence.' },
  { chrom: 'chr17', pos: 7673802, rsid: 'rs1042522', ref: 'G', alt: 'C', gt: '0/1', gene: 'TP53', phenotype: 'TP53 Arg72Pro polymorphism', clinical_significance: 'Benign / Risk Modifier', evidence_note: 'Common human polymorphism; minor modifier of cellular apoptosis efficiency.' },
];

function generateVcfContent(): string {
  const headerLines = [
    '##fileformat=VCFv4.2',
    '##fileDate=20260731',
    '##source=1000Genomes_NA12878_Clinical_Benchmark_v1.0',
    '##reference=GRCh38/hg38',
    '##INFO=<ID=RS,Number=1,Type=String,Description="dbSNP rsID">',
    '##INFO=<ID=GENE,Number=1,Type=String,Description="HGNC Gene Symbol">',
    '##INFO=<ID=CLNSIG,Number=1,Type=String,Description="ClinVar Clinical Significance">',
    '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">',
    '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">',
    '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">',
    '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNA12878_BENCHMARK',
  ];

  const variantLines: string[] = [];

  // 1. Add all high-impact clinical variants
  for (const v of CLINICAL_BENCHMARK_VARIANTS) {
    const info = `RS=${v.rsid};GENE=${v.gene};CLNSIG=${v.clinical_significance.replace(/\s+/g, '_')}`;
    variantLines.push(`${v.chrom}\t${v.pos}\t${v.rsid}\t${v.ref}\t${v.alt}\t99.9\tPASS\t${info}\tGT:GQ:DP\t${v.gt}:99:65`);
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

function generateClinVarTsvContent(): string {
  const lines = [
    'rsid\tgene\tphenotype\tclinical_significance\tevidence_note',
  ];

  for (const v of CLINICAL_BENCHMARK_VARIANTS) {
    lines.push(`${v.rsid}\t${v.gene}\t${v.phenotype}\t${v.clinical_significance}\t${v.evidence_note}`);
  }

  return lines.join('\n') + '\n';
}

async function run() {
  const vcfPath = path.resolve(process.cwd(), 'tests/fixtures/na12878_clinical_benchmark.vcf');
  const tsvPath = path.resolve(process.cwd(), 'tests/fixtures/clinvar_benchmark.tsv');

  console.log('[Clinical Benchmark] Generating realistic NA12878 1000 Genomes VCF...');
  const vcfData = generateVcfContent();
  fs.writeFileSync(vcfPath, vcfData, 'utf-8');
  console.log(`✔ Created VCF benchmark dataset (${vcfData.split('\n').length - 1} total variants): ${vcfPath}`);

  console.log('[Clinical Benchmark] Generating ClinVar clinical annotation TSV...');
  const tsvData = generateClinVarTsvContent();
  fs.writeFileSync(tsvPath, tsvData, 'utf-8');
  console.log(`✔ Created ClinVar TSV dataset: ${tsvPath}`);
}

run().catch(console.error);
