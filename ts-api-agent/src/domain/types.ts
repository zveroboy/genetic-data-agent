export interface UserVariant {
  chrom: string;
  pos: number;
  rsid: string;
  ref: string;
  alt: string;
  gt_raw: string;
}

export interface ClinVarAnnotation {
  rsid: string;
  gene: string;
  phenotype: string;
  clinical_significance: string;
  evidence_note: string;
}

export interface SynthesizedVariant {
  rsid: string;
  gene: string;
  user_genotype: string;
  phenotype: string;
  clinical_significance: string;
  evidence_note: string;
}

export interface QueryGenotypeParams {
  targetId: string;
}
