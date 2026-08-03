/**
 * Agent tools.
 *
 * The genotype tool is **constructed per request** around a `GenotypeRepository` that has
 * already been opened for one published dataset. There is deliberately no module-level
 * genotype tool and no imported repository singleton: a process-wide tool would let any
 * question reach any user's data, and the dataset a question is allowed to touch is a property
 * of the request, not of the process.
 *
 * The literature tool is the opposite case and stays global on purpose. Qdrant holds only
 * published literature — global, identical for everyone, no genotypes — so there is nothing
 * per-user to scope it to.
 */
import { tool } from 'ai';
import { z } from 'zod';

import type { GenotypeRepository } from '../database/duckdb.ts';
import { generateOllamaEmbedding } from '../vector/embeddings.ts';
import { qdrantRepository } from '../vector/qdrant.ts';

export const queryGenotypeParameters = z.object({
  targetId: z
    .string()
    .min(1)
    .describe('The gene symbol (e.g. CYP1A2, SLCO1B1) or rsID (e.g. rs762551) to query.'),
});

/**
 * Builds the genotype tool for one opened dataset.
 *
 * The result carries provenance — dataset content checksum, reference snapshot version and the
 * exact object URIs scanned — alongside the variants, so a model's answer can be traced back
 * to the bytes it came from.
 */
export function createQueryGenotypeTool(repository: GenotypeRepository) {
  return tool({
    description: 'Queries the selected published genomic dataset.',
    parameters: queryGenotypeParameters,
    execute: ({ targetId }) => repository.synthesizeVariant(targetId),
  });
}

/** How many papers one search may return. */
export const LITERATURE_TOP_K = 2;

/**
 * Cosine similarity below which a paper is not offered at all.
 *
 * Re-measured against the current corpus — 362 real PubMed abstracts over the thirteen genes in
 * `FEATURED_TARGETS` — with `nomic-embed-text`, top hit per question, `topK` 20 and no threshold.
 * The table this replaces was calibrated against eight documents whose "abstracts" were assembled
 * from a metadata template (`"… Clinical focus on <gene> genetic variation, drug response, and
 * metabolic pathways."`), so most of every vector was the same boilerplate as every other vector
 * and every score described that template rather than any science. `npm run ingest-pubmed` prints
 * this table on every run, so it can be re-measured instead of inherited.
 *
 * | question → top hit                                              | score |
 * |-----------------------------------------------------------------|-------|
 * | CYP1A2 genotype + caffeine → CYP1A2 genotype and caffeine       | 0.875 |
 * | "am I lactose intolerant? LCT genotype?" → LCT indel and lactose | 0.838 |
 * | statin muscle pain → the genetics of statin-induced myopathy    | 0.780 |
 * | G6PD variants → variants causing G6PD deficiency (WHO)          | 0.774 |
 * | warfarin dose → warfarin dosing algorithms                      | 0.746 |
 * | poor clopidogrel metabolizer → pharmacogenomics of clopidogrel  | 0.735 |
 * | "can I digest lactose?" → from lactose intolerance to nutrition | 0.728 |
 * | fast or slow caffeine metabolizer → CYP1A2 genotype, caffeine   | 0.708 |
 * | ——— the floor ———                                               | 0.65  |
 * | celiac / HLA-DQA1 → host genetics of gut microbiota             | 0.686 |
 * | type 2 diabetes risk → APOE and type 2 diabetes meta-analysis   | 0.684 |
 * | "should I worry about my cholesterol?" → MTHFR atherothrombosis | 0.616 |
 * | blood type → variants causing G6PD deficiency                   | 0.595 |
 * | peanut allergy → genetic factors in malaria                     | 0.518 |
 * | leaking kitchen tap → fava beans                                | 0.483 |
 * | world cup 1998 → G6PD A-variant frequency in Haiti              | 0.467 |
 * | pizza dough recipe → pharmacogenomics of glinides               | 0.446 |
 * | capital of France → G6PD A-variant frequency in Haiti           | 0.426 |
 * | reset my wifi router → MTHFR and diabetic neuropathy            | 0.393 |
 *
 * Every question the reference snapshot can answer now retrieves a paper about the *right gene* as
 * its first hit, which is what changed: warfarin reaches a warfarin dosing paper instead of the
 * nearest boilerplate, and clopidogrel no longer reaches veterinary pharmacology. Worst such
 * signal 0.708, highest score reached by a question with no genetics content at all 0.483 — a
 * margin of 0.225, where the old corpus had sixteen thousandths. The floor sits 0.167 above that
 * noise and 0.058 below that signal, in the middle of the widest gap the measurements leave.
 *
 * The caveat that remains, because it is still true: two questions this corpus cannot answer clear
 * the floor anyway — type 2 diabetes (0.684) and celiac disease (0.686). Both are genetics
 * questions about targets the snapshot does not carry (HLA-DQA1 is in no ClinVar record, so it is
 * not a reference target and no paper was fetched for it), and their top hits are genuinely about
 * genetics, just not about the question. So this is still a floor and not a relevance test: it
 * separates "about genetics" from "not about genetics", which the wide margin above now does
 * reliably, and it cannot separate "about this question" from "about some other gene". The answer
 * prints the score beside the citation and calls it related reading for that reason.
 */
export const LITERATURE_MIN_SCORE = 0.65;

export const searchMedicalLiteratureToolDefinition = {
  description:
    'Performs semantic vector search in Qdrant/PubMed for medical scientific papers related to symptoms or drug responses.',
  parameters: z.object({
    query: z
      .string()
      .min(1)
      .describe('The symptoms or medical topic to search for in scientific publications.'),
  }),
  execute: async ({ query }: { query: string }) => {
    try {
      const vector = await generateOllamaEmbedding(query, 'nomic-embed-text');
      return await qdrantRepository.searchVector(vector, LITERATURE_TOP_K, LITERATURE_MIN_SCORE);
    } catch (err: any) {
      // Literature is supporting evidence, not the answer: a search outage is reported to the
      // model as a tool-level error rather than failing the whole request. Nothing about user
      // genotypes flows through here.
      return [{ error: `Vector search error: ${err.message}` }];
    }
  },
};

export const searchMedicalLiteratureTool = tool(searchMedicalLiteratureToolDefinition);
