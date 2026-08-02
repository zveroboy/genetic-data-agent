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
      return await qdrantRepository.searchVector(vector, 2);
    } catch (err: any) {
      // Literature is supporting evidence, not the answer: a search outage is reported to the
      // model as a tool-level error rather than failing the whole request. Nothing about user
      // genotypes flows through here.
      return [{ error: `Vector search error: ${err.message}` }];
    }
  },
};

export const searchMedicalLiteratureTool = tool(searchMedicalLiteratureToolDefinition);
