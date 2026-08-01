import { z } from 'zod';
import { tool } from 'ai';
import { duckDbRepository } from '../database/duckdb.ts';
import { qdrantRepository } from '../vector/qdrant.ts';
import { generateOllamaEmbedding } from '../vector/embeddings.ts';

export const queryGenotypeToolDefinition = {
  description: 'Queries the user genomic DuckDB database for specific genes or rsIDs.',
  parameters: z.object({
    targetId: z.string().describe('The gene symbol (e.g. CYP1A2, SLCO1B1) or rsID (e.g. rs762551) to query.'),
  }),
  execute: async ({ targetId }: { targetId: string }) => {
    return await duckDbRepository.synthesizeVariant(targetId);
  },
};

export const queryGenotypeTool = tool(queryGenotypeToolDefinition);

export const searchMedicalLiteratureToolDefinition = {
  description: 'Performs semantic vector search in Qdrant/PubMed for medical scientific papers related to symptoms or drug responses.',
  parameters: z.object({
    query: z.string().describe('The symptoms or medical topic to search for in scientific publications.'),
  }),
  execute: async ({ query }: { query: string }) => {
    try {
      const vector = await generateOllamaEmbedding(query, 'nomic-embed-text');
      return await qdrantRepository.searchVector(vector, 2);
    } catch (err: any) {
      return [{ error: `Vector search error: ${err.message}` }];
    }
  },
};

export const searchMedicalLiteratureTool = tool(searchMedicalLiteratureToolDefinition);
