import { generateOllamaEmbedding } from '../vector/embeddings.ts';
import { qdrantRepository } from '../vector/qdrant.ts';
import { TargetNotResolvableError } from '../database/clinvar-coordinate-resolver.ts';
import type { GenotypeProvenance, GenotypeRepository } from '../database/duckdb.ts';
import { TargetNotPresentError } from '../database/parquet-dataset-resolver.ts';
import { createQueryGenotypeTool } from './tools.ts';

export const SYSTEM_PROMPT = `You are an expert bioinformatics AI assistant.
Your primary directive is accuracy. You must NOT invent or hallucinate genetic variants.
You have access to two tools:
1. \`query_genotype\`: Queries the selected published genomic dataset for specific genes or rsIDs.
2. \`search_medical_literature\`: Performs semantic vector search in Qdrant/PubMed for medical literature related to symptoms or drug responses.

Use these tools to formulate clear, scientifically accurate answers.`;

export interface AgentResponse {
  answer: string;
  toolCalls?: any[];
  toolResults?: any[];
  evidence?: any[];
  /** What the genotype tool actually read: dataset checksum, reference version, files. */
  provenance?: GenotypeProvenance;
  literatureHits?: any[];
  toolsUsed?: string[];
}

export interface AskBioinformaticsAgentOptions {
  /**
   * The dataset this question may read. Required: the agent has no ambient access to user
   * data, and a repository is only obtainable for a dataset with a published manifest.
   */
  genotypeRepository: GenotypeRepository;
  dryRunLocal?: boolean;
}

/**
 * Queries one target, turning the two "there is nothing to read" outcomes into an answer
 * rather than a crash.
 *
 * Both are ordinary, expected results — a target the reference cannot place, and a target the
 * dataset provably does not contain — and both are named explicitly. Any other failure
 * propagates: an S3 outage or a corrupted manifest must not be reported as "no variant found".
 *
 * Routed through the same `createQueryGenotypeTool` factory `tools.test.ts` exercises, rather
 * than calling `repository.synthesizeVariant` directly: the factory is Step 9's mandated way
 * tools get their repository, and calling it here keeps the tested artifact and the running
 * path the same code, instead of two implementations that can drift apart.
 */
async function queryGenotype(
  tool: ReturnType<typeof createQueryGenotypeTool>,
  targetId: string,
): Promise<{ evidence: any[]; provenance?: GenotypeProvenance; note?: string }> {
  try {
    const result = await tool.execute!({ targetId }, { toolCallId: 'agent-internal', messages: [] });
    return { evidence: [...result.variants], provenance: result.provenance };
  } catch (err) {
    if (err instanceof TargetNotResolvableError) {
      return {
        evidence: [],
        note: `'${targetId}' is not present in reference snapshot '${err.referenceVersion}'.`,
      };
    }
    if (err instanceof TargetNotPresentError) {
      return {
        evidence: [],
        note: `Dataset '${err.datasetId}' contains no variant at the coordinates for '${targetId}'.`,
      };
    }
    throw err;
  }
}

export async function askBioinformaticsAgent(
  question: string,
  options: AskBioinformaticsAgentOptions
): Promise<AgentResponse> {
  const repository = options.genotypeRepository;
  // Built once per request, around the dataset-scoped repository the caller opened. Every path
  // below reaches user genotypes only through this tool's `execute`, never through the
  // repository directly.
  const genotypeTool = createQueryGenotypeTool(repository);

  // 1. Dry run / local offline mode for instant E2E verification
  if (options.dryRunLocal || (!process.env.CEREBRAS_API_KEY && !process.env.ANTHROPIC_API_KEY)) {
    let targetId = 'rs762551';
    const q = question.toLowerCase();

    if (q.includes('coffee') || q.includes('caffeine')) {
      targetId = 'CYP1A2';
    } else if (q.includes('lactose') || q.includes('milk') || q.includes('dairy')) {
      targetId = 'LCT';
    } else if (q.includes('statin') || q.includes('cholesterol') || q.includes('muscle')) {
      targetId = 'SLCO1B1';
    } else if (q.includes('warfarin') || q.includes('blood thinner') || q.includes('vkorc1') || q.includes('cyp2c9')) {
      targetId = 'VKORC1';
    } else if (q.includes('ssri') || q.includes('antidepressant')) {
      targetId = 'CYP2D6';
    }

    const { evidence, provenance, note } = await queryGenotype(genotypeTool, targetId);
    let literatureHits: any[] = [];
    try {
      const qVector = await generateOllamaEmbedding(question, 'nomic-embed-text');
      literatureHits = await qdrantRepository.searchVector(qVector, 2);
    } catch (err: any) {
      // Literature is optional context; the genotype answer stands without it. The reason is
      // logged rather than discarded so an outage is visible.
      console.warn(`[agent] literature search unavailable: ${err?.message ?? String(err)}`);
    }

    let answer = note ?? 'No clinical variant data found.';
    if (evidence.length > 0) {
      const v = evidence[0];
      answer = `Based on your genotype (${v.userGenotype} for rsID ${v.rsid} in gene ${v.gene}), clinical significance is ${v.clinicalSignificance} (${v.phenotype}). Note: ${v.evidenceNote}`;
      if (literatureHits.length > 0) {
        answer += `\n\n📚 Medical Literature (PubMed RAG): "${literatureHits[0].title}" (PMID: ${literatureHits[0].pmid})`;
      }
    }

    return {
      answer,
      evidence,
      ...(provenance === undefined ? {} : { provenance }),
      literatureHits,
      toolsUsed: ['query_genotype', ...(literatureHits.length > 0 ? ['search_medical_literature'] : [])],
    };
  }

  // 2. Cerebras LLM API (Fast Inference with Llama-3.3-70B)
  if (process.env.CEREBRAS_API_KEY) {
    const modelName = process.env.CEREBRAS_MODEL || 'llama-3.3-70b';
    const res = await fetch('https://api.cerebras.ai/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${process.env.CEREBRAS_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: modelName,
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          { role: 'user', content: question },
        ],
        tools: [
          {
            type: 'function',
            function: {
              name: 'query_genotype',
              description: 'Queries the selected published genomic dataset for specific genes or rsIDs. Returns clinical evidence plus the provenance of what was read.',
              parameters: {
                type: 'object',
                properties: {
                  targetId: {
                    type: 'string',
                    description: 'The gene symbol (e.g. CYP1A2, VKORC1, SLCO1B1) or rsID (e.g. rs762551) to query.',
                  },
                },
                required: ['targetId'],
              },
            },
          },
          {
            type: 'function',
            function: {
              name: 'search_medical_literature',
              description: 'Performs semantic vector search in Qdrant/PubMed for medical literature related to symptoms or drug responses.',
              parameters: {
                type: 'object',
                properties: {
                  query: {
                    type: 'string',
                    description: 'The medical topic or symptoms to search for in PubMed/Qdrant.',
                  },
                },
                required: ['query'],
              },
            },
          },
        ],
        tool_choice: 'auto',
      }),
    });

    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`Cerebras API Error (${res.status}): ${errText}`);
    }

    const data: any = await res.json();
    const choice = data.choices?.[0];
    const toolCalls = choice?.message?.tool_calls;

    if (toolCalls && toolCalls.length > 0) {
      const call = toolCalls[0];
      const fnName = call.function.name;
      let toolOutput: any = [];
      let provenance: GenotypeProvenance | undefined;

      const parseArguments = (): Record<string, unknown> => {
        try {
          return JSON.parse(call.function.arguments);
        } catch (err: any) {
          // A model that emits unparseable arguments is a real fault, not a default to
          // silently paper over with a hard-coded target.
          throw new Error(
            `the model returned unparseable arguments for '${fnName}': ${err?.message ?? String(err)}`,
          );
        }
      };

      if (fnName === 'search_medical_literature') {
        const args = parseArguments();
        const queryStr = typeof args.query === 'string' && args.query.length > 0 ? args.query : question;
        try {
          const vector = await generateOllamaEmbedding(queryStr, 'nomic-embed-text');
          toolOutput = await qdrantRepository.searchVector(vector, 2);
        } catch (err: any) {
          toolOutput = [{ error: err.message }];
        }
      } else {
        const args = parseArguments();
        if (typeof args.targetId !== 'string' || args.targetId.length === 0) {
          throw new Error("the model called 'query_genotype' without a targetId");
        }
        const queried = await queryGenotype(genotypeTool, args.targetId);
        toolOutput = queried.evidence;
        provenance = queried.provenance;
      }

      // Follow-up request with tool output
      const followUpRes = await fetch('https://api.cerebras.ai/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${process.env.CEREBRAS_API_KEY}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model: modelName,
          messages: [
            { role: 'system', content: SYSTEM_PROMPT },
            { role: 'user', content: question },
            choice.message,
            {
              role: 'tool',
              tool_call_id: call.id,
              name: fnName,
              content: JSON.stringify(toolOutput),
            },
          ],
        }),
      });

      const followUpData: any = await followUpRes.json();
      return {
        answer: followUpData.choices?.[0]?.message?.content || 'No response generated.',
        toolCalls,
        toolResults: [{ tool: fnName, result: toolOutput }],
        evidence: fnName === 'query_genotype' ? toolOutput : undefined,
        ...(provenance === undefined ? {} : { provenance }),
        literatureHits: fnName === 'search_medical_literature' ? toolOutput : undefined,
        toolsUsed: [fnName],
      };
    }

    return {
      answer: choice?.message?.content || 'No response generated.',
    };
  }

  return { answer: 'No AI provider configured.' };
}
