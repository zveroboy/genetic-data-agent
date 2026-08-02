import {
  TargetNotResolvableError,
  type ReferenceVocabularyEntry,
} from '../database/clinvar-coordinate-resolver.ts';
import type { GenotypeProvenance, GenotypeRepository } from '../database/duckdb.ts';
import { TargetNotPresentError } from '../database/parquet-dataset-resolver.ts';
import { answerableGenes, routeQuestion } from './question-routing.ts';
import { createQueryGenotypeTool, searchMedicalLiteratureTool } from './tools.ts';

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
  /**
   * The askable surface of the opened reference snapshot, read from the snapshot itself.
   *
   * Required, not optional. The deterministic path has to decide which target a question means,
   * and the only honest source for that is the table it will then query; defaulting to an empty
   * vocabulary would make every question unanswerable, and defaulting to a built-in list is the
   * hardcoded router this replaced.
   */
  referenceVocabulary: readonly ReferenceVocabularyEntry[];
  dryRunLocal?: boolean;
}

/**
 * What to say when the question does not name a target, without naming one anyway.
 *
 * The old code defaulted to CYP1A2, so "what should I have for lunch?" came back as a confident
 * statement about caffeine metabolism. It named the gene it had queried, so it was not a
 * fabrication — but a user reads the genotype, not the gene symbol, and there is no reading of
 * that exchange in which the system answered the question it was asked. Saying so, and listing
 * what *can* be answered, costs one sentence.
 */
function couldNotRouteAnswer(
  vocabulary: readonly ReferenceVocabularyEntry[],
  detail: string,
): string {
  const genes = answerableGenes(vocabulary);
  return (
    `${detail} Nothing was read from your genome. ` +
    `This reference snapshot can answer about ${genes.join(', ')} — name a gene or an rsID, or ` +
    'ask about a drug or condition one of them covers.'
  );
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

/**
 * Runs the literature tool and reports whether it actually found anything.
 *
 * Routed through `searchMedicalLiteratureTool` (`tools.ts`) — the same tool `tools.test.ts`
 * exercises — rather than re-inlining the `generateOllamaEmbedding` + `qdrantRepository`
 * pair here, which used to be duplicated once per call site with no production caller ever
 * running the tested tool at all.
 *
 * The tool already turns a Qdrant/embedding failure into an `{ error }` sentinel instead of
 * throwing, so a failed search and an empty search both come back as non-empty-shaped results;
 * this unwraps that sentinel so callers only ever see real hits, and logs the failure rather
 * than silently discarding it.
 */
async function searchLiterature(query: string, toolCallId: string): Promise<any[]> {
  const result = await searchMedicalLiteratureTool.execute!(
    { query },
    { toolCallId, messages: [] },
  );
  if (Array.isArray(result) && result.length > 0 && result[0] && 'error' in result[0]) {
    console.warn(`[agent] literature search unavailable: ${result[0].error}`);
    return [];
  }
  return Array.isArray(result) ? result : [];
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

  // 1. Dry run / local offline mode for instant E2E verification.
  //
  // Gated on `CEREBRAS_API_KEY` alone — the only provider actually implemented below.
  // `ANTHROPIC_API_KEY` is deliberately NOT treated as a provider signal: there is no Anthropic
  // branch, so if it counted here, setting it (an entirely plausible ambient env var) without
  // also setting `CEREBRAS_API_KEY` would fall through every branch below to the
  // 'No AI provider configured.' response — a 200 with no evidence and no provenance, and the
  // published dataset never read. Falling back to the deterministic local path instead means an
  // unrelated ambient variable degrades to a fully evidenced answer, not a silent non-answer.
  if (options.dryRunLocal || !process.env.CEREBRAS_API_KEY) {
    const vocabulary = options.referenceVocabulary;
    // Derived from the reference snapshot, never from a keyword list kept in this file. The
    // routing decision happens *before* anything is read: it selects one gene symbol or rsID,
    // which then goes through the ordinary resolve path, so it can never widen a scan or reach
    // a coordinate the reference does not place.
    const routing = routeQuestion(question, vocabulary);

    // The literature search is the question's own words, so it is worth running whether or not
    // a gene was identified — it is the one thing an unroutable question can still be given.
    const literatureHits = await searchLiterature(question, 'agent-internal');
    const literatureSuffix =
      literatureHits.length > 0
        ? `\n\n📚 Medical Literature (PubMed RAG): "${literatureHits[0].title}" (PMID: ${literatureHits[0].pmid})`
        : '';
    const literatureTool = literatureHits.length > 0 ? ['search_medical_literature'] : [];

    if (routing.kind !== 'resolved') {
      const detail =
        routing.kind === 'ambiguous'
          ? `Your question could be about ${routing.candidates.join(' or ')} — ` +
            `'${routing.matchedTerms.join("', '")}' fits more than one of them, so I did not guess.`
          : 'I could not tell which gene your question is about.';
      return {
        answer: couldNotRouteAnswer(vocabulary, detail) + literatureSuffix,
        evidence: [],
        literatureHits,
        toolsUsed: [...literatureTool],
      };
    }

    const { evidence, provenance, note } = await queryGenotype(genotypeTool, routing.targetId);

    // "No rows came back" is not "you carry the reference allele", and the difference matters:
    // NA12878 has no call at CYP2D6 rs3892097 because that region is outside GIAB's
    // high-confidence set — the position was never assessed. Telling the two apart properly
    // (absent from the callset vs. called with different alleles) is an open problem; saying
    // which one this is *not* costs nothing and keeps the answer honest meanwhile.
    let answer =
      note ??
      `No genotype for '${routing.targetId}' in this dataset: the reference places it, but the ` +
        'dataset reports no matching call at those coordinates. That is not a statement that ' +
        'you carry the reference allele — a position can be missing because it was never assessed.';
    if (evidence.length > 0) {
      const v = evidence[0];
      answer = `Based on your genotype (${v.userGenotype} for rsID ${v.rsid} in gene ${v.gene}), clinical significance is ${v.clinicalSignificance} (${v.phenotype}). Note: ${v.evidenceNote}`;
      answer += literatureSuffix;
    }

    return {
      answer,
      evidence,
      ...(provenance === undefined ? {} : { provenance }),
      literatureHits,
      toolsUsed: ['query_genotype', ...literatureTool],
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
        // The model gets the tool's own result verbatim, including its `{ error }` sentinel on
        // an embedding/Qdrant failure — unlike the dry-run path above, it is the model (not this
        // code) deciding how to phrase an answer around a degraded tool result.
        toolOutput = await searchMedicalLiteratureTool.execute!(
          { query: queryStr },
          { toolCallId: call.id, messages: [] },
        );
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
