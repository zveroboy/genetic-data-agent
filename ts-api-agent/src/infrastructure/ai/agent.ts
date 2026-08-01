import { generateOllamaEmbedding } from '../vector/embeddings.ts';
import { qdrantRepository } from '../vector/qdrant.ts';
import { duckDbRepository } from '../database/duckdb.ts';

export const SYSTEM_PROMPT = `You are an expert bioinformatics AI assistant. 
Your primary directive is accuracy. You must NOT invent or hallucinate genetic variants.
You have access to two tools:
1. \`query_genotype\`: Queries the user genomic DuckDB database for specific genes or rsIDs.
2. \`search_medical_literature\`: Performs semantic vector search in Qdrant/PubMed for medical literature related to symptoms or drug responses.

Use these tools to formulate clear, scientifically accurate answers.`;

export interface AgentResponse {
  answer: string;
  toolCalls?: any[];
  toolResults?: any[];
  evidence?: any[];
  literatureHits?: any[];
  toolsUsed?: string[];
}

export async function askBioinformaticsAgent(
  question: string,
  options: { dryRunLocal?: boolean } = {}
): Promise<AgentResponse> {
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
    } else if (q.includes('warfarin') || q.includes('blood thinner')) {
      targetId = 'VKORC1';
    } else if (q.includes('ssri') || q.includes('antidepressant')) {
      targetId = 'CYP2D6';
    }

    const evidence = await duckDbRepository.synthesizeVariant(targetId);
    let literatureHits: any[] = [];
    try {
      const qVector = await generateOllamaEmbedding(question, 'nomic-embed-text');
      literatureHits = await qdrantRepository.searchVector(qVector, 2);
    } catch {}

    let answer = 'No clinical variant data found.';
    if (evidence.length > 0) {
      const v = evidence[0];
      answer = `Based on your genotype (${v.user_genotype} for rsID ${v.rsid} in gene ${v.gene}), clinical significance is ${v.clinical_significance} (${v.phenotype}). Note: ${v.evidence_note}`;
      if (literatureHits.length > 0) {
        answer += `\n\n📚 Medical Literature (PubMed RAG): "${literatureHits[0].title}" (PMID: ${literatureHits[0].pmid})`;
      }
    }

    return {
      answer,
      evidence,
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
              description: 'Queries the user genomic DuckDB database for specific genes or rsIDs. Returns top clinical ACMG evidence.',
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

      if (fnName === 'search_medical_literature') {
        let queryStr = question;
        try {
          const args = JSON.parse(call.function.arguments);
          queryStr = args.query || queryStr;
        } catch {}
        try {
          const vector = await generateOllamaEmbedding(queryStr, 'nomic-embed-text');
          toolOutput = await qdrantRepository.searchVector(vector, 2);
        } catch (err: any) {
          toolOutput = [{ error: err.message }];
        }
      } else {
        let targetId = 'rs762551';
        try {
          const args = JSON.parse(call.function.arguments);
          targetId = args.targetId || targetId;
        } catch {}
        toolOutput = await duckDbRepository.synthesizeVariant(targetId);
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
