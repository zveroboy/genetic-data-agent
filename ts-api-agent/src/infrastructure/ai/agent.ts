import {
  TargetNotResolvableError,
  type ReferenceVocabularyEntry,
} from '../database/clinvar-coordinate-resolver.ts';
import type {
  CoordinateCoverage,
  GenotypeProvenance,
  GenotypeRepository,
} from '../database/duckdb.ts';
import { TargetNotPresentError } from '../database/parquet-dataset-resolver.ts';
import {
  type GroundingFinding,
  appendGroundingWarning,
  checkAnswerGrounding,
} from './answer-grounding.ts';
import { type CallBudget, callBudgetFromEnv } from './call-budget.ts';
import { answerableSurface, routeQuestion } from './question-routing.ts';
import { composeRoutedAnswer } from './routed-answer.ts';
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
  /**
   * What the model's prose claimed that the tools did not support, when a model wrote the answer.
   * Empty or absent means nothing in the text contradicted the payload — not that the answer is
   * right. Also appended to `answer` in words, so a reader who never sees this field is warned.
   */
  groundingFindings?: readonly GroundingFinding[];
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
  /**
   * Overridable so a test can pin *what an answer does with a found paper* without a live
   * Qdrant and a live embedding model.
   *
   * Without this seam that behaviour is unobservable in a unit test: the literature tool is a
   * module singleton reaching real services, so under test it always fails into its `{ error }`
   * sentinel and every answer looks identical whether the citation is carried or dropped. It was
   * dropped, on the one branch that had nothing else to offer, and no test could see it.
   */
  searchLiterature?: (query: string) => Promise<any[]>;
  /**
   * Overridable so a test can exhaust the model budget without making 120 paid calls, and so a
   * caller that owns several agents can share one ceiling between them.
   */
  callBudget?: CallBudget;
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
 *
 * The listing is the featured genes and a *count* of everything else, not `answerableGenes`. That
 * function returned 13 symbols when the snapshot held 14 rows and returns 238 when it holds 13,853:
 * pasted into a sentence, that is a wall of symbols with the actual answer buried in it, and it
 * still understates the table by four orders of magnitude in rows. Two clauses keep both halves
 * honest — what a plain-language question reaches, and how much more is reachable by name.
 */
function couldNotRouteAnswer(
  vocabulary: readonly ReferenceVocabularyEntry[],
  detail: string,
): string {
  const { featured, otherVariantCount } = answerableSurface(vocabulary);
  return (
    `${detail} Nothing was read from your genome. ` +
    `This reference snapshot can answer about ${featured.join(', ')} from a plain-language ` +
    `question, and can place about ${approximateCount(otherVariantCount)} more variants if you ` +
    'name a gene symbol or an rsID.'
  );
}

/**
 * A count rounded to the precision it deserves, so a sentence does not claim more than it means.
 *
 * "13,839 more variants" reads as an audited figure; it is the row count of whichever ClinVar
 * release the snapshot was built from, and it changes with every release. "about 13,800" says the
 * same true thing without implying the last two digits matter.
 */
function approximateCount(count: number): string {
  const rounded = count < 100 ? count : Math.round(count / 100) * 100;
  return rounded.toLocaleString('en-US');
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
): Promise<{
  evidence: any[];
  provenance?: GenotypeProvenance;
  note?: string;
  coordinateCoverage?: CoordinateCoverage;
}> {
  try {
    const result = await tool.execute!({ targetId }, { toolCallId: 'agent-internal', messages: [] });
    // The coverage travels with the rows so the composed answer can say how much of the gene it
    // read. Dropping it here would put the answer back to describing 64 of BRCA2's 2,714
    // coordinates in a sentence that sounds like it describes BRCA2.
    return {
      evidence: [...result.variants],
      provenance: result.provenance,
      coordinateCoverage: result.coordinateCoverage,
    };
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
        // Carries the same caveat as the "scanned and found nothing" answer below, and for a
        // stronger reason. Here the position was not merely uncalled — the dataset declares no
        // object that could hold it, which for an X-linked target like G6PD means chrX is
        // absent from this callset entirely. Reporting that as a flat "contains no variant"
        // reads as a checked negative and is the one phrasing the data does not support.
        note:
          `No genotype for '${targetId}' in dataset '${err.datasetId}': nothing there could ` +
          `hold it — ${err.detail}. That is a gap in this dataset's coverage, not a finding ` +
          'about you: it is not a statement that you carry the reference allele.',
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
async function defaultSearchLiterature(query: string): Promise<any[]> {
  const result = await searchMedicalLiteratureTool.execute!(
    { query },
    { toolCallId: 'agent-internal', messages: [] },
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
    return answerLocally(question, options, genotypeTool);
  }

  // 2. Cerebras LLM API, under a spending ceiling.
  //
  // A question reserves its worst case before the first HTTP call and refunds what it did not
  // use, so the budget can never run out mid-question. When there is nothing left to reserve the
  // question is still answered — by the free path above, which reads the same dataset through the
  // same tool and carries the same provenance. The only thing lost is the model's prose.
  const budget = options.callBudget ?? sharedCallBudget();
  if (!budget.reserve(MAX_MODEL_TURNS)) {
    console.warn(
      `[agent] Cerebras call budget exhausted (${budget.remaining()} calls left); ` +
        'answering from the deterministic path',
    );
    return answerLocally(question, options, genotypeTool);
  }

  let turnsUsed = MAX_MODEL_TURNS;
  try {
    const result = await askThroughCerebras(
      question,
      genotypeTool,
      options.referenceVocabulary,
      (turns) => {
        turnsUsed = turns;
      },
    );
    return result;
  } finally {
    budget.refund(MAX_MODEL_TURNS - turnsUsed);
  }
}

/**
 * The deterministic, unpaid answer path.
 *
 * Reached three ways, all of which must produce the same fully evidenced answer: an explicit dry
 * run, no provider key at all, and a spent model budget. Extracted from the middle of
 * `askBioinformaticsAgent` when the third caller appeared — the alternative was a copy, and a
 * second copy of the routing rules is what this module already replaced once.
 */
async function answerLocally(
  question: string,
  options: AskBioinformaticsAgentOptions,
  genotypeTool: ReturnType<typeof createQueryGenotypeTool>,
): Promise<AgentResponse> {
  {
    const vocabulary = options.referenceVocabulary;
    // Derived from the reference snapshot, never from a keyword list kept in this file. The
    // routing decision happens *before* anything is read: it selects gene symbols or rsIDs, each
    // of which then goes through the ordinary resolve path on its own, so it can never widen a
    // scan or reach a coordinate the reference does not place. Several targets only ever come from
    // a question naming several; an inferred target is one or none.
    const routing = routeQuestion(question, vocabulary);

    // The literature search is the question's own words, so it is worth running whether or not
    // a gene was identified — it is the one thing an unroutable question can still be given.
    const literatureHits = await (options.searchLiterature ?? defaultSearchLiterature)(question);
    // Labelled "related reading" and stamped with its similarity, deliberately. The search runs
    // over the question's words against a small corpus, so a hit is a nearest neighbour, not
    // evidence for the genotype above it — and `LITERATURE_MIN_SCORE` documents why no score
    // this model produces can be read as a relevance verdict. Showing the number lets a reader
    // apply the judgement the retrieval cannot.
    const literatureSuffix =
      literatureHits.length > 0
        ? `\n\n📚 Related reading (PubMed RAG, similarity ${Number(literatureHits[0].score).toFixed(2)}): ` +
          `"${literatureHits[0].title}" (PMID: ${literatureHits[0].pmid})`
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

    // Every routed target, each through the same single-target query path, reported in one answer
    // with one merged provenance record. The composition lives in `routed-answer.ts` because
    // reporting N outcomes honestly — a sentence each, a de-duplicated file union, a truncation
    // notice when a question names more targets than one question may read — is more than this
    // function, which also owns routing and literature, can carry without hiding one of them.
    const { answer: routedAnswer, evidence, provenance } = await composeRoutedAnswer(
      routing.targetIds,
      (targetId) => queryGenotype(genotypeTool, targetId),
    );

    // Appended on every outcome, not only on the one that found a genotype. The search ran and
    // its cost was paid regardless; dropping the result when there is no call withheld it from
    // exactly the questions that had nothing else to offer, while the response body carried it
    // all along — so the API and the answer disagreed about what had been found.
    const answer = routedAnswer + literatureSuffix;

    return {
      answer,
      evidence,
      ...(provenance === undefined ? {} : { provenance }),
      literatureHits,
      toolsUsed: ['query_genotype', ...literatureTool],
    };
  }
}

/**
 * The process-wide budget, created on first use.
 *
 * Process-wide because the quota is: two concurrent questions spend the same money, so a
 * per-request budget would bound nothing at all. Lazy because `callBudgetFromEnv` throws on a
 * malformed value, and a module that throws at import time takes the whole API down over a
 * setting the deterministic path does not even use.
 */
let processCallBudget: CallBudget | undefined;
function sharedCallBudget(): CallBudget {
  processCallBudget ??= callBudgetFromEnv();
  return processCallBudget;
}

/** Cerebras models this account can reach change; the one that has to work by default is named
 * in one place. Overridable with `CEREBRAS_MODEL`. */
export const DEFAULT_CEREBRAS_MODEL = 'gemma-4-31b';

/**
 * How many model turns one question may take.
 *
 * Each turn is one HTTP round trip plus whatever tools it calls, so this is the cost ceiling for
 * a single question. Four is enough for the deepest sensible plan here — query a gene, search the
 * literature, query a second gene, answer — while a model that keeps calling tools forever stops
 * costing money at a known bound.
 */
const MAX_MODEL_TURNS = 4;

/** The two tools, in the wire shape the chat completions API expects. */
const CEREBRAS_TOOLS = [
  {
    type: 'function',
    function: {
      name: 'query_genotype',
      description:
        'Queries the selected published genomic dataset for specific genes or rsIDs. Returns clinical evidence plus the provenance of what was read.',
      parameters: {
        type: 'object',
        properties: {
          targetId: {
            type: 'string',
            description:
              'The gene symbol (e.g. CYP1A2, VKORC1, SLCO1B1) or rsID (e.g. rs762551) to query.',
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
      description:
        'Performs semantic vector search in Qdrant/PubMed for medical literature related to symptoms or drug responses.',
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
];

/** Stamped onto any answer produced without the dataset having been read. */
export const UNGROUNDED_ANSWER_WARNING =
  '\n\n⚠️ Nothing was read from your genome for this answer: the model did not consult the ' +
  'dataset, so nothing above is a statement about your variants.';

/**
 * One question, answered by the model, with a bounded tool loop around it.
 *
 * Four properties this loop exists to hold, each of which was violated by the single-shot
 * exchange it replaces — every one observed against a live model, not hypothesised:
 *
 * - **Every tool call runs, over as many turns as the model needs.** Taking `tool_calls[0]` and
 *   stopping meant a question like "am I lactose intolerant?" that opened with a literature
 *   search never reached the genome at all, and the answer came back with no genotype in it.
 * - **The first turn must use a tool** (`tool_choice: 'required'`). Asked how to adjust a warfarin
 *   dose, the model called nothing and wrote a general clinical essay about INR monitoring — a
 *   confident answer to a question about *this* person, with their genome unopened beside it.
 * - **"Nothing found" reaches the model as a sentence, not as `[]`.** An empty array is read as
 *   "this person has no such variant"; the note says whether the position was uncalled or the
 *   chromosome was never in the dataset. That distinction is the whole point of `describeAbsence`,
 *   and it used to be discarded on the way to the model.
 * - **An ungrounded answer is labelled as one.** `tool_choice: 'required'` is a request, not a
 *   guarantee, so the backstop stays: if the genome was never read, the answer says so.
 *
 * - **The prose is checked against the tool results before it is returned.** Grounding the tools
 *   does not make the prose true: the same live run produced an answer citing `CYP3A5 rs776746
 *   C/C` — a gene absent from the reference, an rsID absent from the dataset, invented wholesale
 *   while the correct SLCO1B1 row sat in the response payload beside it. `answer-grounding.ts`
 *   compares the two and the answer carries what it found; nothing is rewritten or suppressed.
 */
async function askThroughCerebras(
  question: string,
  genotypeTool: ReturnType<typeof createQueryGenotypeTool>,
  /**
   * The reference snapshot's askable surface, used only to judge the model's prose — a gene symbol
   * this table does not list is a symbol nothing in this system could have supplied. Routing does
   * not happen on this path; the model chooses its own targets.
   */
  referenceVocabulary: readonly ReferenceVocabularyEntry[],
  /** Reports HTTP calls actually made, so the caller can refund the rest of its reservation. */
  reportTurns: (turns: number) => void,
): Promise<AgentResponse> {
  const modelName = process.env.CEREBRAS_MODEL || DEFAULT_CEREBRAS_MODEL;
  const messages: any[] = [
    { role: 'system', content: SYSTEM_PROMPT },
    { role: 'user', content: question },
  ];

  const toolCallsMade: any[] = [];
  const toolResults: { tool: string; result: any }[] = [];
  const toolsUsed = new Set<string>();
  /**
   * Every variant returned across *all* `query_genotype` calls, keyed by rsID.
   *
   * Keeping only the last non-empty result dropped all but one gene from `variants` whenever the
   * model asked about several — which it does, one call per gene — while its prose went on
   * discussing every one of them. The response then looked like a one-gene answer with a
   * multi-gene narrative on top, and the grounding check below would have read the genes it could
   * no longer see as fabrications. First writer wins per rsID: the same rsID queried twice is the
   * same row from the same immutable dataset.
   */
  const evidenceByRsid = new Map<string, any>();
  const accumulatedEvidence = (): any[] => [...evidenceByRsid.values()];
  let provenance: GenotypeProvenance | undefined;
  let literatureHits: any[] = [];

  for (let turn = 0; turn < MAX_MODEL_TURNS; turn++) {
    const body: Record<string, unknown> = {
      model: modelName,
      messages,
      tools: CEREBRAS_TOOLS,
      // Required on the opening turn only: the question is about a specific person's dataset, so
      // answering it without looking is never right. Afterwards the model has tool output in hand
      // and must be free to stop — `required` on every turn is an infinite loop.
      tool_choice: turn === 0 ? 'required' : 'auto',
    };

    // Counted before the response is inspected: a call that failed still cost a call.
    reportTurns(turn + 1);
    const res = await fetch('https://api.cerebras.ai/v1/chat/completions', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${process.env.CEREBRAS_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      throw new Error(`Cerebras API Error (${res.status}): ${await res.text()}`);
    }

    const data: any = await res.json();
    const choice = data.choices?.[0];
    const toolCalls = choice?.message?.tool_calls ?? [];

    if (toolCalls.length === 0) {
      const answer = choice?.message?.content || 'No response generated.';
      const evidence = accumulatedEvidence();
      // Checked against everything the tools returned this question, not just the last call: the
      // model is free to discuss every gene it queried, and only what no tool produced is flagged.
      // The two warnings are independent and can both apply — "the genome was never read" and
      // "the prose names a variant nobody read" are different faults.
      const groundingFindings = checkAnswerGrounding(answer, {
        variants: evidence,
        referenceVocabulary,
        toolResultText: toolResults.map((entry) => JSON.stringify(entry.result)),
      });
      const labelled = appendGroundingWarning(
        toolsUsed.has('query_genotype') ? answer : answer + UNGROUNDED_ANSWER_WARNING,
        groundingFindings,
      );
      return {
        answer: labelled,
        toolCalls: toolCallsMade,
        toolResults,
        evidence,
        ...(provenance === undefined ? {} : { provenance }),
        literatureHits,
        toolsUsed: [...toolsUsed],
        ...(groundingFindings.length === 0 ? {} : { groundingFindings }),
      };
    }

    messages.push(choice.message);

    for (const call of toolCalls) {
      const fnName = call.function?.name;
      let args: Record<string, unknown>;
      try {
        args = JSON.parse(call.function?.arguments || '{}');
      } catch (err: any) {
        // A model that emits unparseable arguments is a real fault, not a default to silently
        // paper over with a hard-coded target.
        throw new Error(
          `the model returned unparseable arguments for '${fnName}': ${err?.message ?? String(err)}`,
        );
      }

      let output: any;
      if (fnName === 'search_medical_literature') {
        const query =
          typeof args.query === 'string' && args.query.length > 0 ? args.query : question;
        // The tool's result reaches the model verbatim, including its `{ error }` sentinel on an
        // embedding/Qdrant failure — unlike the deterministic path, it is the model, not this
        // code, deciding how to phrase an answer around a degraded search.
        output = await searchMedicalLiteratureTool.execute!(
          { query },
          { toolCallId: call.id, messages: [] },
        );
        if (Array.isArray(output) && !(output[0] && 'error' in output[0])) {
          literatureHits = output;
        }
      } else if (fnName === 'query_genotype') {
        if (typeof args.targetId !== 'string' || args.targetId.length === 0) {
          throw new Error("the model called 'query_genotype' without a targetId");
        }
        const queried = await queryGenotype(genotypeTool, args.targetId);
        // The note travels with the empty result. Handing back a bare `[]` invites exactly the
        // sentence the note exists to prevent — "you have no G6PD variants" for a chromosome this
        // dataset never contained.
        output = {
          variants: queried.evidence,
          ...(queried.note === undefined ? {} : { note: queried.note }),
        };
        for (const variant of queried.evidence) {
          if (!evidenceByRsid.has(variant.rsid)) evidenceByRsid.set(variant.rsid, variant);
        }
        if (queried.provenance !== undefined) provenance = queried.provenance;
      } else {
        // Not a tool this agent offers. Reported to the model as a tool result rather than
        // thrown: it is the model's mistake to correct on the next turn, not a request failure.
        output = { error: `unknown tool '${fnName}'` };
      }

      toolsUsed.add(fnName);
      toolCallsMade.push(call);
      toolResults.push({ tool: fnName, result: output });
      messages.push({
        role: 'tool',
        tool_call_id: call.id,
        name: fnName,
        content: JSON.stringify(output),
      });
    }
  }

  // The model kept calling tools until the turn budget ran out. Everything the tools found is
  // still returned — it is real, it was read — but no prose is invented to wrap it. No grounding
  // check either: this sentence is built from the tool results themselves, so there is no model
  // prose here to disagree with them.
  const exhaustedEvidence = accumulatedEvidence();
  return {
    answer:
      `I stopped after ${MAX_MODEL_TURNS} model turns without a final answer. ` +
      (exhaustedEvidence.length > 0
        ? `What was read from your dataset: ${exhaustedEvidence
            .map((v) => `${v.userGenotype} for ${v.rsid} in ${v.gene}`)
            .join('; ')}.`
        : 'Nothing was read from your genome.'),
    toolCalls: toolCallsMade,
    toolResults,
    evidence: exhaustedEvidence,
    ...(provenance === undefined ? {} : { provenance }),
    literatureHits,
    toolsUsed: [...toolsUsed],
  };
}
