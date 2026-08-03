/**
 * Builds the literature corpus in Qdrant from real PubMed abstracts.
 *
 * What this replaced: the script called `esearch` and then `esummary`, and `esummary` returns
 * metadata only — title, journal, year, never an abstract. So it *wrote* one, per document:
 * `"<title>. Published in <journal> (<year>). Clinical focus on <gene> genetic variation, drug
 * response, and metabolic pathways."` That template is what got embedded, which meant most of
 * every vector was the same boilerplate as every other vector, and the eight documents in the
 * collection sat inside a band a few hundredths of cosine similarity wide. Relevance was poor
 * because there was almost nothing gene-specific in the text being compared. Abstracts come from
 * `efetch` and nowhere else, so `efetch` is now the step that produces a document — and an
 * article PubMed has no abstract for is dropped rather than described.
 *
 * The corpus is also no longer four hardcoded genes. Queries are derived from
 * `FEATURED_TARGETS` — the variants the demo features, not the ~14,000-row coordinate table —
 * so the corpus covers what a plain-language question can reach and adding a featured variant
 * extends it with no edit here.
 *
 * Operator tool, not request-path code: it talks to NCBI, to Ollama and to Qdrant, and it is run
 * by hand (`npm run ingest-pubmed`). Point ids are assigned 1..N in fetch order, so a re-run
 * replaces the corpus in place. Drop the collection first
 * (`curl -X DELETE $QDRANT_HOST/collections/genomic_pubmed`) whenever the query set *shrinks* —
 * otherwise points beyond the new N survive from the previous run.
 */
import { FEATURED_TARGETS } from '../src/infrastructure/database/clinvar-source-records.ts';
import { generateOllamaEmbedding } from '../src/infrastructure/vector/embeddings.ts';
import {
  parsePubMedArticles,
  pubmedCorpusQueries,
  type PubMedArticle,
} from '../src/infrastructure/vector/pubmed-abstracts.ts';
import { qdrantRepository } from '../src/infrastructure/vector/qdrant.ts';
import type { PubMedVectorDocument } from '../src/infrastructure/vector/qdrant.ts';

const EUTILS = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils';

/**
 * Papers requested per query.
 *
 * Measured: 21 queries × 20 hits = 420 hits, 367 distinct PMIDs after de-duplication, 362
 * documents once the five PubMed publishes no abstract for are dropped.
 */
const HITS_PER_QUERY = 20;

/**
 * PMIDs per `efetch` call.
 *
 * Batched, not one request per PMID: 300 individual `efetch` calls at the anonymous rate limit
 * would take a minute and a half of wall clock and 300 chances to be throttled, for the same
 * bytes. 50 was measured rather than chosen — batches of 100 had NCBI close the connection
 * mid-response (`UND_ERR_SOCKET`, ~80 KB in) often enough to fail a run.
 */
const EFETCH_BATCH = 50;

/** A dropped connection is not a reason to lose 20 minutes of fetching; it is a reason to retry. */
const MAX_ATTEMPTS = 3;

/** Points per Qdrant upsert: 300 × 768 floats in one request is several megabytes of JSON. */
const UPSERT_BATCH = 100;

/**
 * An API key raises NCBI's ceiling from 3 requests/second to 10, across *all* E-utilities.
 *
 * Exceeding it is not a soft failure: NCBI returns HTTP 429 and, on repeated abuse, blocks the
 * source IP. So every request in this script goes through `eutils()`, which sleeps the remainder
 * of the interval before dispatching — 340 ms anonymous (just under 3/s, with headroom for clock
 * jitter), 110 ms with a key. A deliberate delay in a script nobody watches is cheaper than an
 * IP block that outlives the run.
 */
const NCBI_API_KEY = process.env.NCBI_API_KEY ?? '';
const REQUEST_INTERVAL_MS = NCBI_API_KEY.length > 0 ? 110 : 340;

let nextRequestAt = 0;

/**
 * One rate-limited E-utilities request, retried on a dropped connection.
 *
 * Serialised by construction — every caller awaits it, so the sleep below is the only pacing this
 * script needs. `POST` is used for `efetch`: NCBI's guidance is to POST long id lists, and a
 * 50-PMID query string is long enough to be worth not putting in a URL.
 */
async function eutils(
  endpoint: string,
  params: Record<string, string>,
  method: 'GET' | 'POST' = 'GET',
): Promise<Response> {
  const body = new URLSearchParams({ db: 'pubmed', ...params });
  if (NCBI_API_KEY.length > 0) body.set('api_key', NCBI_API_KEY);

  for (let attempt = 1; ; attempt += 1) {
    const wait = nextRequestAt - Date.now();
    if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));
    nextRequestAt = Date.now() + REQUEST_INTERVAL_MS;
    try {
      const res =
        method === 'GET'
          ? await fetch(`${EUTILS}/${endpoint}?${body.toString()}`)
          : await fetch(`${EUTILS}/${endpoint}`, { method: 'POST', body });
      if (!res.ok) {
        throw new Error(`NCBI ${endpoint} failed (${res.status}): ${(await res.text()).slice(0, 300)}`);
      }
      return res;
    } catch (err: any) {
      if (attempt >= MAX_ATTEMPTS) throw err;
      console.warn(`[ncbi] ${endpoint} attempt ${attempt} failed (${err.message}); retrying...`);
      nextRequestAt = Date.now() + REQUEST_INTERVAL_MS * attempt * 3;
    }
  }
}

/** PMIDs a query matches, most relevant first. */
async function searchPmids(term: string): Promise<string[]> {
  const res = await eutils('esearch.fcgi', {
    term,
    // Relevance rather than NCBI's default (most recent): a corpus of the newest 20 papers to
    // mention a gene is mostly papers that mention it in passing.
    sort: 'relevance',
    retmax: String(HITS_PER_QUERY),
    retmode: 'json',
  });
  const data: any = await res.json();
  return data.esearchresult?.idlist ?? [];
}

/** The articles PubMed publishes an abstract for, plus the PMIDs dropped for having none. */
async function fetchArticles(pmids: readonly string[]): Promise<{
  articles: PubMedArticle[];
  skipped: string[];
}> {
  const articles: PubMedArticle[] = [];
  const skipped: string[] = [];
  for (let start = 0; start < pmids.length; start += EFETCH_BATCH) {
    const batch = pmids.slice(start, start + EFETCH_BATCH);
    console.log(`[efetch] abstracts for ${batch.length} PMIDs (${start + batch.length}/${pmids.length})...`);
    const res = await eutils('efetch.fcgi', { id: batch.join(','), retmode: 'xml' }, 'POST');
    const parsed = parsePubMedArticles(await res.text());
    articles.push(...parsed.articles);
    skipped.push(...parsed.skippedPmids);
  }
  return { articles, skipped };
}

/**
 * Probe questions with a known right answer, and two with none.
 *
 * They are here rather than in a notebook because they are how `LITERATURE_MIN_SCORE` in
 * `src/infrastructure/ai/tools.ts` is calibrated: the threshold has to sit above the best score
 * any question with no answer can reach, and below the score every question with an answer does
 * reach. The comment on that constant carries a table this section prints, so re-running the
 * ingestion re-measures it instead of inheriting numbers from a corpus that no longer exists.
 */
const PROBES: readonly { question: string; expect: string | null }[] = [
  { question: 'How should my warfarin dose be adjusted?', expect: 'warfarin / VKORC1' },
  { question: 'Am I at risk of statin-induced muscle pain?', expect: 'statin myopathy / SLCO1B1' },
  { question: 'Can I digest lactose?', expect: 'lactase / LCT' },
  { question: 'Am I lactose intolerant? What is my LCT genotype?', expect: 'lactase / LCT' },
  { question: 'Am I a poor clopidogrel metabolizer?', expect: 'clopidogrel / CYP2C19' },
  { question: 'Do I have any G6PD variants?', expect: 'G6PD' },
  {
    question: 'What is my CYP1A2 genotype and how does it affect caffeine metabolism?',
    expect: 'caffeine / CYP1A2',
  },
  { question: 'What is the capital of France?', expect: null },
  { question: 'How do I fix a leaking kitchen tap?', expect: null },
];

/** Embeds each probe, records its top hit, and prints the margin the threshold has to fit in. */
async function calibrate(): Promise<void> {
  console.log('\n=== threshold calibration (top hit per probe, unfiltered) ===');
  let worstSignal = Number.POSITIVE_INFINITY;
  let bestNoise = 0;
  for (const probe of PROBES) {
    const vector = await generateOllamaEmbedding(probe.question, 'nomic-embed-text');
    // A high topK on purpose: the threshold is chosen from what retrieval *can* reach, not from
    // what a two-result production search happens to show.
    const [top] = await qdrantRepository.searchVector(vector, 20);
    const score = top === undefined ? 0 : top.score;
    if (probe.expect === null) bestNoise = Math.max(bestNoise, score);
    else worstSignal = Math.min(worstSignal, score);
    console.log(
      `${score.toFixed(3)}  ${probe.expect === null ? 'NOISE ' : 'signal'}  ${probe.question}`,
    );
    console.log(`         → ${top === undefined ? '(no hit)' : `${top.gene}: ${top.title}`}`);
  }
  console.log(
    `\nworst signal ${worstSignal.toFixed(3)}, best noise ${bestNoise.toFixed(3)} — margin ` +
      `${(worstSignal - bestNoise).toFixed(3)}`,
  );
}

/** Points the collection reports. The repository has no count method and this is an operator tool. */
async function reportPointCount(): Promise<void> {
  const host = process.env.QDRANT_HOST || 'http://localhost:6333';
  const res = await fetch(`${host}/collections/genomic_pubmed`);
  const data: any = await res.json();
  console.log(`[qdrant] collection 'genomic_pubmed' reports ${data.result?.points_count} points.`);
}

async function runIngestion(): Promise<void> {
  const queries = pubmedCorpusQueries(FEATURED_TARGETS);
  console.log(`🚀 PubMed literature ingestion: ${queries.length} queries over ${
    new Set(queries.map((query) => query.gene)).size
  } genes`);
  console.log(`   NCBI rate limit: ${REQUEST_INTERVAL_MS} ms between requests` +
    `${NCBI_API_KEY.length > 0 ? ' (API key present, 10/s)' : ' (no API key, 3/s)'}\n`);

  await qdrantRepository.initCollection(768);

  // De-duplicated across genes: the same paper answers for two genes often enough (warfarin
  // papers name both VKORC1 and CYP2C9), and storing it twice would let one paper occupy both
  // slots of a two-result search. First query to find a PMID owns it.
  const geneByPmid = new Map<string, string>();
  for (const query of queries) {
    const pmids = await searchPmids(query.term);
    let fresh = 0;
    for (const pmid of pmids) {
      if (geneByPmid.has(pmid)) continue;
      geneByPmid.set(pmid, query.gene);
      fresh += 1;
    }
    console.log(`[esearch] ${query.gene}: ${pmids.length} hits, ${fresh} new — ${query.term}`);
  }

  const { articles, skipped } = await fetchArticles([...geneByPmid.keys()]);
  console.log(
    `\n✔ ${articles.length} articles with a real abstract; ${skipped.length} PMIDs skipped for ` +
      'having none (PubMed publishes no abstract for them, and a synthesised one is the defect ' +
      'this script exists to have removed).',
  );

  console.log('\n[ollama] embedding abstracts with nomic-embed-text (768 dims)...');
  const documents: PubMedVectorDocument[] = [];
  const startedAt = Date.now();
  for (const article of articles) {
    // Sequential: 362 abstracts took 8.3 s against a local Ollama, which is not worth the
    // concurrency. The elapsed time is printed so that stays a measurement, not an assumption.
    const vector = await generateOllamaEmbedding(`${article.title}\n\n${article.abstract}`, 'nomic-embed-text');
    documents.push({
      id: documents.length + 1,
      pmid: article.pmid,
      gene: geneByPmid.get(article.pmid) ?? '',
      title: article.title,
      abstract: article.abstract,
      year: article.year,
      vector,
    });
  }
  console.log(`✔ embedded ${documents.length} documents in ${((Date.now() - startedAt) / 1000).toFixed(1)}s`);

  // Chunked, so a chunk that fails names how far the run got instead of losing the whole batch.
  for (let start = 0; start < documents.length; start += UPSERT_BATCH) {
    await qdrantRepository.upsertPoints(documents.slice(start, start + UPSERT_BATCH));
  }

  await reportPointCount();
  await calibrate();

  const sample = documents[0];
  if (sample !== undefined) {
    console.log(`\n[spot check] PMID ${sample.pmid} (${sample.gene}, ${sample.year})`);
    console.log(`  ${sample.abstract.slice(0, 400)}...`);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  runIngestion().catch((err) => {
    console.error('❌ Ingestion failed:', err);
    process.exit(1);
  });
}
