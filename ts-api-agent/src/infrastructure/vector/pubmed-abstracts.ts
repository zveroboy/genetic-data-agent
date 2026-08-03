/**
 * Reads PubMed's `efetch` XML, and builds the query list the literature corpus is fetched with.
 *
 * Both halves exist because the corpus used to be fabricated. `scripts/ingest_pubmed.ts` called
 * `esummary` — title, journal, year, and no abstract anywhere in the response — and then wrote
 * the sentence `"<title>. Published in <journal> (<year>). Clinical focus on <gene> genetic
 * variation, drug response, and metabolic pathways."` into the field the retriever embeds. Every
 * document therefore shared most of its text with every other document, and the eight vectors sat
 * in a narrow cone: "coffee" and "clopidogrel" scored within a few hundredths of each other
 * because the words being compared were the boilerplate, not the science. Real abstracts only
 * come from `efetch`, and `efetch` only speaks XML, so parsing it is not optional.
 *
 * The parsing lives here, apart from the script, for one reason: it is the only part with edge
 * cases worth pinning down (structured abstracts, inline markup, escaped text, articles with no
 * abstract at all), and a pure function over a string is the only part that can be tested without
 * the network. Deliberately no XML library — the shapes below are the ones PubMed actually emits,
 * and a dependency would be a large surface for a job this narrow.
 */
import type { ReferenceTarget } from '../database/clinvar-source-records.ts';

/** One article, as PubMed reports it. `abstract` is always PubMed's own prose. */
export interface PubMedArticle {
  readonly pmid: string;
  readonly title: string;
  readonly abstract: string;
  readonly year: string;
}

/**
 * What a parse produced: the usable articles, and the PMIDs deliberately left out.
 *
 * Skipped PMIDs are returned rather than logged from in here, so the caller can report how much
 * of a fetch was dropped. An article with no abstract is *not* backfilled with a placeholder:
 * a sentence assembled from metadata is exactly what this module was written to remove, and one
 * fabricated document in a corpus is one document that can outrank a real one.
 */
export interface PubMedParseResult {
  readonly articles: readonly PubMedArticle[];
  readonly skippedPmids: readonly string[];
}

/** The five entities XML defines; everything else PubMed escapes numerically. */
const NAMED_ENTITIES: Readonly<Record<string, string>> = Object.freeze({
  amp: '&',
  lt: '<',
  gt: '>',
  quot: '"',
  apos: "'",
});

/**
 * Turns one element's inner XML into plain text.
 *
 * Order matters and is the reverse of what looks natural. Markup is stripped *first*, then
 * entities are decoded: PubMed writes real inline elements bare (`<i>P</i> &lt; 0.05` — italic
 * markup around a variable, and an escaped less-than sign in the same sentence), so decoding
 * first would turn `&lt;i&gt;` into a tag and the next step would delete the text it wrapped.
 */
export function xmlTextContent(innerXml: string): string {
  const withoutMarkup = innerXml.replace(/<[^>]*>/g, '');
  const decoded = withoutMarkup.replace(
    /&(#[0-9]+|#[xX][0-9A-Fa-f]+|[A-Za-z][A-Za-z0-9]*);/g,
    (whole, entity: string) => {
      if (entity.startsWith('#x') || entity.startsWith('#X')) {
        return String.fromCodePoint(Number.parseInt(entity.slice(2), 16));
      }
      if (entity.startsWith('#')) {
        return String.fromCodePoint(Number.parseInt(entity.slice(1), 10));
      }
      // An unknown named entity is left verbatim rather than dropped: losing it would silently
      // delete a character from the text that gets embedded.
      return NAMED_ENTITIES[entity] ?? whole;
    },
  );
  return decoded.replace(/\s+/g, ' ').trim();
}

/** NLM's marker for "this section carries no heading"; it is not a heading. */
const UNLABELLED = 'UNLABELLED';

/** `CONCLUSION AND RELEVANCE` shouts in an abstract; `Conclusion and relevance` reads. */
function readableLabel(label: string): string {
  const trimmed = xmlTextContent(label);
  if (trimmed.length === 0) return '';
  return trimmed.charAt(0).toUpperCase() + trimmed.slice(1).toLowerCase();
}

/**
 * The abstract of one `<PubmedArticle>`, or the empty string when it has none.
 *
 * Structured abstracts arrive as several `<AbstractText Label="METHODS" …>` sections and are
 * joined with their headings kept: dropping the labels runs `To demonstrate that…` straight into
 * `Forty-eight patients were enrolled…`, and keeping them costs one word per section.
 *
 * Only the primary `<Abstract>` is read. `<OtherAbstract>` holds publisher translations — the
 * same content in French or Spanish — and embedding it would place a document's own translation
 * next to it as a second, differently-worded near-duplicate.
 */
export function extractAbstract(articleXml: string): string {
  const abstract = /<Abstract>([\s\S]*?)<\/Abstract>/.exec(articleXml);
  if (abstract === null) return '';
  const sections: string[] = [];
  for (const section of abstract[1]!.matchAll(/<AbstractText\b([^>]*)>([\s\S]*?)<\/AbstractText>/g)) {
    const text = xmlTextContent(section[2]!);
    if (text.length === 0) continue;
    const label = /\bLabel="([^"]*)"/.exec(section[1]!);
    const heading = label === null ? '' : readableLabel(label[1]!);
    sections.push(heading.length === 0 || heading.toUpperCase() === UNLABELLED ? text : `${heading}: ${text}`);
  }
  return sections.join(' ');
}

/** First `<PubDate>`'s four-digit year — `<Year>`, or the leading year of a `<MedlineDate>`. */
function extractYear(articleXml: string): string {
  const pubDate = /<PubDate>([\s\S]*?)<\/PubDate>/.exec(articleXml);
  const scope = pubDate === null ? articleXml : pubDate[1]!;
  const year = /<Year>(\d{4})<\/Year>/.exec(scope) ?? /<MedlineDate>[^<]*?(\d{4})/.exec(scope);
  return year === null ? '' : year[1]!;
}

/**
 * Every article in an `efetch&retmode=xml` response that carries a real abstract.
 *
 * Both `<PubmedArticle>` and `<PubmedBookArticle>` are read — a PubMed search returns book
 * chapters and GeneReviews entries among the journal articles, and those are abstracted too.
 * An article missing a PMID, a title or an abstract is reported as skipped instead of being
 * repaired: every field here is copied from the response or the document does not exist.
 */
export function parsePubMedArticles(xml: string): PubMedParseResult {
  const articles: PubMedArticle[] = [];
  const skippedPmids: string[] = [];
  for (const match of xml.matchAll(
    /<Pubmed(?:Book)?Article>([\s\S]*?)<\/Pubmed(?:Book)?Article>/g,
  )) {
    const articleXml = match[1]!;
    // The first PMID in the block is the article's own; later ones belong to its reference list.
    const pmid = /<PMID\b[^>]*>(\d+)<\/PMID>/.exec(articleXml)?.[1] ?? '';
    if (pmid.length === 0) continue;
    const titleXml =
      /<ArticleTitle\b[^>]*>([\s\S]*?)<\/ArticleTitle>/.exec(articleXml) ??
      /<BookTitle\b[^>]*>([\s\S]*?)<\/BookTitle>/.exec(articleXml);
    const title = titleXml === null ? '' : xmlTextContent(titleXml[1]!);
    const abstract = extractAbstract(articleXml);
    if (title.length === 0 || abstract.length === 0) {
      skippedPmids.push(pmid);
      continue;
    }
    articles.push({ pmid, title, abstract, year: extractYear(articleXml) });
  }
  return { articles, skippedPmids };
}

/** One PubMed search: the gene the hits are filed under, and the term that finds them. */
export interface PubMedCorpusQuery {
  readonly gene: string;
  readonly term: string;
}

/** Words that separate a paper about a gene from a paper that merely names it in a table. */
const GENETICS_VOCABULARY = ['polymorphism', 'variant', 'genotype', 'pharmacogenomics'];

/** A multi-word phrase has to reach PubMed quoted, or it is silently ANDed word by word. */
function asPubMedTerm(phrase: string): string {
  return phrase.includes(' ') ? `"${phrase}"` : phrase;
}

/**
 * The corpus query list, derived from the **featured** targets rather than typed out here.
 *
 * The four hardcoded genes this replaced (CYP1A2, LCT, SLCO1B1, VKORC1) covered under a third of
 * what the demo answers about, so a question about G6PD or clopidogrel had no paper to find and
 * retrieval returned the nearest paper about something else. Reading `FEATURED_TARGETS` instead
 * means adding a featured variant extends the corpus with no edit in the ingestion script.
 *
 * **Featured, not placeable.** The coordinate table is the ~14,000 variants the system can place;
 * this list is the 13 genes it can answer about from a symptom. Only the second belongs here. Pass
 * the coordinate table's genes instead and this function returns hundreds of queries, the
 * ingestion script issues hundreds of unthrottled `esearch`/`efetch` round-trips against a public
 * NCBI endpoint that rate-limits at three requests a second, and the corpus fills with papers for
 * genes no question can reach — drowning the ones that can in a vector store where every hit
 * competes with every other. Two queries per featured gene is a corpus somebody can reason about.
 *
 * Two queries per gene, because the two vocabularies find different papers. The genetics query
 * finds what the gene is known for; the lay query reuses the patient words already declared on
 * the target (`warfarin`, `statin`, `lactose`, `coffee`) and finds the clinical papers a patient's
 * own phrasing has to match at retrieval time. Genes with no lay terms get the first query only.
 * Targets sharing a gene (APOE has two rsIDs) are collapsed, so the list is one entry per query,
 * per gene.
 */
export function pubmedCorpusQueries(
  targets: readonly ReferenceTarget[],
): readonly PubMedCorpusQuery[] {
  const layTermsByGene = new Map<string, string[]>();
  for (const target of targets) {
    const bucket = layTermsByGene.get(target.gene) ?? [];
    for (const term of target.layTerms ?? []) if (!bucket.includes(term)) bucket.push(term);
    layTermsByGene.set(target.gene, bucket);
  }

  const queries: PubMedCorpusQuery[] = [];
  for (const [gene, layTerms] of layTermsByGene) {
    queries.push({
      gene,
      term: `${gene}[Title/Abstract] AND (${GENETICS_VOCABULARY.join(' OR ')})`,
    });
    if (layTerms.length > 0) {
      queries.push({ gene, term: `${gene} AND (${layTerms.map(asPubMedTerm).join(' OR ')})` });
    }
  }
  return queries;
}
