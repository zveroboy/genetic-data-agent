/**
 * The literature corpus is only as honest as this parser.
 *
 * The defect this closes: the ingestion script never called `efetch`, so no abstract was ever
 * read. It called `esummary`, which returns metadata only, and wrote a template sentence —
 * `"<title>. Published in <journal> (<year>). Clinical focus on <gene> genetic variation, drug
 * response, and metabolic pathways."` — into the field the retriever embeds. Eight documents, each
 * mostly identical text, all clustered within a few hundredths of cosine similarity of each other.
 *
 * So the assertions below are about the two failure modes that would quietly bring that back: an
 * abstract that parses to *less* than PubMed published (a structured abstract reduced to its last
 * section, markup deleting the text it wraps, `&lt;` swallowing the rest of a sentence), and an
 * article with no abstract surviving as a document anyway. The fixtures are cut down from real
 * `efetch&retmode=xml` responses and keep the element shapes verbatim, including the inline
 * `<i>P</i> &lt; 0.05` that decoding in the wrong order destroys.
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { ReferenceTarget } from '../database/clinvar-source-records.ts';
import {
  extractAbstract,
  parsePubMedArticles,
  pubmedCorpusQueries,
  xmlTextContent,
} from './pubmed-abstracts.ts';

/** PMID 42426980, trimmed: a structured abstract, inline italics and an escaped `<`. */
const STRUCTURED_ARTICLE = `<PubmedArticle><MedlineCitation Status="Publisher" Owner="NLM"><PMID Version="1">42426980</PMID><Article PubModel="Print-Electronic"><Journal><JournalIssue CitedMedium="Internet"><PubDate><Year>2026</Year><Month>Jul</Month></PubDate></JournalIssue><Title>The Annals of pharmacotherapy</Title></Journal><ArticleTitle>Optimizing Time in Therapeutic Range in Patients With HeartMate 3: Pharmacist-Led Anticoagulation Using Pharmacogenetics of Warfarin.</ArticleTitle><Abstract><AbstractText Label="BACKGROUND" NlmCategory="BACKGROUND">Suboptimal time in therapeutic range (TTR) during warfarin anticoagulation is frequently observed in patients supported with a left ventricular assist device.</AbstractText><AbstractText Label="METHODS" NlmCategory="METHODS">Pharmacogenetic screening for CYP2C9*2, CYP2C9*3, and VKORC1 were performed in the intervention group.</AbstractText><AbstractText Label="RESULTS" NlmCategory="RESULTS">The TTR was 78.1% for IG and 67.1% for CG (<i>P</i> &lt; 0.05).</AbstractText><AbstractText Label="CONCLUSION AND RELEVANCE" NlmCategory="CONCLUSIONS">Pharmacist-led warfarin management resulted in significantly higher TTR.</AbstractText></Abstract></Article></MedlineCitation></PubmedArticle>`;

/** An unlabelled abstract, a `MedlineDate` range, and a publisher translation to ignore. */
const PLAIN_ARTICLE = `<PubmedArticle><MedlineCitation Status="MEDLINE" Owner="NLM"><PMID Version="1">11788828</PMID><Article PubModel="Print"><Journal><JournalIssue CitedMedium="Print"><PubDate><MedlineDate>2002 Feb-Mar</MedlineDate></PubDate></JournalIssue><Title>Nature genetics</Title></Journal><ArticleTitle>Identification of a variant associated with adult-type hypolactasia.</ArticleTitle><Abstract><AbstractText Label="UNLABELLED" NlmCategory="UNASSIGNED">Lactase persistence is associated with a DNA variant 13910 bp upstream of the lactase gene <i>LCT</i>.</AbstractText></Abstract><OtherAbstract Language="fre" Type="Publisher"><AbstractText>La persistance de la lactase est associée à un variant.</AbstractText></OtherAbstract></Article></MedlineCitation></PubmedArticle>`;

/** PMID 3023816's shape: an older record PubMed carries with no `<Abstract>` element at all. */
const ABSTRACTLESS_ARTICLE = `<PubmedArticle><MedlineCitation Status="MEDLINE" Owner="NLM"><PMID Version="1">3023816</PMID><Article PubModel="Print"><Journal><JournalIssue CitedMedium="Print"><PubDate><Year>1986</Year><Month>Jan</Month></PubDate></JournalIssue><Title>Molecular and cellular biology</Title></Journal><ArticleTitle>Drosophila forked locus.</ArticleTitle></Article></MedlineCitation></PubmedArticle>`;

const RESPONSE = `<?xml version="1.0" ?><!DOCTYPE PubmedArticleSet PUBLIC "-//NLM//DTD PubMedArticle, 1st January 2024//EN" "https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_240101.dtd"><PubmedArticleSet>${STRUCTURED_ARTICLE}${ABSTRACTLESS_ARTICLE}${PLAIN_ARTICLE}</PubmedArticleSet>`;

describe('PubMed abstract extraction', () => {
  it('keeps every section of a structured abstract, with its heading', () => {
    const abstract = extractAbstract(STRUCTURED_ARTICLE);
    // All four sections, in publication order — a single-section parse would drop the methods
    // and results, which is most of what makes the document findable.
    assert.match(abstract, /^Background: Suboptimal time in therapeutic range/);
    assert.ok(abstract.includes('Methods: Pharmacogenetic screening for CYP2C9*2'));
    assert.ok(abstract.includes('Results: The TTR was 78.1%'));
    assert.ok(abstract.includes('Conclusion and relevance: Pharmacist-led warfarin management'));
  });

  it('strips inline markup but keeps escaped text', () => {
    // `<i>P</i> &lt; 0.05`: the italics go, the comparison stays. Decoding entities before
    // removing tags would turn `&lt;` into `<` and delete ` 0.05).` with it.
    assert.ok(extractAbstract(STRUCTURED_ARTICLE).includes('(P < 0.05).'));
    assert.equal(xmlTextContent('the <i>LCT</i> gene'), 'the LCT gene');
    assert.equal(xmlTextContent('5&#8242; &#x3B2;-globin &amp; friends'), "5′ β-globin & friends");
    // An entity PubMed's DTD defines but this parser does not know must survive as written,
    // rather than vanishing from the text that gets embedded.
    assert.equal(xmlTextContent('caffeine &notanentity; clearance'), 'caffeine &notanentity; clearance');
  });

  it('omits the NLM "UNLABELLED" marker and any translated abstract', () => {
    const abstract = extractAbstract(PLAIN_ARTICLE);
    assert.ok(abstract.startsWith('Lactase persistence is associated'), abstract);
    assert.ok(!abstract.includes('Unlabelled'));
    // The French publisher translation is the same paper in other words; embedding it would put a
    // near-duplicate of the document next to the document.
    assert.ok(!abstract.includes('La persistance'));
  });

  it('reports an article with no abstract as skipped instead of inventing one', () => {
    const { articles, skippedPmids } = parsePubMedArticles(RESPONSE);
    assert.deepEqual(skippedPmids, ['3023816']);
    assert.deepEqual(
      articles.map((article) => article.pmid),
      ['42426980', '11788828'],
    );
    // The template the old script wrote must not be reachable from any input.
    for (const article of articles) {
      assert.ok(!/Clinical focus on/.test(article.abstract));
      assert.ok(!/Published in/.test(article.abstract));
    }
  });

  it('reads the PMID, title and year each article declares', () => {
    const { articles } = parsePubMedArticles(RESPONSE);
    assert.deepEqual(articles[0], {
      pmid: '42426980',
      title:
        'Optimizing Time in Therapeutic Range in Patients With HeartMate 3: Pharmacist-Led Anticoagulation Using Pharmacogenetics of Warfarin.',
      abstract: extractAbstract(STRUCTURED_ARTICLE),
      year: '2026',
    });
    // A `MedlineDate` range ("2002 Feb-Mar") has no `<Year>`; its leading year is the year.
    assert.equal(articles[1]!.year, '2002');
  });

  it('parses an empty or abstract-free response without throwing', () => {
    assert.deepEqual(parsePubMedArticles('<PubmedArticleSet></PubmedArticleSet>'), {
      articles: [],
      skippedPmids: [],
    });
    assert.equal(extractAbstract('<PubmedArticle><Abstract></Abstract></PubmedArticle>'), '');
  });
});

describe('PubMed corpus queries', () => {
  const TARGETS: readonly ReferenceTarget[] = [
    { rsid: 'rs9923231', gene: 'VKORC1', layTerms: ['warfarin', 'blood thinner'] },
    { rsid: 'rs429358', gene: 'APOE' },
    { rsid: 'rs7412', gene: 'APOE' },
  ];

  it('asks one genetics query per gene and one lay query where lay terms exist', () => {
    assert.deepEqual(pubmedCorpusQueries(TARGETS), [
      {
        gene: 'VKORC1',
        term: 'VKORC1[Title/Abstract] AND (polymorphism OR variant OR genotype OR pharmacogenomics)',
      },
      // Multi-word lay phrases are quoted; unquoted, PubMed ANDs "blood" with "thinner" and the
      // OR group stops meaning what it says.
      { gene: 'VKORC1', term: 'VKORC1 AND (warfarin OR "blood thinner")' },
      {
        gene: 'APOE',
        term: 'APOE[Title/Abstract] AND (polymorphism OR variant OR genotype OR pharmacogenomics)',
      },
    ]);
  });

  it('covers every featured gene, and only those', async () => {
    // The corpus used to be four hardcoded genes against thirteen featured ones, so a question
    // about G6PD or clopidogrel had no paper to find and retrieval returned the nearest paper
    // about something else. This is the assertion that adding a featured target extends the corpus.
    const { FEATURED_TARGETS } = await import('../database/clinvar-source-records.ts');
    const genes = new Set(pubmedCorpusQueries(FEATURED_TARGETS).map((query) => query.gene));
    assert.deepEqual(genes, new Set(FEATURED_TARGETS.map((target) => target.gene)));
    assert.equal(genes.size, 13);
  });

  it('is derived from the featured list, never from the coordinate table', async () => {
    // The corpus list and the coordinate universe are different things, and this is the test that
    // says so. The table is ~14,000 rows over ~240 genes; feeding those here would mean hundreds
    // of unthrottled round-trips against NCBI and a corpus of papers for genes no plain-language
    // question can reach, competing with the ones that can. The bound is the featured list.
    const { FEATURED_TARGETS } = await import('../database/clinvar-source-records.ts');
    const queries = pubmedCorpusQueries(FEATURED_TARGETS);
    assert.ok(
      queries.length <= 2 * FEATURED_TARGETS.length,
      `${queries.length} queries for ${FEATURED_TARGETS.length} featured targets`,
    );
    assert.ok(queries.length < 40, 'the corpus query list must stay hand-sized');
  });
});
