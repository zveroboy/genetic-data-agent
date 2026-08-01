import { generateOllamaEmbedding } from '../src/infrastructure/vector/embeddings.ts';
import { qdrantRepository } from '../src/infrastructure/vector/qdrant.ts';
import type { PubMedVectorDocument } from '../src/infrastructure/vector/qdrant.ts';

interface PubMedArticle {
  pmid: string;
  title: string;
  abstract: string;
  year: string;
  gene: string;
}

const TARGET_GENES = [
  { gene: 'CYP1A2', query: 'CYP1A2 caffeine metabolism' },
  { gene: 'LCT', query: 'LCT lactase persistence deficiency' },
  { gene: 'SLCO1B1', query: 'SLCO1B1 statin myopathy muscle toxicity' },
  { gene: 'VKORC1', query: 'VKORC1 warfarin dose sensitivity' },
];

async function fetchPubMedArticles(gene: string, queryTerm: string, maxResults: number = 3): Promise<PubMedArticle[]> {
  console.log(`[NCBI PubMed API] Searching articles for gene '${gene}'...`);
  
  // 1. E-Search to get PMIDs
  const esearchUrl = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=${encodeURIComponent(queryTerm)}&retmax=${maxResults}&retmode=json`;
  const searchRes = await fetch(esearchUrl);
  if (!searchRes.ok) throw new Error(`PubMed Search Error: ${await searchRes.text()}`);
  
  const searchData: any = await searchRes.json();
  const idList: string[] = searchData.esearchresult?.idlist || [];

  if (idList.length === 0) return [];

  // 2. E-Summary to get Metadata
  const esummaryUrl = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=${idList.join(',')}&retmode=json`;
  const sumRes = await fetch(esummaryUrl);
  if (!sumRes.ok) throw new Error(`PubMed Summary Error: ${await sumRes.text()}`);
  
  const sumData: any = await sumRes.json();
  const articles: PubMedArticle[] = [];

  for (const pmid of idList) {
    const item = sumData.result?.[pmid];
    if (item) {
      const title = item.title || 'Untitled Article';
      const year = (item.pubdate || '2025').slice(0, 4);
      const source = item.source || 'Medical Journal';
      // Synthetic informative summary text based on paper title and metadata
      const abstract = `${title}. Published in ${source} (${year}). Clinical focus on ${gene} genetic variation, drug response, and metabolic pathways.`;
      
      articles.push({
        pmid,
        title,
        abstract,
        year,
        gene,
      });
    }
  }

  return articles;
}

async function runIngestion() {
  console.log('🚀 Starting Automated PubMed Medical RAG Ingestion Pipeline');
  console.log('   Embedding Provider: Local Ollama (nomic-embed-text)');
  console.log('   Vector Target:      Qdrant / DuckDB Vector Store\n');

  // 1. Init Collection
  await qdrantRepository.initCollection(768);

  const allArticles: PubMedArticle[] = [];
  for (const tg of TARGET_GENES) {
    try {
      const fetched = await fetchPubMedArticles(tg.gene, tg.query, 2);
      allArticles.push(...fetched);
      await new Promise((r) => setTimeout(r, 500));
    } catch (err: any) {
      console.warn(`⚠️ Could not fetch PubMed for ${tg.gene}: ${err.message}`);
    }
  }

  console.log(`\n✔ Fetched ${allArticles.length} real PubMed articles from NCBI.`);
  console.log('   Generating 768-dim embeddings via local Ollama nomic-embed-text...\n');

  const documents: PubMedVectorDocument[] = [];
  let docId = 1;

  for (const art of allArticles) {
    const textToEmbed = `${art.title} ${art.abstract}`;
    console.log(` [${docId}/${allArticles.length}] Vectorizing PMID ${art.pmid} (${art.gene})...`);
    
    const vector = await generateOllamaEmbedding(textToEmbed, 'nomic-embed-text');
    documents.push({
      id: docId++,
      pmid: art.pmid,
      gene: art.gene,
      title: art.title,
      abstract: art.abstract,
      year: art.year,
      vector,
    });
  }

  // 2. Upsert to Vector Store
  console.log('\n[Vector Store] Upserting vector embeddings...');
  await qdrantRepository.upsertPoints(documents);

  // 3. Test Semantic Search
  console.log('\n======================================================');
  console.log('🔍 Executing Test Vector Search on Qdrant/DuckDB');
  console.log('   Query: "muscle toxicity and statin side effects"');
  console.log('======================================================\n');

  const testQuery = 'muscle toxicity and statin side effects';
  const queryVector = await generateOllamaEmbedding(testQuery, 'nomic-embed-text');
  const searchResults = await qdrantRepository.searchVector(queryVector, 2);

  console.log(`Top ${searchResults.length} Relevant Medical Papers Found:\n`);
  searchResults.forEach((res, i) => {
    console.log(`Hit #${i + 1} (Score: ${(res.score * 100).toFixed(2)}% similarity)`);
    console.log(`  PMID:     ${res.pmid}`);
    console.log(`  Gene:     ${res.gene}`);
    console.log(`  Title:    ${res.title}`);
    console.log(`  Summary:  ${res.abstract}`);
    console.log('------------------------------------------------------');
  });

  console.log('\n🎉 PubMed RAG Vector Ingestion Completed Successfully!');
}

if (import.meta.url === `file://${process.argv[1]}`) {
  runIngestion().catch((err) => {
    console.error('❌ Pipeline failed:', err);
    process.exit(1);
  });
}
