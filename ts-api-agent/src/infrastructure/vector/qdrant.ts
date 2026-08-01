import { duckDbRepository } from '../database/duckdb.ts';

export interface PubMedVectorDocument {
  id: number;
  pmid: string;
  gene: string;
  title: string;
  abstract: string;
  year: string;
  vector: number[];
}

export class QdrantRepository {
  private qdrantUrl: string;
  private collectionName: string;

  constructor() {
    this.qdrantUrl = process.env.QDRANT_HOST || 'http://localhost:6333';
    this.collectionName = 'genomic_pubmed';
  }

  async isQdrantAlive(): Promise<boolean> {
    try {
      const res = await fetch(`${this.qdrantUrl}/healthz`, { method: 'GET' });
      return res.ok;
    } catch {
      return false;
    }
  }

  async initCollection(vectorSize: number = 768): Promise<void> {
    const isAlive = await this.isQdrantAlive();
    if (!isAlive) {
      console.log('[QdrantRepository] Qdrant server offline. Using DuckDB native vector store fallback.');
      return;
    }

    try {
      // Check if collection exists
      const checkRes = await fetch(`${this.qdrantUrl}/collections/${this.collectionName}`);
      if (checkRes.status === 404) {
        console.log(`[QdrantRepository] Creating collection '${this.collectionName}' (vector size: ${vectorSize})...`);
        const createRes = await fetch(`${this.qdrantUrl}/collections/${this.collectionName}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            vectors: {
              size: vectorSize,
              distance: 'Cosine',
            },
          }),
        });
        if (!createRes.ok) {
          console.warn(`[QdrantRepository] Collection creation warning: ${await createRes.text()}`);
        } else {
          console.log(`✔ Qdrant collection '${this.collectionName}' created successfully.`);
        }
      }
    } catch (err: any) {
      console.warn(`[QdrantRepository] Failed to init collection: ${err.message}`);
    }
  }

  async upsertPoints(documents: PubMedVectorDocument[]): Promise<void> {
    const isAlive = await this.isQdrantAlive();

    if (isAlive) {
      const points = documents.map((doc) => ({
        id: doc.id,
        vector: doc.vector,
        payload: {
          pmid: doc.pmid,
          gene: doc.gene,
          title: doc.title,
          abstract: doc.abstract,
          year: doc.year,
        },
      }));

      const res = await fetch(`${this.qdrantUrl}/collections/${this.collectionName}/points?wait=true`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ points }),
      });

      if (!res.ok) {
        console.warn(`[QdrantRepository] Upsert failed: ${await res.text()}`);
      } else {
        console.log(`✔ Successfully upserted ${documents.length} points to Qdrant collection '${this.collectionName}'.`);
      }
    }

    // Always keep DuckDB synchronized as a fallback vector store
    await duckDbRepository.initVectorTable(documents);
  }

  async searchVector(queryVector: number[], topK: number = 3): Promise<any[]> {
    const isAlive = await this.isQdrantAlive();

    if (isAlive) {
      try {
        const res = await fetch(`${this.qdrantUrl}/collections/${this.collectionName}/points/search`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            vector: queryVector,
            limit: topK,
            with_payload: true,
          }),
        });

        if (res.ok) {
          const data: any = await res.json();
          return (data.result || []).map((hit: any) => ({
            score: hit.score,
            ...hit.payload,
          }));
        }
      } catch (err: any) {
        console.warn(`[QdrantRepository] Qdrant search fallback: ${err.message}`);
      }
    }

    // DuckDB vector fallback search
    return duckDbRepository.searchVectorDuckDb(queryVector, topK);
  }
}

export const qdrantRepository = new QdrantRepository();
