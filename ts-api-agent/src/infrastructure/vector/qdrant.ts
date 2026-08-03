/**
 * Global literature store. Holds published PubMed abstracts only — never a user genotype — so
 * it is deliberately process-wide and shared, unlike the per-dataset genotype repository.
 *
 * The former DuckDB/JSON mirror of these vectors is gone: it wrote a `data/` file from the
 * serving path and shared a module with user data. Qdrant is now the only store; when it is
 * unreachable the search reports that, rather than answering from a stale local copy.
 */
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
      console.warn('[QdrantRepository] Qdrant server offline; literature search is unavailable.');
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
    if (!(await this.isQdrantAlive())) {
      throw new Error(
        `[QdrantRepository] cannot upsert ${documents.length} documents: Qdrant at ${this.qdrantUrl} is unreachable`,
      );
    }

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
      throw new Error(`[QdrantRepository] upsert failed: ${await res.text()}`);
    }
    console.log(`✔ Successfully upserted ${documents.length} points to Qdrant collection '${this.collectionName}'.`);
  }

  /**
   * Nearest `topK` points, optionally cut off at `minScore`.
   *
   * The cut is applied by Qdrant (`score_threshold`) rather than by the caller: a filter here
   * would still return `topK` rows and let the caller forget to drop them, and the collection
   * knows the metric it was built with.
   */
  async searchVector(queryVector: number[], topK: number = 3, minScore?: number): Promise<any[]> {
    if (!(await this.isQdrantAlive())) {
      throw new Error(`[QdrantRepository] Qdrant at ${this.qdrantUrl} is unreachable`);
    }

    const res = await fetch(`${this.qdrantUrl}/collections/${this.collectionName}/points/search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        vector: queryVector,
        limit: topK,
        with_payload: true,
        ...(minScore === undefined ? {} : { score_threshold: minScore }),
      }),
    });

    if (!res.ok) {
      throw new Error(
        `[QdrantRepository] literature search failed (${res.status}): ${await res.text()}`,
      );
    }

    const data: any = await res.json();
    return (data.result || []).map((hit: any) => ({ score: hit.score, ...hit.payload }));
  }
}

export const qdrantRepository = new QdrantRepository();
