/**
 * One answered variant, as it reaches the agent and the wire.
 *
 * camelCase, deliberately: this is the `variants[]` payload `POST /ask` returns, and every wire
 * payload in this system is JSON-compatible primitives in camelCase (`contracts/ingestion-v1.md`).
 * The snake_case spellings live one layer down, where they belong — they are the physical Parquet
 * and reference-snapshot column names, and `infrastructure/database/duckdb.ts` is the single
 * place that translates between the two.
 */
export interface SynthesizedVariant {
  rsid: string;
  gene: string;
  userGenotype: string;
  phenotype: string;
  clinicalSignificance: string;
  evidenceNote: string;
}
