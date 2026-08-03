/**
 * Agent tool wiring tests.
 *
 * The genotype tool is the agent's only door to user data, so it must be constructed around a
 * repository that was already opened for one published dataset. A module-level tool bound to a
 * process-wide repository would let any question reach any dataset, which is precisely the
 * shape these tests forbid.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import type { GenotypeQueryResult, GenotypeRepository } from '../database/duckdb.ts';
import { createQueryGenotypeTool, searchMedicalLiteratureTool } from './tools.ts';

const RESULT: GenotypeQueryResult = {
  targetId: 'SLCO1B1',
  variants: [
    {
      rsid: 'rs4149056',
      gene: 'SLCO1B1',
      userGenotype: 'T/C',
      phenotype: 'Statins myopathy risk',
      clinicalSignificance: 'Risk Factor',
      evidenceNote: 'Intermediate OATP1B1 function.',
    },
  ],
  provenance: {
    datasetId: 'ds-serving-001',
    datasetChecksumSha256: 'a'.repeat(64),
    referenceBuild: 'GRCh38',
    referenceVersion: 'demo-clinvar-grch38-v3',
    filesScanned: ['s3://genomic-artifacts/datasets/ds-serving-001/…/chrom=12/part-000.parquet'],
    targetsResolved: 1,
  },
  coordinateCoverage: { listed: 1, read: 1 },
};

function fakeRepository(): GenotypeRepository & { readonly calls: string[] } {
  const calls: string[] = [];
  return {
    calls,
    datasetId: 'ds-serving-001',
    async synthesizeVariant(targetId: string): Promise<GenotypeQueryResult> {
      calls.push(targetId);
      return { ...RESULT, targetId };
    },
  };
}

describe('query_genotype tool', () => {
  it('queries the repository it was constructed with', async () => {
    const repository = fakeRepository();
    const tool = createQueryGenotypeTool(repository);

    const result = await tool.execute!({ targetId: 'SLCO1B1' }, {
      toolCallId: 'call-1',
      messages: [],
    });

    assert.deepEqual(repository.calls, ['SLCO1B1']);
    assert.deepEqual(result, RESULT);
  });

  it('hands the agent the provenance of what it read', async () => {
    const tool = createQueryGenotypeTool(fakeRepository());

    const result = (await tool.execute!({ targetId: 'SLCO1B1' }, {
      toolCallId: 'call-1',
      messages: [],
    })) as GenotypeQueryResult;

    assert.equal(result.provenance.datasetChecksumSha256, 'a'.repeat(64));
    assert.equal(result.provenance.referenceVersion, 'demo-clinvar-grch38-v3');
    assert.equal(result.provenance.filesScanned.length, 1);
    assert.doesNotThrow(() => JSON.stringify(result));
  });

  it('rejects an empty target id', () => {
    const tool = createQueryGenotypeTool(fakeRepository());

    assert.equal(tool.parameters.safeParse({ targetId: '' }).success, false);
    assert.equal(tool.parameters.safeParse({ targetId: 'CYP1A2' }).success, true);
  });

  it('gives two repositories two independent tools', async () => {
    const first = fakeRepository();
    const second = fakeRepository();

    await createQueryGenotypeTool(first).execute!({ targetId: 'CYP1A2' }, {
      toolCallId: 'a',
      messages: [],
    });

    assert.deepEqual(first.calls, ['CYP1A2']);
    assert.deepEqual(second.calls, [], 'a tool must only ever reach its own dataset');
  });

  it('keeps the global literature tool separate from user data', () => {
    assert.ok(searchMedicalLiteratureTool.execute !== undefined);

    const source = readFileSync(fileURLToPath(new URL('./tools.ts', import.meta.url)), 'utf8');
    assert.ok(
      !/import\s*\{[^}]*\bduckDbRepository\b/.test(source),
      'the tool module must not import a process-wide user-data repository',
    );
    assert.ok(!source.includes('queryGenotypeTool ='), 'no module-level genotype tool singleton');
  });
});
