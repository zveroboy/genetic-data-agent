/**
 * Guards the claim `index.test.ts`'s "reports a dataset whose targets are absent as an answer"
 * case makes in its comment: that the *shipped* `askBioinformaticsAgent` absorbs
 * `TargetNotResolvableError`/`TargetNotPresentError` into a note-bearing answer rather than
 * letting them propagate as a crash. That index.test.ts harness fakes the agent entirely, so it
 * cannot prove anything about `agent.ts`'s own behaviour — this file runs the *real*
 * `askBioinformaticsAgent` and the *real* `createQueryGenotypeTool` (`agent.ts:34-60`) around a
 * fake `GenotypeRepository`, so the only thing under test is the agent's own error handling.
 *
 * `dryRunLocal: true` is used throughout to reach this code deterministically, without a
 * Cerebras or Anthropic API key.
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { TargetNotResolvableError } from '../database/clinvar-coordinate-resolver.ts';
import type { GenotypeQueryResult, GenotypeRepository } from '../database/duckdb.ts';
import { TargetNotPresentError } from '../database/parquet-dataset-resolver.ts';
import { askBioinformaticsAgent } from './agent.ts';

function repositoryThatThrows(error: Error): GenotypeRepository {
  return {
    datasetId: 'ds-serving-001',
    async synthesizeVariant(): Promise<GenotypeQueryResult> {
      throw error;
    },
  };
}

describe('askBioinformaticsAgent — absorbing "nothing to read" outcomes', () => {
  it('turns TargetNotPresentError into an empty-evidence note, not a thrown error', async () => {
    const repository = repositoryThatThrows(
      new TargetNotPresentError('ds-serving-001', 'the requested coordinates'),
    );

    const response = await askBioinformaticsAgent('What is my BRCA1 status?', {
      genotypeRepository: repository,
      dryRunLocal: true,
    });

    assert.deepEqual(response.evidence, []);
    assert.equal('provenance' in response, false, 'nothing was read, so nothing is claimed as read');
    assert.match(response.answer, /contains no variant/);
  });

  it('turns TargetNotResolvableError into an empty-evidence note, not a thrown error', async () => {
    const repository = repositoryThatThrows(
      new TargetNotResolvableError('NOT_A_GENE', 'demo-clinvar-grch38-v1'),
    );

    const response = await askBioinformaticsAgent('Tell me about NOT_A_GENE', {
      genotypeRepository: repository,
      dryRunLocal: true,
    });

    assert.deepEqual(response.evidence, []);
    assert.match(response.answer, /not present in reference snapshot/);
  });

  it('does not absorb an unrelated failure — it propagates', async () => {
    const repository = repositoryThatThrows(new Error('S3 outage: connection reset'));

    await assert.rejects(
      () =>
        askBioinformaticsAgent('What is my BRCA1 status?', {
          genotypeRepository: repository,
          dryRunLocal: true,
        }),
      /S3 outage/,
    );
  });
});
