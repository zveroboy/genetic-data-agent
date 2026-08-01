import path from 'path';
import { duckDbRepository } from './infrastructure/database/duckdb.ts';
import { askBioinformaticsAgent } from './infrastructure/ai/agent.ts';

async function runE2eTest() {
  console.log('[E2E Test] Step 1: Initializing DuckDB from fixtures...');
  const vcfPath = path.resolve(process.cwd(), 'tests/fixtures/demo_user.vcf');
  const tsvPath = path.resolve(process.cwd(), 'tests/fixtures/annotations_mock.tsv');
  
  await duckDbRepository.initFromFixtures(vcfPath, tsvPath);
  console.log('[E2E Test] DuckDB fixtures initialized successfully.');

  const question = 'Can I drink coffee?';
  console.log(`\n[E2E Test] Step 2: Querying AI Agent with question: "${question}"...`);
  
  const response = await askBioinformaticsAgent(question, { dryRunLocal: true });

  console.log('\n--- E2E Test Response ---');
  console.log('Answer:', response.answer);
  console.log('Evidence:', JSON.stringify(response.evidence, null, 2));
  console.log('-------------------------\n');

  if (!response.evidence || response.evidence.length === 0) {
    throw new Error('E2E Test FAILED: No evidence extracted for CYP1A2 / rs762551');
  }

  const caffeineVariant = response.evidence.find((v: any) => v.rsid === 'rs762551');
  if (!caffeineVariant) {
    throw new Error('E2E Test FAILED: rs762551 not found in query_genotype tool output');
  }

  if (caffeineVariant.user_genotype !== 'C/C') {
    throw new Error(`E2E Test FAILED: Expected user_genotype 'C/C' for rs762551, got '${caffeineVariant.user_genotype}'`);
  }

  console.log('✔ E2E Test PASSED: Successfully extracted C/C for rs762551 (CYP1A2) and produced deterministic response!');
}

runE2eTest().catch((err) => {
  console.error('E2E Test FAILED with error:', err);
  process.exit(1);
});
