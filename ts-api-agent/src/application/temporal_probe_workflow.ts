import { proxyActivities } from '@temporalio/workflow';

export interface TemporalRustProbeInput {
  message: string;
  iterations: number;
}

export interface TemporalRustProbeResult {
  echoed: string;
  workerLanguage: 'rust';
}

const rustProbe = proxyActivities<{
  rustActivityProbe(input: { message: string; iterations: number }): Promise<{
    echoed: string;
    workerLanguage: 'rust';
  }>;
}>({
  taskQueue: 'genomic-ingestion-rust',
  startToCloseTimeout: '30 seconds',
  heartbeatTimeout: '1 second',
  retry: { maximumAttempts: 2 },
});

/**
 * Feasibility probe workflow: schedules the external Rust activity by name and returns
 * its result unchanged. Throwaway scaffolding for the Task 1 cross-language gate.
 */
export async function temporalRustProbeWorkflow(
  input: TemporalRustProbeInput,
): Promise<TemporalRustProbeResult> {
  return await rustProbe.rustActivityProbe(input);
}
