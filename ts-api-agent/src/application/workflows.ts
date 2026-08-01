import { proxyActivities } from '@temporalio/workflow';
import type * as activities from './activities.js';

const { downloadVcf, parseAndIndexVcf, validateDataset } = proxyActivities<typeof activities>({
  startToCloseTimeout: '10 minutes',
  heartbeatTimeout: '10 seconds',
  retry: {
    initialInterval: '2 seconds',
    maximumInterval: '1 minute',
    maximumAttempts: 3,
    nonRetryableErrorTypes: ['InvalidVcfFormatError'],
  },
});

export async function GenomicIngestionWorkflow(userId: string, fileKey: string): Promise<void> {
  const localFilePath = await downloadVcf(fileKey);
  await parseAndIndexVcf(userId, localFilePath);
  await validateDataset(userId);
}
