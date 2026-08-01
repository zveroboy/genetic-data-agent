import { proxyActivities, defineQuery, setHandler } from '@temporalio/workflow';
import type * as activities from './activities.js';

export interface IngestionProgress {
  step: 'DOWNLOADING_S3' | 'PARSING_VCF' | 'VALIDATING' | 'COMPLETED' | 'ERROR';
  fileKey: string;
  percentage: number;
  message: string;
}

export const getProgressQuery = defineQuery<IngestionProgress>('getProgress');

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
  let progress: IngestionProgress = {
    step: 'DOWNLOADING_S3',
    fileKey,
    percentage: 10,
    message: 'Checking and downloading genomic VCF file from S3 / FTP...',
  };

  setHandler(getProgressQuery, () => progress);

  try {
    progress = {
      step: 'DOWNLOADING_S3',
      fileKey,
      percentage: 25,
      message: `Downloading dataset '${fileKey}' from S3 storage...`,
    };
    const localFilePath = await downloadVcf(fileKey);

    progress = {
      step: 'PARSING_VCF',
      fileKey,
      percentage: 55,
      message: 'Multi-threaded parsing & indexing via Rust Rayon engine into DuckDB...',
    };
    await parseAndIndexVcf(userId, localFilePath);

    progress = {
      step: 'VALIDATING',
      fileKey,
      percentage: 85,
      message: 'Running ACMG validation and genetic variant integrity checks...',
    };
    await validateDataset(userId);

    progress = {
      step: 'COMPLETED',
      fileKey,
      percentage: 100,
      message: 'Genomic dataset successfully ingested, indexed, and validated in DuckDB!',
    };
  } catch (err: any) {
    progress = {
      step: 'ERROR',
      fileKey,
      percentage: progress.percentage,
      message: err.message || 'Error occurred during genomic ingestion.',
    };
    throw err;
  }
}
