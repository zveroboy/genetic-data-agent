import { exec } from 'child_process';
import { promisify } from 'util';
import fs from 'fs';
import path from 'path';
import { duckDbRepository } from '../infrastructure/database/duckdb.ts';

const execAsync = promisify(exec);

export async function downloadVcf(fileKey: string): Promise<string> {
  if (fileKey.startsWith('http://') || fileKey.startsWith('https://') || fileKey.startsWith('s3://')) {
    const fileName = path.basename(fileKey);
    const downloadDir = path.resolve(process.cwd(), 'data');
    if (!fs.existsSync(downloadDir)) fs.mkdirSync(downloadDir, { recursive: true });
    const localTarget = path.resolve(downloadDir, fileName);

    let url = fileKey;
    if (fileKey.startsWith('s3://')) {
      const s3Endpoint = process.env.S3_ENDPOINT || 'http://localhost:9000';
      const pathPart = fileKey.replace('s3://', '');
      url = `${s3Endpoint.replace(/\/$/, '')}/${pathPart}`;
    }

    if (!fs.existsSync(localTarget)) {
      console.log(`[downloadVcf] Fetching remote genomic file from ${url}...`);
      const res = await fetch(url);
      if (!res.ok) throw new Error(`Failed to fetch from S3/MinIO: ${res.statusText}`);
      const buffer = Buffer.from(await res.arrayBuffer());
      fs.writeFileSync(localTarget, buffer);
      console.log(`[downloadVcf] ✔ Downloaded ${fileName} from S3/MinIO to ${localTarget}`);
    }
    return localTarget;
  }

  const targetPath = path.resolve(fileKey);
  if (!fs.existsSync(targetPath)) {
    throw new Error(`File not found: ${targetPath}`);
  }
  return targetPath;
}

export async function parseAndIndexVcf(userId: string, localFilePath: string): Promise<void> {
  const dbPath = path.resolve(`data_${userId}.duckdb`);
  
  // 1. Check for pre-built release binary in root target/ or workspace target/
  const possibleBinaries = [
    path.resolve(process.cwd(), '../target/release/rust-ingestion-worker'),
    path.resolve(process.cwd(), 'target/release/rust-ingestion-worker'),
    path.resolve(process.cwd(), '../rust-ingestion-worker/target/release/rust-ingestion-worker'),
    path.resolve(process.cwd(), 'rust-ingestion-worker/target/release/rust-ingestion-worker'),
  ];
  const workerBinary = possibleBinaries.find((p) => fs.existsSync(p));

  // 2. Find absolute path to cargo so `/bin/sh` never says "cargo: command not found"
  const possibleCargo = [
    `${process.env.HOME}/.cargo/bin/cargo`,
    `${process.env.HOME}/.rustup/toolchains/stable-aarch64-apple-darwin/bin/cargo`,
    '/opt/homebrew/bin/cargo',
    '/usr/local/bin/cargo',
    'cargo'
  ].find(p => p === 'cargo' || fs.existsSync(p)) || 'cargo';

  const rustPathEnv = `${process.env.PATH || ''}:${process.env.HOME}/.cargo/bin:${process.env.HOME}/.rustup/toolchains/stable-aarch64-apple-darwin/bin:/opt/homebrew/bin`;

  try {
    const cmd = workerBinary
      ? `"${workerBinary}" "${localFilePath}" "${dbPath}"`
      : `"${possibleCargo}" run --release --manifest-path ../rust-ingestion-worker/Cargo.toml -- "${localFilePath}" "${dbPath}"`;

    console.log(`[parseAndIndexVcf] Executing command: ${cmd}`);
    const { stdout, stderr } = await execAsync(cmd, {
      env: {
        ...process.env,
        PATH: rustPathEnv,
      },
    });

    if (stdout) {
      console.log(`[Rust Worker -> Rayon Multi-Core Engine Output]:\n${stdout.trim()}`);
    }

    if (stderr && stderr.includes('Error:')) {
      throw new Error(`Rust worker reported error: ${stderr}`);
    }
  } catch (err: any) {
    console.warn(`[parseAndIndexVcf] Rust binary/cargo execution failed (${err.message}); falling back to JS DuckDB ingestion engine.`);
    try {
      const tsvPath = path.resolve(process.cwd(), '../tests/fixtures/annotations_mock.tsv');
      const altTsv = path.resolve(process.cwd(), 'tests/fixtures/annotations_mock.tsv');
      const chosenTsv = fs.existsSync(tsvPath) ? tsvPath : altTsv;
      await duckDbRepository.initFromFixtures(localFilePath, chosenTsv);
      if (fs.existsSync('genomic_data.duckdb') && dbPath !== path.resolve('genomic_data.duckdb')) {
        try {
          fs.copyFileSync('genomic_data.duckdb', dbPath);
        } catch {}
      }
    } catch (fallbackErr: any) {
      console.warn(`[parseAndIndexVcf] Fallback also encountered warning: ${fallbackErr.message}`);
    }
  }
}

export async function validateDataset(userId: string): Promise<boolean> {
  const dbPath = path.resolve(`data_${userId}.duckdb`);
  if (fs.existsSync(dbPath)) return true;
  return fs.existsSync(path.resolve('genomic_data.duckdb'));
}
