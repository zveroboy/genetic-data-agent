import http from 'node:http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { Hono } from 'hono';
import { Connection, Client } from '@temporalio/client';
import { askBioinformaticsAgent } from './infrastructure/ai/agent.ts';
import { duckDbRepository } from './infrastructure/database/duckdb.ts';
import { newDatasetId } from './application/dataset-catalog.ts';
import {
  CONTROL_PLANE_TASK_QUEUE,
  GenomicIngestionWorkflow,
  getProgressQuery,
} from './application/workflows.ts';
import { DATASET_KEYS, isDatasetKey } from './domain/datasets.ts';

export const app = new Hono();
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const defaultVcf = path.resolve(__dirname, '../../tests/fixtures/demo_user.vcf');
const defaultTsv = path.resolve(__dirname, '../../tests/fixtures/annotations_mock.tsv');

async function autoInitFixtures(targetVcf?: string) {
  try {
    const chosenVcf = targetVcf && fs.existsSync(targetVcf) ? targetVcf : defaultVcf;
    if (fs.existsSync(chosenVcf) && fs.existsSync(defaultTsv)) {
      await duckDbRepository.initFromFixtures(chosenVcf, defaultTsv);
      console.log(`[Auto-Init] DuckDB initialized successfully from ${chosenVcf}.`);
    }
  } catch (err: any) {
    console.warn('[Auto-Init Warning]:', err.message);
  }
}

// Trigger auto-init on server start
autoInitFixtures();

// Static HTML Web UI (Zero-Build index.html)
function getHtmlUi(): string {
  const htmlPath = path.resolve(__dirname, '../public/index.html');
  if (fs.existsSync(htmlPath)) {
    return fs.readFileSync(htmlPath, 'utf-8');
  }
  return '<h1>Error: public/index.html not found</h1>';
}

app.get('/', (c) => c.html(getHtmlUi()));
app.get('/ui', (c) => c.html(getHtmlUi()));
app.get('/health', (c) => c.json({ status: 'ok', service: 'ts-api-agent' }));

/**
 * Starts one real ingestion run.
 *
 * The request chooses a seeded catalog key and nothing else; the S3 identity comes from the
 * allowlist inside the workflow's first activity. There is no simulation fallback: fabricating
 * a progress stream would report a dataset as ingested when no manifest was ever published.
 */
app.post('/api/ingestion/start', async (c) => {
  let body: { datasetKey?: string } = {};
  try {
    body = await c.req.json();
  } catch {}

  const datasetKey = body.datasetKey ?? 'demo-small';
  if (!isDatasetKey(datasetKey)) {
    return c.json({ error: `unknown dataset key '${datasetKey}'`, allowed: DATASET_KEYS }, 400);
  }

  const datasetId = newDatasetId(datasetKey);
  const temporalHost = process.env.TEMPORAL_HOST || 'localhost:7233';

  try {
    const connection = await Connection.connect({ address: temporalHost });
    const client = new Client({ connection });

    const handle = await client.workflow.start(GenomicIngestionWorkflow, {
      args: [{ datasetId, datasetKey }],
      taskQueue: CONTROL_PLANE_TASK_QUEUE,
      workflowId: `genomic-ingestion-${datasetId}`,
    });

    console.log(`[Temporal] Started GenomicIngestionWorkflow ID: ${handle.workflowId}`);
    return c.json({ workflowId: handle.workflowId, datasetId, datasetKey, status: 'started' });
  } catch (err: any) {
    console.error('[API Server /api/ingestion/start Error]:', err);
    return c.json({ error: `could not reach Temporal at ${temporalHost}: ${err.message}` }, 503);
  }
});

app.get('/api/ingestion/status/:workflowId', async (c) => {
  const workflowId = c.req.param('workflowId');
  const temporalHost = process.env.TEMPORAL_HOST || 'localhost:7233';
  try {
    const connection = await Connection.connect({ address: temporalHost });
    const client = new Client({ connection });
    const handle = client.workflow.getHandle(workflowId);

    const progress = await handle.query(getProgressQuery);
    return c.json(progress);
  } catch (err: any) {
    console.error(`[API Server /api/ingestion/status Error for ${workflowId}]:`, err);
    return c.json({ error: err.message || 'Workflow not found or query failed.' }, 500);
  }
});

app.post('/ask', async (c) => {
  try {
    const body = await c.req.json<{ question: string; dryRunLocal?: boolean }>();
    if (!body.question) {
      return c.json({ error: 'Question is required' }, 400);
    }

    try {
      const result = await askBioinformaticsAgent(body.question, {
        dryRunLocal: body.dryRunLocal,
      });
      return c.json(result);
    } catch (agentErr: any) {
      if (agentErr?.message?.includes('user_variants')) {
        await autoInitFixtures();
        const retryResult = await askBioinformaticsAgent(body.question, {
          dryRunLocal: body.dryRunLocal,
        });
        return c.json(retryResult);
      }
      throw agentErr;
    }
  } catch (err: any) {
    console.error('[API Server /ask Error]:', err);
    return c.json({ error: err.message || String(err) }, 500);
  }
});

app.post('/init-fixtures', async (c) => {
  await autoInitFixtures();
  return c.json({ status: 'fixtures initialized', vcf: defaultVcf, tsv: defaultTsv });
});

const port = Number(process.env.PORT) || 3000;

if (import.meta.url === `file://${process.argv[1]}`) {
  console.log(`[ts-api-agent] Starting server on port ${port}...`);

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
    const reqMethod = req.method || 'GET';

    const headers: Record<string, string> = {};
    for (const [key, value] of Object.entries(req.headers)) {
      if (value) headers[key] = Array.isArray(value) ? value.join(', ') : value;
    }

    let body: any = null;
    if (['POST', 'PUT', 'PATCH'].includes(reqMethod)) {
      const buffers = [];
      for await (const chunk of req) {
        buffers.push(chunk);
      }
      body = Buffer.concat(buffers);
    }

    const fetchReq = new Request(url.toString(), {
      method: reqMethod,
      headers: new Headers(headers),
      body,
    });

    const honoRes = await app.fetch(fetchReq);

    res.statusCode = honoRes.status;
    honoRes.headers.forEach((val, key) => {
      res.setHeader(key, val);
    });

    const resBody = await honoRes.arrayBuffer();
    res.end(Buffer.from(resBody));
  });

  server.listen(port, () => {
    console.log(`🚀 TS API Agent HTTP Server active at http://localhost:${port}`);
  });
}
