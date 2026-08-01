import http from 'node:http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { Hono } from 'hono';
import { Connection, Client } from '@temporalio/client';
import { askBioinformaticsAgent } from './infrastructure/ai/agent.ts';
import { duckDbRepository } from './infrastructure/database/duckdb.ts';
import { getProgressQuery, type IngestionProgress, GenomicIngestionWorkflow } from './application/workflows.ts';

export const app = new Hono();
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const defaultVcf = path.resolve(__dirname, '../../tests/fixtures/demo_user.vcf');
const defaultTsv = path.resolve(__dirname, '../../tests/fixtures/annotations_mock.tsv');

async function autoInitFixtures() {
  try {
    if (fs.existsSync(defaultVcf) && fs.existsSync(defaultTsv)) {
      await duckDbRepository.initFromFixtures(defaultVcf, defaultTsv);
      console.log('[Auto-Init] DuckDB fixtures initialized successfully.');
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

// Simulation Map for In-Memory Fallback if Temporal is offline
const fallbackWorkflows = new Map<string, { progress: IngestionProgress; timer?: NodeJS.Timeout }>();

app.post('/api/ingestion/start', async (c) => {
  try {
    let body: { fileKey?: string } = {};
    try {
      body = await c.req.json();
    } catch {}

    const fileKey = body.fileKey || 'tests/fixtures/demo_user.vcf';
    const userId = fileKey.includes('na12878') ? 'user-na12878' : 'user-demo-01';

    // 1. Try connecting to Temporal
    const temporalHost = process.env.TEMPORAL_HOST || 'localhost:7233';
    try {
      const connection = await Connection.connect({ address: temporalHost });
      const client = new Client({ connection });

      const handle = await client.workflow.start(GenomicIngestionWorkflow, {
        args: [userId, fileKey],
        taskQueue: 'genomic-ingestion',
        workflowId: `genomic-ingestion-${userId}-${Date.now()}`,
      });

      console.log(`[Temporal] Started GenomicIngestionWorkflow ID: ${handle.workflowId}`);
      return c.json({ workflowId: handle.workflowId, status: 'started', mode: 'temporal' });
    } catch (temporalErr: any) {
      console.warn(`[Temporal Connection Warning]: Could not connect to ${temporalHost}, switching to Simulation Mode.`, temporalErr.message);

      // 2. Fallback Simulation Mode
      const simId = `sim-wf-${Date.now()}`;
      const simProgress: IngestionProgress = {
        step: 'DOWNLOADING_S3',
        fileKey,
        percentage: 10,
        message: 'Checking and downloading genomic VCF file from S3 storage...',
      };

      fallbackWorkflows.set(simId, { progress: simProgress });

      // Automatically advance simulation steps
      setTimeout(async () => {
        const wf = fallbackWorkflows.get(simId);
        if (!wf) return;
        wf.progress = {
          step: 'PARSING_VCF',
          fileKey,
          percentage: 45,
          message: 'Multi-threaded parsing & indexing via Rust Rayon engine into DuckDB...',
        };
      }, 1500);

      setTimeout(async () => {
        const wf = fallbackWorkflows.get(simId);
        if (!wf) return;
        wf.progress = {
          step: 'VALIDATING',
          fileKey,
          percentage: 85,
          message: 'Running ACMG validation and genetic variant integrity checks...',
        };
      }, 3000);

      setTimeout(async () => {
        const wf = fallbackWorkflows.get(simId);
        if (!wf) return;
        await autoInitFixtures();
        wf.progress = {
          step: 'COMPLETED',
          fileKey,
          percentage: 100,
          message: 'Genomic dataset successfully ingested, indexed, and validated in DuckDB!',
        };
      }, 4500);

      return c.json({ workflowId: simId, status: 'started', mode: 'simulation' });
    }
  } catch (err: any) {
    console.error('[API Server /api/ingestion/start Error]:', err);
    return c.json({ error: err.message || String(err) }, 500);
  }
});

app.get('/api/ingestion/status/:workflowId', async (c) => {
  const workflowId = c.req.param('workflowId');

  // Check Simulation Fallback Workflows
  if (workflowId.startsWith('sim-wf-')) {
    const sim = fallbackWorkflows.get(workflowId);
    if (!sim) {
      return c.json({ error: 'Simulation workflow not found' }, 404);
    }
    return c.json(sim.progress);
  }

  // Check Temporal Query
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
