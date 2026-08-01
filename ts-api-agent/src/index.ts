import http from 'node:http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { Hono } from 'hono';
import { askBioinformaticsAgent } from './infrastructure/ai/agent.ts';
import { duckDbRepository } from './infrastructure/database/duckdb.ts';

const app = new Hono();
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

app.get('/health', (c) => c.json({ status: 'ok', service: 'ts-api-agent' }));

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

const port = 3000;

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
