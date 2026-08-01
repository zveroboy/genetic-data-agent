import http from 'node:http';
import { Hono } from 'hono';
import { askBioinformaticsAgent } from './infrastructure/ai/agent.ts';
import { duckDbRepository } from './infrastructure/database/duckdb.ts';

const app = new Hono();

app.get('/health', (c) => c.json({ status: 'ok', service: 'ts-api-agent' }));

app.post('/ask', async (c) => {
  try {
    const body = await c.req.json<{ question: string; dryRunLocal?: boolean }>();
    if (!body.question) {
      return c.json({ error: 'Question is required' }, 400);
    }

    const result = await askBioinformaticsAgent(body.question, {
      dryRunLocal: body.dryRunLocal,
    });
    return c.json(result);
  } catch (err: any) {
    console.error('[API Server /ask Error]:', err);
    return c.json({ error: err.message || String(err) }, 500);
  }
});

app.post('/init-fixtures', async (c) => {
  const vcfPath = '../tests/fixtures/demo_user.vcf';
  const tsvPath = '../tests/fixtures/annotations_mock.tsv';
  await duckDbRepository.initFromFixtures(vcfPath, tsvPath);
  return c.json({ status: 'fixtures initialized' });
});

const port = 3000;

if (import.meta.url === `file://${process.argv[1]}`) {
  const server = http.createServer(async (req, res) => {
    try {
      const url = `http://${req.headers.host || 'localhost'}${req.url}`;
      const method = req.method || 'GET';
      const headers = new Headers();
      for (const [key, val] of Object.entries(req.headers)) {
        if (val) headers.set(key, Array.isArray(val) ? val.join(',') : val);
      }

      let body: any = undefined;
      if (method !== 'GET' && method !== 'HEAD') {
        const buffers: Buffer[] = [];
        for await (const chunk of req) {
          buffers.push(chunk);
        }
        body = Buffer.concat(buffers);
      }

      const request = new Request(url, { method, headers, body } as any);
      const response = await app.fetch(request);

      res.statusCode = response.status;
      response.headers.forEach((value, key) => res.setHeader(key, value));

      const responseBuffer = await response.arrayBuffer();
      res.end(Buffer.from(responseBuffer));
    } catch (err: any) {
      console.error('Server error:', err);
      res.statusCode = 500;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ error: err.message }));
    }
  });

  server.listen(port, () => {
    console.log(`TS API Agent running natively on http://localhost:${port}`);
  });
}

export default app;
