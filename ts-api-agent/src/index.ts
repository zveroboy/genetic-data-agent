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
  const vcfPath = path.resolve(process.cwd(), '../tests/fixtures/demo_user.vcf');
  const tsvPath = path.resolve(process.cwd(), '../tests/fixtures/annotations_mock.tsv');
  const altVcf = path.resolve(process.cwd(), 'tests/fixtures/demo_user.vcf');
  const altTsv = path.resolve(process.cwd(), 'tests/fixtures/annotations_mock.tsv');
  const chosenVcf = fs.existsSync(vcfPath) ? vcfPath : altVcf;
  const chosenTsv = fs.existsSync(tsvPath) ? tsvPath : altTsv;
  await duckDbRepository.initFromFixtures(chosenVcf, chosenTsv);
  return c.json({ status: 'fixtures initialized', vcf: chosenVcf, tsv: chosenTsv });
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
