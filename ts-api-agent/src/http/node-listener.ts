import http from 'node:http';

import type { Hono } from 'hono';

/** Bridges Node's `http` server onto Hono's fetch handler; no framework adapter needed. */
export function nodeListener(app: Hono): http.RequestListener {
  return async (req, res) => {
    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);
    const method = req.method ?? 'GET';

    const headers = new Headers();
    for (const [key, value] of Object.entries(req.headers)) {
      if (value === undefined) continue;
      headers.set(key, Array.isArray(value) ? value.join(', ') : value);
    }

    let body: Buffer | undefined;
    if (method !== 'GET' && method !== 'HEAD') {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      body = Buffer.concat(chunks);
    }

    const response = await app.fetch(new Request(url, { method, headers, body }));

    res.statusCode = response.status;
    response.headers.forEach((value, key) => res.setHeader(key, value));
    res.end(Buffer.from(await response.arrayBuffer()));
  };
}
