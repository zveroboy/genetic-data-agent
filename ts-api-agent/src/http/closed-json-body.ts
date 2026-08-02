import type { Context } from 'hono';

type ParsedBody =
  | { readonly ok: true; readonly body: Record<string, unknown> }
  | { readonly ok: false; readonly error: string; readonly message: string };

/**
 * Reads a closed JSON object body.
 *
 * "Closed" is the load-bearing part: a field the endpoint does not know about is rejected
 * rather than ignored. Ignoring it would make `{"datasetKey":"demo-small","bucket":"…"}` and
 * `{"datasetKey":"demo-small"}` indistinguishable to the caller, and the day someone starts
 * reading `bucket` the API silently gains an override nobody reviewed.
 */
export async function readClosedJsonObject(
  c: Context,
  allowedFields: readonly string[],
): Promise<ParsedBody> {
  let parsed: unknown;
  try {
    parsed = await c.req.json();
  } catch {
    return { ok: false, error: 'MalformedRequestBody', message: 'the body must be a JSON object' };
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return { ok: false, error: 'MalformedRequestBody', message: 'the body must be a JSON object' };
  }

  const unexpected = Object.keys(parsed).filter((key) => !allowedFields.includes(key));
  if (unexpected.length > 0) {
    return {
      ok: false,
      error: 'UnrecognizedRequestField',
      message:
        `this endpoint accepts only ${allowedFields.join(', ')}; got ${unexpected.join(', ')}. ` +
        'Object URIs, buckets, source paths and manifest overrides are never taken from a request.',
    };
  }
  return { ok: true, body: parsed as Record<string, unknown> };
}
