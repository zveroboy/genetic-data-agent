/**
 * Shared object-identity verification tests.
 *
 * Publication and serving both route through this module, so the per-field checks are pinned
 * here once, against every field an S3 head can contradict, rather than twice through two call
 * sites. What the call-site tests still own is the part that is theirs: that publication writes
 * no manifest and that serving issues no scan when verification fails.
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { ParquetObjectSchema, type ParquetObject } from './ingestion-contracts.ts';
import {
  ObjectVerificationError,
  type IdentityDeclaredBy,
  type ObjectVerificationCode,
  verifyObjectIdentities,
  verifyObjectIdentity,
} from './object-identity.ts';
import type {
  ConditionalPutOutcome,
  ObjectHead,
  ObjectLocation,
  ObjectStore,
} from '../infrastructure/object-store/object-store.ts';

const BUCKET = 'genomic-artifacts';

function parquetObject(key: string): ParquetObject {
  return ParquetObjectSchema.parse({
    bucket: BUCKET,
    key,
    etag: 'd41d8cd98f00b204e9800998ecf8427e-3',
    versionId: 'v-1',
    chrom: '12',
    checksumSha256: 'a'.repeat(64),
    byteSize: 1024,
    rowCount: 10,
    minPos: 1,
    maxPos: 100,
  });
}

/** The head a matching object produces, before a test contradicts one field of it. */
function matchingHead(object: ParquetObject): ObjectHead {
  return {
    bucket: object.bucket,
    key: object.key,
    etag: object.etag,
    versionId: object.versionId,
    contentLength: object.byteSize,
    checksumSha256: object.checksumSha256,
  };
}

function verificationErrorFor(
  head: ObjectHead | null,
  declaredBy: IdentityDeclaredBy = 'manifest',
): ObjectVerificationError {
  const object = parquetObject('datasets/ds/versions/v1/attempt-1/chrom=12/part-0.parquet');
  try {
    verifyObjectIdentity(object, head, declaredBy);
  } catch (error) {
    assert.ok(error instanceof ObjectVerificationError, `unexpected error: ${error}`);
    return error;
  }
  return assert.fail('verification accepted an object it should have refused');
}

describe('verifyObjectIdentity', () => {
  it('accepts an object whose stored identity matches its descriptor', () => {
    const object = parquetObject('datasets/ds/versions/v1/attempt-1/chrom=12/part-0.parquet');
    assert.doesNotThrow(() => verifyObjectIdentity(object, matchingHead(object), 'manifest'));
  });

  const contradictions: ReadonlyArray<{
    readonly code: ObjectVerificationCode;
    readonly head: (head: ObjectHead) => ObjectHead | null;
  }> = [
    { code: 'OBJECT_MISSING', head: () => null },
    { code: 'ETAG_MISSING', head: (head) => ({ ...head, etag: null }) },
    { code: 'ETAG_MISMATCH', head: (head) => ({ ...head, etag: 'another-upload' }) },
    { code: 'VERSION_ID_MISMATCH', head: (head) => ({ ...head, versionId: 'v-2' }) },
    { code: 'SIZE_MISMATCH', head: (head) => ({ ...head, contentLength: head.contentLength! - 1 }) },
    { code: 'CHECKSUM_METADATA_MISSING', head: (head) => ({ ...head, checksumSha256: null }) },
    {
      code: 'CHECKSUM_METADATA_MISMATCH',
      head: (head) => ({ ...head, checksumSha256: 'f'.repeat(64) }),
    },
  ];

  for (const { code, head } of contradictions) {
    it(`refuses an object that contradicts its descriptor with ${code}`, () => {
      const object = parquetObject('datasets/ds/versions/v1/attempt-1/chrom=12/part-0.parquet');
      const error = verificationErrorFor(head(matchingHead(object)));

      assert.equal(error.code, code);
      assert.equal(error.name, 'ObjectVerificationFailed');
      assert.equal(error.key, object.key);
      // The code and the offending key lead the message: it is what an operator sees. The HTTP
      // layer and Temporal's retry classification both key off `error.name`
      // ('ObjectVerificationFailed', asserted above), never off this message text.
      assert.ok(error.message.startsWith(`${code}: object '${object.key}' `));
    });
  }

  it('reports an absent ETag as ETAG_MISSING rather than a mismatch against null', () => {
    const object = parquetObject('datasets/ds/versions/v1/attempt-1/chrom=12/part-0.parquet');
    // Both the ETag and the checksum are absent *and* the size disagrees; the diagnosis names
    // the missing field, which is the one an operator can act on.
    const error = verificationErrorFor({
      ...matchingHead(object),
      etag: null,
      contentLength: 1,
      checksumSha256: null,
    });

    assert.equal(error.code, 'ETAG_MISSING');
  });

  it('names the document that declared the identity it is contradicting', () => {
    const object = parquetObject('datasets/ds/versions/v1/attempt-1/chrom=12/part-0.parquet');
    const drifted = { ...matchingHead(object), etag: 'another-upload' };

    assert.match(verificationErrorFor(drifted, 'inventory').message, /the inventory declares/);
    assert.match(verificationErrorFor(drifted, 'manifest').message, /the manifest declares/);
  });
});

/** Heads whatever it was seeded with, recording the order requests were issued in. */
class FakeObjectStore implements ObjectStore {
  readonly requests: string[] = [];
  readonly heads = new Map<string, ObjectHead | null>();

  seed(head: ObjectHead | null, location: ObjectLocation): void {
    this.heads.set(`${location.bucket}/${location.key}`, head);
  }

  async head(location: ObjectLocation): Promise<ObjectHead | null> {
    this.requests.push(`HEAD ${location.bucket}/${location.key}`);
    return this.heads.get(`${location.bucket}/${location.key}`) ?? null;
  }

  async headMany(locations: readonly ObjectLocation[]): Promise<readonly (ObjectHead | null)[]> {
    return Promise.all(locations.map((location) => this.head(location)));
  }

  async getJson(): Promise<unknown> {
    return assert.fail('identity verification must not read object bodies');
  }

  async putJsonConditional(): Promise<ConditionalPutOutcome> {
    return assert.fail('identity verification must not write');
  }
}

describe('verifyObjectIdentities', () => {
  const objects = ['a', 'b', 'c'].map((part) =>
    parquetObject(`datasets/ds/versions/v1/attempt-1/chrom=12/part-${part}.parquet`),
  );

  function storeWith(overrides: ReadonlyMap<string, ObjectHead | null>): FakeObjectStore {
    const store = new FakeObjectStore();
    for (const object of objects) {
      const override = overrides.get(object.key);
      store.seed(override === undefined ? matchingHead(object) : override, object);
    }
    return store;
  }

  it('heads exactly the declared objects, in the order it was given them', async () => {
    // `FakeObjectStore.headMany` is a plain `Promise.all` and ignores `concurrency`, so this
    // only pins that `verifyObjectIdentities` issues one HEAD per declared object and passes
    // them through in input order — it cannot observe the *bounded*-concurrency guarantee,
    // which the fake makes trivially true regardless of `concurrency`. The real guarantee (that
    // requests still start in input order with only `concurrency` in flight) is pinned against
    // `headManyBounded` by 'heads objects in canonical manifest order with bounded concurrency'
    // in `control-plane-activities.test.ts`.
    const store = storeWith(new Map());

    await verifyObjectIdentities(store, objects, { declaredBy: 'manifest' });

    assert.deepEqual(
      store.requests,
      objects.map((object) => `HEAD ${BUCKET}/${object.key}`),
    );
  });

  it('blames the first failure in canonical order, not the first head to disagree', async () => {
    const store = storeWith(
      new Map([
        [objects[1]!.key, null],
        [objects[2]!.key, null],
      ]),
    );

    await assert.rejects(
      () => verifyObjectIdentities(store, objects, { declaredBy: 'manifest' }),
      (error: unknown) => {
        assert.ok(error instanceof ObjectVerificationError);
        assert.equal(error.key, objects[1]!.key);
        return true;
      },
    );
  });

  it('verifies an empty inventory without touching the store', async () => {
    const store = storeWith(new Map());

    await verifyObjectIdentities(store, [], { declaredBy: 'inventory' });

    assert.deepEqual(store.requests, []);
  });
});
