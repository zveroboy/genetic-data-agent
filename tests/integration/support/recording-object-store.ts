/**
 * An `ObjectStore` decorator that answers one question the finished state of a bucket cannot:
 * *was the manifest written only after every object it declares already existed?*
 *
 * Looking at MinIO after a successful run proves nothing about ordering — both the manifest and
 * the Parquet objects are there either way, and `LastModified` has one-second granularity, so a
 * publish that raced its own inventory would look identical to one that did not.
 *
 * So the check is taken at the only instant it is decidable. When the production
 * `publishDataset` activity calls `putJsonConditional` for a `manifest.json`, this decorator
 * pauses, HEADs every object the manifest is about to declare through an **independent** S3
 * client, records what it found, and only then lets the write through. An object that is not
 * there at that moment is recorded as absent, and the test fails — regardless of what the bucket
 * looks like afterwards.
 *
 * The decorator observes; it never substitutes. Every call is delegated to the real store.
 */
import type {
  ConditionalPutOutcome,
  HeadManyOptions,
  ObjectHead,
  ObjectLocation,
  ObjectStore,
} from '../../../ts-api-agent/src/infrastructure/object-store/object-store.ts';

export interface RecordedOperation {
  readonly op: 'head' | 'headMany' | 'getJson' | 'putJsonConditional';
  readonly bucket: string;
  readonly key: string;
  /** Monotonic sequence number across every operation this decorator saw. */
  readonly seq: number;
}

export interface DeclaredObjectPresence {
  readonly key: string;
  readonly present: boolean;
  readonly etag: string | null;
  readonly contentLength: number | null;
  readonly checksumSha256: string | null;
}

/** What was true at the instant a manifest write was about to happen. */
export interface ManifestWriteObservation {
  readonly bucket: string;
  readonly key: string;
  readonly seq: number;
  readonly declaredKeys: readonly string[];
  readonly presence: readonly DeclaredObjectPresence[];
  /** Sequence numbers of the operations that preceded this write. */
  readonly precedingOps: readonly RecordedOperation[];
}

export interface PublicationRecorder extends ObjectStore {
  readonly operations: readonly RecordedOperation[];
  readonly manifestWrites: readonly ManifestWriteObservation[];
}

function isManifestKey(key: string): boolean {
  return key.endsWith('/manifest.json');
}

function declaredKeysOf(value: unknown): string[] {
  const objects = (value as { parquetObjects?: { key?: unknown }[] } | null)?.parquetObjects;
  if (!Array.isArray(objects)) return [];
  return objects.map((object) => String(object.key));
}

/**
 * Wraps `inner` with publication-order recording.
 *
 * `probe` must be a *separate* client from `inner`: the point is to observe the bucket the way
 * an outside reader would, not to ask the same object under test whether it is happy.
 */
export function recordPublicationOrder(inner: ObjectStore, probe: ObjectStore): PublicationRecorder {
  const operations: RecordedOperation[] = [];
  const manifestWrites: ManifestWriteObservation[] = [];
  let seq = 0;

  const record = (op: RecordedOperation['op'], location: ObjectLocation): number => {
    seq += 1;
    operations.push({ op, bucket: location.bucket, key: location.key, seq });
    return seq;
  };

  return {
    operations,
    manifestWrites,

    async head(location: ObjectLocation): Promise<ObjectHead | null> {
      record('head', location);
      return inner.head(location);
    },

    async headMany(
      locations: readonly ObjectLocation[],
      options?: HeadManyOptions,
    ): Promise<readonly (ObjectHead | null)[]> {
      for (const location of locations) record('headMany', location);
      return inner.headMany(locations, options);
    },

    async getJson(location: ObjectLocation): Promise<unknown> {
      record('getJson', location);
      return inner.getJson(location);
    },

    async putJsonConditional(
      location: ObjectLocation,
      value: unknown,
    ): Promise<ConditionalPutOutcome> {
      if (isManifestKey(location.key)) {
        const declaredKeys = declaredKeysOf(value);
        const precedingOps = [...operations];
        const presence: DeclaredObjectPresence[] = [];
        for (const key of declaredKeys) {
          const head = await probe.head({ bucket: location.bucket, key });
          presence.push({
            key,
            present: head !== null,
            etag: head?.etag ?? null,
            contentLength: head?.contentLength ?? null,
            checksumSha256: head?.checksumSha256 ?? null,
          });
        }
        const writeSeq = record('putJsonConditional', location);
        manifestWrites.push({
          bucket: location.bucket,
          key: location.key,
          seq: writeSeq,
          declaredKeys,
          presence,
          precedingOps,
        });
        return inner.putJsonConditional(location, value);
      }

      record('putJsonConditional', location);
      return inner.putJsonConditional(location, value);
    },
  };
}
