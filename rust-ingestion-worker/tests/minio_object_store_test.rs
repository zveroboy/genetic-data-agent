//! Integration tests for the S3/MinIO object-store adapter.
//!
//! Every test here talks to a real MinIO (`docker compose up -d minio`) and is therefore
//! `#[ignore]`d: the default `cargo test` run stays hermetic and fast. Run them with
//!
//! ```text
//! cargo test --manifest-path rust-ingestion-worker/Cargo.toml \
//!     --test minio_object_store_test -- --ignored
//! ```
//!
//! Each test owns a freshly created, uniquely named bucket and deletes it again — including
//! when the test body panics — so a shared developer MinIO is never clobbered and nothing is
//! left behind. Objects are seeded and verified through a *separate* SDK client built in this
//! file, so an assertion never re-uses the code under test to confirm its own claim.

use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use futures::FutureExt;
use rust_ingestion_worker::contracts::{FailureType, SourceObject, VARIANTS_SEGMENT};
use rust_ingestion_worker::object_store::{
    canonical_etag, ObjectStoreConfig, ObjectStoreError, S3ObjectStore, UploadRequest,
    CHECKSUM_METADATA_KEY,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

// ---------------------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------------------

fn env_or(name: &str, fallback: &str) -> String {
    std::env::var(name).ok().filter(|value| !value.is_empty()).unwrap_or_else(|| fallback.to_string())
}

/// The dev MinIO from `docker-compose.yml`, overridable for CI.
fn test_config() -> ObjectStoreConfig {
    ObjectStoreConfig {
        endpoint: env_or("S3_ENDPOINT", "http://localhost:9000"),
        region: env_or("S3_REGION", "us-east-1"),
        access_key_id: env_or("S3_ACCESS_KEY", "admin"),
        secret_access_key: env_or("S3_SECRET_KEY", "password123"),
        force_path_style: true,
    }
}

/// A second, independent client used only to seed fixtures and to verify results. Building it
/// here rather than borrowing one out of [`S3ObjectStore`] keeps the assertions honest.
fn raw_client(config: &ObjectStoreConfig) -> Client {
    let sdk = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(config.region.clone()))
        .endpoint_url(config.endpoint.clone())
        .force_path_style(config.force_path_style)
        .credentials_provider(Credentials::new(
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            None,
            None,
            "minio-object-store-test",
        ))
        .build();
    Client::from_conf(sdk)
}

/// Buckets are namespaced per process *and* per test, so parallel runs (including the other
/// language's integration suite) cannot collide.
fn unique_bucket(label: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the epoch")
        .as_nanos();
    let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("rust-objstore-{label}-{nanos:x}-{sequence}")
}

/// Creates a bucket, runs `body`, then removes every object and the bucket itself. A panic in
/// `body` is caught, the bucket is still torn down, and the panic is re-raised afterwards, so a
/// failing assertion never leaks a bucket into the shared MinIO.
async fn with_bucket<F, Fut>(label: &str, body: F)
where
    F: FnOnce(Client, ObjectStoreConfig, String) -> Fut,
    Fut: Future<Output = ()>,
{
    let config = test_config();
    let client = raw_client(&config);
    let bucket = unique_bucket(label);

    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .unwrap_or_else(|error| panic!("cannot create bucket '{bucket}': {error}; is MinIO running? (docker compose up -d minio)"));

    let outcome = AssertUnwindSafe(body(client.clone(), config, bucket.clone()))
        .catch_unwind()
        .await;

    empty_bucket(&client, &bucket).await;
    client
        .delete_bucket()
        .bucket(&bucket)
        .send()
        .await
        .unwrap_or_else(|error| panic!("cannot delete bucket '{bucket}': {error}"));

    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

async fn empty_bucket(client: &Client, bucket: &str) {
    let mut continuation = None;
    loop {
        let page = client
            .list_objects_v2()
            .bucket(bucket)
            .set_continuation_token(continuation)
            .send()
            .await
            .unwrap_or_else(|error| panic!("cannot list '{bucket}': {error}"));

        for object in page.contents() {
            let key = object.key().expect("listed object has a key");
            client
                .delete_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .unwrap_or_else(|error| panic!("cannot delete '{bucket}/{key}': {error}"));
        }

        if page.is_truncated() == Some(true) {
            continuation = page.next_continuation_token().map(str::to_string);
        } else {
            return;
        }
    }
}

// ---------------------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------------------

/// Uploads `body` and returns the [`SourceObject`] descriptor the control plane would have
/// recorded for it: canonical (unquoted) ETag, the bucket's version ID if any, exact size.
async fn seed_source(client: &Client, bucket: &str, key: &str, body: &[u8]) -> SourceObject {
    let response = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await
        .unwrap_or_else(|error| panic!("cannot seed '{bucket}/{key}': {error}"));

    SourceObject {
        bucket: bucket.to_string(),
        key: key.to_string(),
        etag: canonical_etag(response.e_tag()).expect("MinIO returns an ETag for a PUT"),
        version_id: response.version_id().map(str::to_string),
        content_length: body.len() as u64,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn write_local(directory: &Path, name: &str, bytes: &[u8]) -> PathBuf {
    let path = directory.join(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create local fixture directory");
    }
    std::fs::write(&path, bytes).expect("write local fixture");
    path
}

/// A deterministic, incompressible-enough blob of `size` bytes.
fn pseudo_random_bytes(size: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

/// One published object as MinIO reports it back, read with the independent client.
struct HeadFacts {
    etag: String,
    content_length: u64,
    checksum_metadata: Option<String>,
}

async fn head(client: &Client, bucket: &str, key: &str) -> HeadFacts {
    let response = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap_or_else(|error| panic!("cannot HEAD '{bucket}/{key}': {error}"));

    let metadata: HashMap<String, String> = response
        .metadata()
        .map(|entries| {
            entries
                .iter()
                .map(|(name, value)| (name.to_ascii_lowercase(), value.clone()))
                .collect()
        })
        .unwrap_or_default();

    HeadFacts {
        etag: canonical_etag(response.e_tag()).expect("MinIO returns an ETag for a HEAD"),
        content_length: response.content_length().expect("MinIO reports a content length") as u64,
        checksum_metadata: metadata.get(CHECKSUM_METADATA_KEY).cloned(),
    }
}

async fn list_keys(client: &Client, bucket: &str, prefix: &str) -> Vec<String> {
    let page = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await
        .unwrap_or_else(|error| panic!("cannot list '{bucket}/{prefix}': {error}"));
    let mut keys: Vec<String> = page
        .contents()
        .iter()
        .filter_map(|object| object.key().map(str::to_string))
        .collect();
    keys.sort();
    keys
}

const ATTEMPT_PREFIX: &str = "datasets/ds-test-001/versions/iv-test-001/attempt-1/";

fn expect_failure(error: &ObjectStoreError, expected: FailureType) {
    assert_eq!(
        error.failure_type(),
        expected,
        "'{error}' must map onto {expected}"
    );
    assert_eq!(
        error.failure_type().is_retryable(),
        expected.is_retryable(),
        "retryability comes from the frozen contract"
    );
}

// ---------------------------------------------------------------------------------------
// download_exact
// ---------------------------------------------------------------------------------------

/// The happy path: the declared ETag matches what MinIO holds before *and* after the transfer,
/// the bytes land on disk verbatim, and the adapter reports the canonical unquoted ETag.
#[tokio::test]
#[ignore = "requires MinIO: docker compose up -d minio"]
async fn downloads_the_exact_source_object_when_the_etag_matches() {
    with_bucket("dl-exact", |client, config, bucket| async move {
        let body = b"##fileformat=VCFv4.2\n#CHROM\tPOS\tID\tREF\tALT\n1\t100\t.\tA\tG\n";
        let source = seed_source(&client, &bucket, "samples/demo_user.vcf", body).await;

        let store = S3ObjectStore::new(&config).await;
        let scratch = TempDir::new().expect("temp dir");
        let destination = scratch.path().join("source.vcf");

        let downloaded = store
            .download_exact(&source, &destination)
            .await
            .expect("the exact source downloads");

        assert_eq!(std::fs::read(&destination).expect("read download"), body);
        assert_eq!(downloaded.byte_size, body.len() as u64);
        assert_eq!(downloaded.etag, source.etag);
        assert!(
            !downloaded.etag.contains('"'),
            "the reported ETag must be in canonical unquoted form, got {:?}",
            downloaded.etag
        );
        assert_eq!(downloaded.version_id, source.version_id);
    })
    .await;
}

/// A source whose ETag no longer matches the one the workflow was scheduled with is a changed
/// source: deterministic for this workflow input, so it must never be retried.
#[tokio::test]
#[ignore = "requires MinIO: docker compose up -d minio"]
async fn rejects_a_source_whose_etag_does_not_match() {
    with_bucket("dl-etag", |client, config, bucket| async move {
        let mut source =
            seed_source(&client, &bucket, "samples/demo_user.vcf", b"original contents\n").await;
        source.etag = "0123456789abcdef0123456789abcdef".to_string();

        let store = S3ObjectStore::new(&config).await;
        let scratch = TempDir::new().expect("temp dir");
        let destination = scratch.path().join("source.vcf");

        let error = store
            .download_exact(&source, &destination)
            .await
            .expect_err("a mismatched ETag must be refused");

        expect_failure(&error, FailureType::SourceObjectChanged);
        assert!(!error.failure_type().is_retryable());
        assert!(
            !destination.exists(),
            "a refused download must not leave a partial file behind"
        );
    })
    .await;
}

/// The same check, exercised through a real overwrite rather than a fabricated ETag: the
/// object the workflow allowlisted has been replaced by different bytes.
#[tokio::test]
#[ignore = "requires MinIO: docker compose up -d minio"]
async fn rejects_a_source_that_was_replaced_after_it_was_allowlisted() {
    with_bucket("dl-replaced", |client, config, bucket| async move {
        let key = "samples/demo_user.vcf";
        let source = seed_source(&client, &bucket, key, b"the allowlisted bytes\n").await;
        let replaced = seed_source(&client, &bucket, key, b"somebody else's bytes\n").await;
        assert_ne!(source.etag, replaced.etag, "the fixture must actually change");

        let store = S3ObjectStore::new(&config).await;
        let scratch = TempDir::new().expect("temp dir");
        let destination = scratch.path().join("source.vcf");

        let error = store
            .download_exact(&source, &destination)
            .await
            .expect_err("a replaced source must be refused");

        expect_failure(&error, FailureType::SourceObjectChanged);
        assert!(!destination.exists());
    })
    .await;
}

/// A declared size that disagrees with the object is the same class of problem as a changed
/// ETag, and is caught before a single byte is transferred.
#[tokio::test]
#[ignore = "requires MinIO: docker compose up -d minio"]
async fn rejects_a_source_whose_declared_size_does_not_match() {
    with_bucket("dl-size", |client, config, bucket| async move {
        let mut source =
            seed_source(&client, &bucket, "samples/demo_user.vcf", b"twenty two bytes long\n").await;
        source.content_length += 1;

        let store = S3ObjectStore::new(&config).await;
        let scratch = TempDir::new().expect("temp dir");
        let destination = scratch.path().join("source.vcf");

        let error = store
            .download_exact(&source, &destination)
            .await
            .expect_err("a mismatched content length must be refused");

        expect_failure(&error, FailureType::SourceObjectChanged);
    })
    .await;
}

/// A source object that no longer exists cannot be produced by a retry of this activity with
/// this input either, so it is `SourceObjectChanged` rather than a transient store failure.
#[tokio::test]
#[ignore = "requires MinIO: docker compose up -d minio"]
async fn rejects_a_source_that_no_longer_exists() {
    with_bucket("dl-missing", |client, config, bucket| async move {
        let key = "samples/demo_user.vcf";
        let source = seed_source(&client, &bucket, key, b"here for now\n").await;
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .expect("delete the source");

        let store = S3ObjectStore::new(&config).await;
        let scratch = TempDir::new().expect("temp dir");
        let destination = scratch.path().join("source.vcf");

        let error = store
            .download_exact(&source, &destination)
            .await
            .expect_err("a deleted source must be refused");

        expect_failure(&error, FailureType::SourceObjectChanged);
    })
    .await;
}

/// The adapter's mid-flight guard is `If-Match` on the `GET`: it is what stops a replacement
/// slipped in between the pre-download `HEAD` and the transfer from being served. That guard is
/// only worth anything if the store actually enforces it, so the protocol assumption is
/// asserted here directly, with the independent client, in the quoted entity-tag form the
/// adapter sends. If this ever fails, `download_exact` is relying on a check the store ignores.
#[tokio::test]
#[ignore = "requires MinIO: docker compose up -d minio"]
async fn the_store_enforces_if_match_on_a_get() {
    with_bucket("dl-ifmatch", |client, _config, bucket| async move {
        let key = "samples/demo_user.vcf";
        let original = seed_source(&client, &bucket, key, b"the allowlisted bytes\n").await;
        seed_source(&client, &bucket, key, b"somebody else's bytes\n").await;

        let error = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .if_match(format!("\"{}\"", original.etag))
            .send()
            .await
            .expect_err("a stale If-Match must be refused by the store, not served");

        let status = error
            .raw_response()
            .map(|response| response.status().as_u16());
        assert_eq!(
            status,
            Some(412),
            "a stale If-Match must fail the precondition; got {status:?}"
        );

        // The same header, current, must still serve the object: the guard has to be exact,
        // not merely restrictive.
        let current = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .expect("head the replacement");
        let current_etag = canonical_etag(current.e_tag()).expect("an ETag");
        client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .if_match(format!("\"{current_etag}\""))
            .send()
            .await
            .expect("a matching If-Match must be served");
    })
    .await;
}

/// A full-genome VCF does not fit in memory, so the body is streamed to disk. This uses an
/// object far larger than any single read buffer and checks the bytes arrive intact and in
/// order — a chunked writer that dropped or reordered a chunk changes the digest.
#[tokio::test]
#[ignore = "requires MinIO: docker compose up -d minio"]
async fn streams_a_large_source_to_a_temp_file_without_buffering_it() {
    with_bucket("dl-stream", |client, config, bucket| async move {
        const SIZE: usize = 12 * 1024 * 1024;
        let body = pseudo_random_bytes(SIZE, 0x5eed_1234);
        let source = seed_source(&client, &bucket, "samples/na12878_hg001.vcf.gz", &body).await;

        let store = S3ObjectStore::new(&config).await;
        let scratch = TempDir::new().expect("temp dir");
        let destination = scratch.path().join("na12878.vcf.gz");

        let downloaded = store
            .download_exact(&source, &destination)
            .await
            .expect("a large source downloads");

        assert_eq!(downloaded.byte_size, SIZE as u64);
        let landed = std::fs::read(&destination).expect("read download");
        assert_eq!(landed.len(), SIZE);
        assert_eq!(
            sha256_hex(&landed),
            sha256_hex(&body),
            "the streamed bytes must be identical to the source"
        );
    })
    .await;
}

/// An attempt writes only to paths it created; reusing a scratch path could append to, or
/// silently mix with, a previous attempt's leftovers.
#[tokio::test]
#[ignore = "requires MinIO: docker compose up -d minio"]
async fn refuses_to_download_onto_an_existing_local_file() {
    with_bucket("dl-existing", |client, config, bucket| async move {
        let source = seed_source(&client, &bucket, "samples/demo_user.vcf", b"fresh\n").await;

        let scratch = TempDir::new().expect("temp dir");
        let destination = write_local(scratch.path(), "source.vcf", b"a previous attempt\n");

        let store = S3ObjectStore::new(&config).await;
        let error = store
            .download_exact(&source, &destination)
            .await
            .expect_err("an existing destination must be refused");

        expect_failure(&error, FailureType::ArtifactWriteFailed);
        assert_eq!(
            std::fs::read(&destination).expect("read"),
            b"a previous attempt\n",
            "the pre-existing file must be left untouched"
        );
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// upload_file
// ---------------------------------------------------------------------------------------

/// The publication path the TypeScript verifier reads back: every object carries the frozen
/// `sha256` user-metadata entry holding the lowercase hex digest of its body, and the returned
/// ETag is the canonical unquoted form the manifest records.
#[tokio::test]
#[ignore = "requires MinIO: docker compose up -d minio"]
async fn uploads_multiple_parquet_files_with_sha256_metadata() {
    with_bucket("up-multi", |client, config, bucket| async move {
        let scratch = TempDir::new().expect("temp dir");
        let files = [
            ("chrom=1/part-000.parquet", pseudo_random_bytes(64 * 1024, 1)),
            ("chrom=12/part-000.parquet", pseudo_random_bytes(48 * 1024, 12)),
            ("chrom=X/part-000.parquet", pseudo_random_bytes(4 * 1024, 23)),
        ];

        let store = S3ObjectStore::new(&config).await;
        let mut uploaded = Vec::new();
        for (relative_path, bytes) in &files {
            let local = write_local(scratch.path(), relative_path, bytes);
            let key = format!("{ATTEMPT_PREFIX}{VARIANTS_SEGMENT}{relative_path}");
            let checksum = sha256_hex(bytes);

            uploaded.push(
                store
                    .upload_file(&UploadRequest {
                        bucket: &bucket,
                        attempt_prefix: ATTEMPT_PREFIX,
                        key: &key,
                        local_path: &local,
                        checksum_sha256: &checksum,
                    })
                    .await
                    .unwrap_or_else(|error| panic!("upload of '{key}' failed: {error}")),
            );
        }

        for (index, (relative_path, bytes)) in files.iter().enumerate() {
            let key = format!("{ATTEMPT_PREFIX}{VARIANTS_SEGMENT}{relative_path}");
            let facts = head(&client, &bucket, &key).await;
            let checksum = sha256_hex(bytes);

            assert_eq!(
                facts.checksum_metadata.as_deref(),
                Some(checksum.as_str()),
                "'{key}' must carry x-amz-meta-{CHECKSUM_METADATA_KEY} with the lowercase hex digest"
            );
            assert_eq!(facts.content_length, bytes.len() as u64);
            assert_eq!(
                facts.etag, uploaded[index].etag,
                "the ETag reported by the upload must equal the one a later HEAD reads back"
            );
            assert!(!uploaded[index].etag.contains('"'), "canonical unquoted ETag");
            assert_eq!(uploaded[index].key, key);
            assert_eq!(uploaded[index].bucket, bucket);
        }

        assert_eq!(
            list_keys(&client, &bucket, ATTEMPT_PREFIX).await.len(),
            files.len(),
            "exactly the enumerated inventory is published, nothing else"
        );
    })
    .await;
}

/// A retry of the same attempt re-uploads the same inventory to the same prefix. It must
/// succeed, must not append anything, and must not change any object's published identity.
#[tokio::test]
#[ignore = "requires MinIO: docker compose up -d minio"]
async fn retrying_an_identical_attempt_prefix_keeps_the_published_identity() {
    with_bucket("up-retry", |client, config, bucket| async move {
        let scratch = TempDir::new().expect("temp dir");
        let bytes = pseudo_random_bytes(32 * 1024, 7);
        let relative_path = "chrom=1/part-000.parquet";
        let local = write_local(scratch.path(), relative_path, &bytes);
        let key = format!("{ATTEMPT_PREFIX}{VARIANTS_SEGMENT}{relative_path}");
        let checksum = sha256_hex(&bytes);

        let store = S3ObjectStore::new(&config).await;
        let request = UploadRequest {
            bucket: &bucket,
            attempt_prefix: ATTEMPT_PREFIX,
            key: &key,
            local_path: &local,
            checksum_sha256: &checksum,
        };

        let first = store.upload_file(&request).await.expect("first upload");
        let after_first = head(&client, &bucket, &key).await;

        let second = store.upload_file(&request).await.expect("retried upload");
        let after_second = head(&client, &bucket, &key).await;

        assert_eq!(first.key, second.key);
        assert_eq!(first.etag, second.etag, "identical bytes keep an identical ETag");
        assert_eq!(after_first.etag, after_second.etag);
        assert_eq!(after_first.content_length, after_second.content_length);
        assert_eq!(
            after_second.checksum_metadata.as_deref(),
            Some(checksum.as_str()),
            "the retry must not drop the checksum metadata"
        );
        assert_eq!(
            list_keys(&client, &bucket, ATTEMPT_PREFIX).await,
            vec![key],
            "a retry replaces the object in place; it never appends to the prefix"
        );
    })
    .await;
}

/// The destination key is derived by the caller from Temporal metadata, never from anything the
/// VCF says. The adapter refuses any key that is not strictly below the attempt prefix it was
/// handed, so a bug or a hostile relative path cannot escape the attempt's own namespace.
#[tokio::test]
#[ignore = "requires MinIO: docker compose up -d minio"]
async fn refuses_a_destination_key_outside_the_attempt_prefix() {
    with_bucket("up-escape", |client, config, bucket| async move {
        let scratch = TempDir::new().expect("temp dir");
        let bytes = b"parquet-ish".to_vec();
        let local = write_local(scratch.path(), "part-000.parquet", &bytes);
        let checksum = sha256_hex(&bytes);
        let store = S3ObjectStore::new(&config).await;

        let rejected = [
            // A sibling attempt of the same artifact version.
            "datasets/ds-test-001/versions/iv-test-001/attempt-2/variants/chrom=1/part-000.parquet",
            // Another dataset entirely.
            "datasets/other/versions/iv-test-001/attempt-1/variants/chrom=1/part-000.parquet",
            // The published manifest, which only the control plane may write.
            "datasets/ds-test-001/manifest.json",
            // A traversal that textually starts with the prefix but climbs back out.
            "datasets/ds-test-001/versions/iv-test-001/attempt-1/../attempt-2/x.parquet",
            // The prefix itself: a directory marker, not an object of the inventory.
            ATTEMPT_PREFIX,
            "",
        ];

        for key in rejected {
            let error = store
                .upload_file(&UploadRequest {
                    bucket: &bucket,
                    attempt_prefix: ATTEMPT_PREFIX,
                    key,
                    local_path: &local,
                    checksum_sha256: &checksum,
                })
                .await
                .err()
                .unwrap_or_else(|| panic!("'{key}' must not be accepted as a destination"));
            expect_failure(&error, FailureType::ArtifactValidationFailed);
        }

        assert!(
            list_keys(&client, &bucket, "").await.is_empty(),
            "a refused upload must not have written anything"
        );
    })
    .await;
}

/// The digest travels as lowercase hex and is the digest of the object body. A caller that
/// supplies anything else would publish an object the TypeScript verifier rejects with
/// `CHECKSUM_METADATA_MISMATCH`, so the adapter refuses it up front.
#[tokio::test]
#[ignore = "requires MinIO: docker compose up -d minio"]
async fn refuses_a_checksum_that_is_not_lowercase_hex_sha256() {
    with_bucket("up-checksum", |client, config, bucket| async move {
        let scratch = TempDir::new().expect("temp dir");
        let bytes = b"parquet-ish".to_vec();
        let local = write_local(scratch.path(), "part-000.parquet", &bytes);
        let key = format!("{ATTEMPT_PREFIX}{VARIANTS_SEGMENT}chrom=1/part-000.parquet");
        let store = S3ObjectStore::new(&config).await;

        let upper = sha256_hex(&bytes).to_ascii_uppercase();
        for checksum in [upper.as_str(), "", "deadbeef", "not-hex-at-all"] {
            let error = store
                .upload_file(&UploadRequest {
                    bucket: &bucket,
                    attempt_prefix: ATTEMPT_PREFIX,
                    key: &key,
                    local_path: &local,
                    checksum_sha256: checksum,
                })
                .await
                .expect_err("a non-canonical digest must be refused");
            expect_failure(&error, FailureType::ArtifactValidationFailed);
        }

        assert!(list_keys(&client, &bucket, "").await.is_empty());
    })
    .await;
}
