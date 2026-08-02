//! The S3/MinIO adapter: fetch the exact allowlisted source object, publish the locally
//! enumerated Parquet inventory.
//!
//! This module is the *only* place in the crate that speaks S3, and it speaks nothing else:
//! there is no Temporal here, no DuckDB, no VCF parsing. It is handed a `{bucket, key}` pair
//! and never parses an `s3://` URI or builds a public HTTP URL from one. Keys arrive from the
//! caller, which derives them from Temporal activity metadata — never from anything a VCF
//! says.
//!
//! Two conventions cross the language boundary and are frozen, normatively, in
//! `contracts/ingestion-v1.md` ("S3 storage conventions"). Read that document rather than this
//! comment; the TypeScript counterpart is
//! `ts-api-agent/src/infrastructure/object-store/s3-object-store.ts`.
//!
//! 1. Every uploaded object carries its content digest as the S3 **user metadata** entry named
//!    [`CHECKSUM_METADATA_KEY`] (`x-amz-meta-sha256` on the wire), lowercase hex SHA-256 of the
//!    body — the same value the manifest records as `checksumSha256`.
//! 2. ETags are recorded and compared in canonical *unquoted* form: the header value with its
//!    surrounding double quotes removed and nothing else altered, so a multipart `-N` suffix
//!    survives ([`canonical_etag`]).
//!
//! Failures map onto the frozen [`FailureType`] taxonomy with the same discipline
//! `artifact::classify_source_error` uses: an environmental or transient fault is retryable, a
//! deterministic one is not, and an unrecognised fault is treated as transient because a
//! wasted retry is cheaper than a wrongly permanent failure.
//!
//! **Deployment note.** Immutability of a published version prefix is enforced by the bucket
//! policy, not by this adapter: production denies overwrite of objects under
//! `datasets/*/versions/*/` for the worker's principal. That is compose/ops configuration
//! (Task 9), deliberately not a feature of this code — an adapter that managed its own bucket
//! policy would need the very permissions the policy exists to withhold. Listing likewise
//! exists only for cleanup and audit and is not implemented here, because nothing in the query
//! path may ever select objects by listing: the manifest inventory is the only selector.

use std::path::Path;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use tokio::io::AsyncWriteExt;

use crate::contracts::{FailureType, SourceObject, VARIANTS_SEGMENT};

/// S3 user metadata entry carrying the lowercase hex SHA-256 of an object's content.
/// Mirrors `CHECKSUM_METADATA_KEY` in `object-store.ts`; the wire header is
/// `x-amz-meta-sha256`.
pub const CHECKSUM_METADATA_KEY: &str = "sha256";

/// Length of a hex-encoded SHA-256 digest.
const SHA256_HEX_LENGTH: usize = 64;

/// Write-behind buffer used while streaming a download to disk. A fixed budget: it does not
/// grow with the object, which may be a full-genome VCF.
const DOWNLOAD_BUFFER_BYTES: usize = 1024 * 1024;

/// Published Parquet is opaque binary; the format is declared by the manifest, not by a
/// guessable content type.
const PARQUET_CONTENT_TYPE: &str = "application/octet-stream";

// ---------------------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------------------

/// Everything this adapter can fail with, each pinned to a frozen contract failure type so a
/// caller never has to re-derive retryability.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreError {
    /// The adapter cannot be configured at all: a missing or malformed endpoint, credentials
    /// that were never supplied. Treated as store-unavailable, which is the tolerant
    /// direction — an operator can fix the environment without a new workflow.
    #[error("object store configuration is invalid: {0}")]
    Configuration(String),
    /// The source object is no longer the one the workflow was scheduled against. Deterministic
    /// for the current Workflow input, so it must not be retried.
    #[error("source object changed: {0}")]
    SourceChanged(String),
    /// S3/MinIO is unreachable, timing out, or returning a transient error.
    #[error("object store unavailable: {0}")]
    Unavailable(String),
    /// A local disk or upload failure that a retry may well get past.
    #[error("artifact write failed: {0}")]
    WriteFailed(String),
    /// A deterministic invariant of the request itself was violated — a destination outside the
    /// attempt prefix, a digest that is not canonical hex. Retrying reproduces it exactly.
    #[error("artifact validation failed: {0}")]
    ValidationFailed(String),
}

impl ObjectStoreError {
    pub fn failure_type(&self) -> FailureType {
        match self {
            Self::Configuration(_) | Self::Unavailable(_) => FailureType::ObjectStoreUnavailable,
            Self::SourceChanged(_) => FailureType::SourceObjectChanged,
            Self::WriteFailed(_) => FailureType::ArtifactWriteFailed,
            Self::ValidationFailed(_) => FailureType::ArtifactValidationFailed,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------------------

/// Endpoint, region, credentials and addressing mode. Every value is explicit: nothing is
/// discovered from an instance profile, and an `s3://` string is rejected rather than rewritten
/// into an HTTP endpoint.
#[derive(Clone)]
pub struct ObjectStoreConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// MinIO addresses buckets by path, not by virtual host.
    pub force_path_style: bool,
}

/// Hand-written so the secret key can never reach a log line through a `{:?}` on a config, a
/// worker context, or anything that transitively derives `Debug` from one.
impl std::fmt::Debug for ObjectStoreConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreConfig")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("force_path_style", &self.force_path_style)
            .finish()
    }
}

/// Default region when `S3_REGION` is unset. Matches the TypeScript adapter.
const DEFAULT_REGION: &str = "us-east-1";

impl ObjectStoreConfig {
    /// Reads the same variables as `s3ObjectStoreConfigFromEnv` in TypeScript, so both planes
    /// are pointed at one store by one piece of configuration.
    pub fn from_env() -> Result<Self, ObjectStoreError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// The pure form of [`from_env`](Self::from_env), so the parsing rules are unit-testable
    /// without mutating the process environment.
    pub fn from_lookup(
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, ObjectStoreError> {
        let required = |name: &str| -> Result<String, ObjectStoreError> {
            lookup(name)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    ObjectStoreError::Configuration(format!(
                        "{name} is not set; the object store endpoint and credentials must be explicit"
                    ))
                })
        };

        let endpoint = required("S3_ENDPOINT")?;
        // An `s3://` value is refused, never rewritten: a bucket URI must not be turned into a
        // public HTTP endpoint by this code.
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            return Err(ObjectStoreError::Configuration(format!(
                "S3_ENDPOINT must be an absolute http(s) URL, got '{endpoint}'"
            )));
        }

        let force_path_style = match lookup("S3_FORCE_PATH_STYLE").as_deref() {
            None | Some("") => true,
            Some("true") => true,
            Some("false") => false,
            Some(other) => {
                return Err(ObjectStoreError::Configuration(format!(
                    "S3_FORCE_PATH_STYLE must be 'true' or 'false', got '{other}'"
                )))
            }
        };

        Ok(Self {
            endpoint,
            region: lookup("S3_REGION")
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| DEFAULT_REGION.to_string()),
            access_key_id: required("S3_ACCESS_KEY")?,
            secret_access_key: required("S3_SECRET_KEY")?,
            force_path_style,
        })
    }
}

// ---------------------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------------------

/// What a completed [`S3ObjectStore::download_exact`] observed, re-verified after the transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadedSource {
    /// Canonical unquoted ETag, identical to the one declared on the way in.
    pub etag: String,
    pub version_id: Option<String>,
    /// Bytes actually written to the destination.
    pub byte_size: u64,
}

/// One Parquet file to publish. Borrowed rather than owned: the caller already holds every
/// field, and an upload must not be able to outlive the local file it names.
#[derive(Debug, Clone, Copy)]
pub struct UploadRequest<'a> {
    pub bucket: &'a str,
    /// The attempt-unique prefix, ending in `/`. The only namespace this upload may write to.
    pub attempt_prefix: &'a str,
    /// The full destination key. Must be strictly below `attempt_prefix`. The caller composes
    /// it as `{attempt_prefix}variants/{relative_path}`; this adapter does not add or remove a
    /// segment — Task 5 still owns composing the key — it only refuses one that escapes the
    /// prefix or never enters the `variants/` segment (defence in depth).
    pub key: &'a str,
    pub local_path: &'a Path,
    /// Lowercase hex SHA-256 of the file's bytes, published as the `sha256` user metadata entry.
    pub checksum_sha256: &'a str,
}

/// The published identity of one object, as the manifest will record it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadedObject {
    pub bucket: String,
    pub key: String,
    /// Canonical unquoted ETag.
    pub etag: String,
    pub version_id: Option<String>,
}

// ---------------------------------------------------------------------------------------
// ETag canonicalisation
// ---------------------------------------------------------------------------------------

/// The canonical cross-language ETag form: the header value with its surrounding double quotes
/// removed and nothing else altered.
///
/// A multipart ETag's `-N` suffix is deliberately left intact — it is part of the identity, and
/// the digest before it is not an MD5 of the whole object. The TypeScript peer
/// (`canonicalEtag`) applies exactly this transformation, and a value that disagrees fails
/// every `ETAG_MISMATCH` comparison.
pub fn canonical_etag(raw: Option<&str>) -> Option<String> {
    let raw = raw?;
    if raw.is_empty() {
        return None;
    }
    let canonical = raw
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .unwrap_or(raw);
    if canonical.is_empty() {
        None
    } else {
        Some(canonical.to_string())
    }
}

/// The inverse of [`canonical_etag`]: an `If-Match` header carries an RFC 9110 entity-tag,
/// which is quoted. Contract values are stored canonical (unquoted), so the quotes are put
/// back on the way out rather than the canonical form being weakened to match the wire.
fn http_entity_tag(canonical: &str) -> String {
    format!("\"{canonical}\"")
}

// ---------------------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------------------

/// The AWS SDK adapter, configured for MinIO path-style access.
#[derive(Debug, Clone)]
pub struct S3ObjectStore {
    client: Client,
}

impl S3ObjectStore {
    /// Builds a client from explicit configuration. The shared SDK defaults (timeouts, retry
    /// classification, HTTP client) come from `aws-config`; region, endpoint, credentials and
    /// addressing mode are overridden here, so no credential or region discovery ever runs.
    pub async fn new(config: &ObjectStoreConfig) -> Self {
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .endpoint_url(config.endpoint.clone())
            .credentials_provider(Credentials::new(
                config.access_key_id.clone(),
                config.secret_access_key.clone(),
                None,
                None,
                "genomic-ingestion-worker",
            ))
            .load()
            .await;

        let s3 = aws_sdk_s3::config::Builder::from(&shared)
            .force_path_style(config.force_path_style)
            .build();
        Self {
            client: Client::from_conf(s3),
        }
    }

    /// Reads [`ObjectStoreConfig::from_env`] and builds the adapter.
    pub async fn from_env() -> Result<Self, ObjectStoreError> {
        Ok(Self::new(&ObjectStoreConfig::from_env()?).await)
    }

    /// Streams the *exact* object `source` names onto `destination`, verifying its identity
    /// before and after the transfer.
    ///
    /// Three independent checks have to agree, because each catches a different window:
    ///
    /// - a `HEAD` before the transfer, so a source that already changed costs no bandwidth;
    /// - `If-Match` (and the version ID, when the catalog recorded one) on the `GET` itself, so
    ///   S3 refuses to serve different bytes even if they were swapped in after that `HEAD`;
    /// - a `HEAD` after the transfer, which closes the remaining window in which a multi-part
    ///   read could have spanned a replacement.
    ///
    /// The body is consumed chunk by chunk through a fixed [`DOWNLOAD_BUFFER_BYTES`] write
    /// buffer and is never held whole in memory: the source may be a full-genome VCF, so the
    /// memory cost of a download must not scale with it. A destination that already exists is
    /// refused rather than truncated or appended to, and a failed transfer removes whatever
    /// partial file it created.
    pub async fn download_exact(
        &self,
        source: &SourceObject,
        destination: &Path,
    ) -> Result<DownloadedSource, ObjectStoreError> {
        refuse_existing_path(destination)?;

        let before = self.head_source(source).await?;
        assert_source_identity(source, &before, "before the download")?;

        let outcome = self.stream_to_file(source, destination).await;
        if outcome.is_err() {
            remove_partial(destination);
        }
        let written = outcome?;

        let after = match self.head_source(source).await {
            Ok(after) => after,
            Err(error) => {
                remove_partial(destination);
                return Err(error);
            }
        };
        if let Err(error) = assert_source_identity(source, &after, "after the download") {
            remove_partial(destination);
            return Err(error);
        }

        // A body that stopped early is a transport fault, not a changed object: the identity
        // checks on both sides of the transfer agreed. It stays retryable.
        if written != source.content_length {
            remove_partial(destination);
            return Err(ObjectStoreError::Unavailable(format!(
                "'{}/{}' delivered {written} of {} bytes",
                source.bucket, source.key, source.content_length
            )));
        }

        Ok(DownloadedSource {
            etag: after.etag,
            version_id: after.version_id,
            byte_size: written,
        })
    }

    /// Uploads one locally produced Parquet file to `request.key`, tagging it with the frozen
    /// `sha256` user metadata entry the TypeScript verifier reads back.
    ///
    /// The destination is validated first: the key must sit strictly below the attempt prefix
    /// the caller was granted, and the digest must be canonical lowercase hex. Both are
    /// deterministic refusals — the point is that no key can be smuggled in from file content,
    /// and no object can be published that would fail verification on the other side.
    pub async fn upload_file(
        &self,
        request: &UploadRequest<'_>,
    ) -> Result<UploadedObject, ObjectStoreError> {
        validate_bucket(request.bucket)?;
        validate_destination(request.attempt_prefix, request.key)?;
        validate_checksum(request.checksum_sha256, request.key)?;

        // `from_path` streams the file and sets Content-Length from its metadata, so a large
        // partition is never read into memory here either.
        let body = ByteStream::from_path(request.local_path).await.map_err(|error| {
            ObjectStoreError::WriteFailed(format!(
                "cannot read '{}' for upload: {error}",
                request.local_path.display()
            ))
        })?;

        let response = self
            .client
            .put_object()
            .bucket(request.bucket)
            .key(request.key)
            .body(body)
            .content_type(PARQUET_CONTENT_TYPE)
            .metadata(CHECKSUM_METADATA_KEY, request.checksum_sha256)
            .send()
            .await
            .map_err(|error| {
                upload_error(
                    &format!("cannot upload '{}/{}'", request.bucket, request.key),
                    &error,
                )
            })?;

        let etag = canonical_etag(response.e_tag()).ok_or_else(|| {
            ObjectStoreError::WriteFailed(format!(
                "'{}/{}' was stored without an ETag; its identity cannot be published",
                request.bucket, request.key
            ))
        })?;

        Ok(UploadedObject {
            bucket: request.bucket.to_string(),
            key: request.key.to_string(),
            etag,
            version_id: response.version_id().map(str::to_string),
        })
    }

    /// What a `HEAD` reveals about the source, already canonicalised.
    async fn head_source(&self, source: &SourceObject) -> Result<ObservedSource, ObjectStoreError> {
        let response = self
            .client
            .head_object()
            .bucket(&source.bucket)
            .key(&source.key)
            .set_version_id(source.version_id.clone())
            .send()
            .await
            .map_err(|error| {
                source_error(
                    &format!("cannot inspect '{}/{}'", source.bucket, source.key),
                    &error,
                )
            })?;

        let etag = canonical_etag(response.e_tag()).ok_or_else(|| {
            ObjectStoreError::Unavailable(format!(
                "'{}/{}' was HEADed without an ETag; its identity cannot be verified",
                source.bucket, source.key
            ))
        })?;

        Ok(ObservedSource {
            etag,
            version_id: response.version_id().map(str::to_string),
            content_length: response.content_length().unwrap_or(-1),
        })
    }

    /// `GET` the pinned object and write it to disk chunk by chunk.
    async fn stream_to_file(
        &self,
        source: &SourceObject,
        destination: &Path,
    ) -> Result<u64, ObjectStoreError> {
        let mut response = self
            .client
            .get_object()
            .bucket(&source.bucket)
            .key(&source.key)
            .if_match(http_entity_tag(&source.etag))
            .set_version_id(source.version_id.clone())
            .send()
            .await
            .map_err(|error| {
                source_error(
                    &format!("cannot read '{}/{}'", source.bucket, source.key),
                    &error,
                )
            })?;

        let file = tokio::fs::File::create(destination).await.map_err(|error| {
            ObjectStoreError::WriteFailed(format!(
                "cannot create '{}': {error}",
                destination.display()
            ))
        })?;
        let mut writer = tokio::io::BufWriter::with_capacity(DOWNLOAD_BUFFER_BYTES, file);

        let mut written = 0u64;
        while let Some(chunk) = response.body.next().await {
            let chunk = chunk.map_err(|error| {
                ObjectStoreError::Unavailable(format!(
                    "the body of '{}/{}' stopped after {written} bytes: {error}",
                    source.bucket, source.key
                ))
            })?;
            writer.write_all(&chunk).await.map_err(|error| {
                ObjectStoreError::WriteFailed(format!(
                    "cannot write '{}': {error}",
                    destination.display()
                ))
            })?;
            written += chunk.len() as u64;
        }

        writer.flush().await.map_err(|error| {
            ObjectStoreError::WriteFailed(format!(
                "cannot flush '{}': {error}",
                destination.display()
            ))
        })?;
        writer.into_inner().sync_all().await.map_err(|error| {
            ObjectStoreError::WriteFailed(format!(
                "cannot persist '{}': {error}",
                destination.display()
            ))
        })?;
        Ok(written)
    }
}

/// One `HEAD` of the source, canonicalised.
#[derive(Debug, Clone)]
struct ObservedSource {
    etag: String,
    version_id: Option<String>,
    /// `-1` when the store reported none, which fails the comparison rather than passing it.
    content_length: i64,
}

/// The source must be byte-for-byte the object the workflow was scheduled against.
///
/// `version_id` is compared only when the catalog recorded one: an unversioned bucket reports
/// none, and a bucket that gains versioning later must not retroactively invalidate an input
/// whose recorded `versionId` is `null`.
fn assert_source_identity(
    source: &SourceObject,
    observed: &ObservedSource,
    when: &str,
) -> Result<(), ObjectStoreError> {
    let changed = |what: &str, expected: String, actual: String| {
        ObjectStoreError::SourceChanged(format!(
            "'{}/{}' {when}: {what} is '{actual}', the workflow was scheduled with '{expected}'",
            source.bucket, source.key
        ))
    };

    if observed.etag != source.etag {
        return Err(changed("ETag", source.etag.clone(), observed.etag.clone()));
    }
    if let Some(expected) = &source.version_id {
        if observed.version_id.as_deref() != Some(expected.as_str()) {
            return Err(changed(
                "version ID",
                expected.clone(),
                observed.version_id.clone().unwrap_or_else(|| "<none>".to_string()),
            ));
        }
    }
    if observed.content_length < 0 || observed.content_length as u64 != source.content_length {
        return Err(changed(
            "content length",
            source.content_length.to_string(),
            observed.content_length.to_string(),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Request validation
// ---------------------------------------------------------------------------------------

fn validate_bucket(bucket: &str) -> Result<(), ObjectStoreError> {
    if bucket.is_empty() {
        return Err(ObjectStoreError::ValidationFailed(
            "the destination bucket is empty".to_string(),
        ));
    }
    Ok(())
}

/// The key must be strictly below the attempt prefix, with no way to climb back out.
///
/// This is the single enforcement point for "upload only to the exact attempt prefix supplied
/// by the caller". S3 keys are opaque strings — MinIO would happily store `a/../b` verbatim —
/// so the check is textual and rejects anything whose segments are not plain names: a `..`
/// segment, an empty segment, a bare `.`, a backslash or a control character. A destination
/// derived from file content rather than from Temporal metadata fails here.
///
/// It also requires the relative path to begin with the `variants/` segment
/// ([`VARIANTS_SEGMENT`]) — defence in depth. Task 5 still owns composing
/// `{attemptPrefix}variants/{relativePath}`; this only means a key that omits the segment fails
/// at upload time instead of surfacing later as the TypeScript verifier's
/// `KEY_OUTSIDE_ALLOWED_PREFIX`.
fn validate_destination(attempt_prefix: &str, key: &str) -> Result<(), ObjectStoreError> {
    let reject = |reason: String| Err(ObjectStoreError::ValidationFailed(reason));

    if attempt_prefix.is_empty() || !attempt_prefix.ends_with('/') {
        return reject(format!(
            "the attempt prefix '{attempt_prefix}' must be non-empty and end with '/'"
        ));
    }
    // One trailing slash is the directory marker; anything else (`a//`) has an empty segment
    // and must not pass, so exactly one is removed before the segment check.
    if !is_plain_key(attempt_prefix.strip_suffix('/').unwrap_or(attempt_prefix)) {
        return reject(format!(
            "the attempt prefix '{attempt_prefix}' is not a plain key path"
        ));
    }
    let Some(relative) = key.strip_prefix(attempt_prefix) else {
        return reject(format!(
            "'{key}' is outside the attempt prefix '{attempt_prefix}'; an attempt may only \
             write below the prefix it was granted"
        ));
    };
    if relative.is_empty() {
        return reject(format!(
            "'{key}' is the attempt prefix itself, not an object of the inventory"
        ));
    }
    if !is_plain_key(relative) {
        return reject(format!(
            "'{key}' does not stay below '{attempt_prefix}': '{relative}' is not a plain \
             relative key path"
        ));
    }
    // Defence in depth: Task 5 still owns composing `{attemptPrefix}variants/{relativePath}`,
    // but a key that never enters the `variants/` segment fails here rather than surfacing only
    // at publish time, matching the TypeScript verifier's `KEY_OUTSIDE_ALLOWED_PREFIX`.
    if !relative.starts_with(VARIANTS_SEGMENT) {
        return reject(format!(
            "'{key}' does not sit under the '{VARIANTS_SEGMENT}' segment below \
             '{attempt_prefix}'; an upload must be composed as \
             '{{attemptPrefix}}{VARIANTS_SEGMENT}{{relativePath}}'"
        ));
    }
    Ok(())
}

/// A `/`-separated path of plain segments: no empty segment, no `.`, no `..`, no backslash and
/// no control character.
fn is_plain_key(path: &str) -> bool {
    !path.is_empty()
        && !path.contains('\\')
        && !path.chars().any(char::is_control)
        && path
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

/// The digest is published verbatim as metadata and compared verbatim by the verifier, so it is
/// refused here unless it is exactly the frozen encoding: 64 lowercase hex characters.
fn validate_checksum(checksum: &str, key: &str) -> Result<(), ObjectStoreError> {
    let is_lowercase_hex =
        |byte: &u8| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte);
    let canonical = checksum.len() == SHA256_HEX_LENGTH && checksum.bytes().all(|byte| is_lowercase_hex(&byte));
    if canonical {
        return Ok(());
    }
    // Named explicitly enough to debug a Task 5 wiring bug: the length and the first
    // out-of-place byte, not just "invalid".
    let first_bad = checksum
        .bytes()
        .enumerate()
        .find(|(_, byte)| !is_lowercase_hex(byte))
        .map(|(index, byte)| format!(", first invalid character '{}' at index {index}", byte as char))
        .unwrap_or_default();
    Err(ObjectStoreError::ValidationFailed(format!(
        "the checksum declared for '{key}' is not a {SHA256_HEX_LENGTH}-character lowercase hex \
         SHA-256 (got {} characters{first_bad}); publishing it would fail cross-language \
         verification",
        checksum.len(),
    )))
}

// ---------------------------------------------------------------------------------------
// Local filesystem helpers
// ---------------------------------------------------------------------------------------

/// An attempt writes only to paths it created; mirrors `artifact::refuse_existing_path`.
fn refuse_existing_path(path: &Path) -> Result<(), ObjectStoreError> {
    match path.try_exists() {
        Ok(false) => Ok(()),
        Ok(true) => Err(ObjectStoreError::WriteFailed(format!(
            "download destination '{}' already exists; an attempt never reuses a path",
            path.display()
        ))),
        Err(error) => Err(ObjectStoreError::WriteFailed(format!(
            "cannot inspect '{}': {error}",
            path.display()
        ))),
    }
}

/// Best effort: a failed download must not leave bytes behind for the next attempt to trip
/// over. If the removal itself fails there is nothing useful to do — the original error is the
/// one worth reporting, and [`refuse_existing_path`] will catch the leftover next time.
fn remove_partial(path: &Path) {
    if let Err(error) = std::fs::remove_file(path) {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(
                path = %path.display(),
                %error,
                "could not remove a partial download"
            );
        }
    }
}

// ---------------------------------------------------------------------------------------
// SDK error classification
// ---------------------------------------------------------------------------------------

fn status_of<E>(error: &SdkError<E>) -> Option<u16> {
    error
        .raw_response()
        .map(|response| response.status().as_u16())
}

fn describe<E>(context: &str, error: &SdkError<E>) -> String
where
    E: std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
{
    format!("{context}: {}", DisplayErrorContext(error))
}

/// Classifies a failure while reading the *source*.
///
/// `404` and `412` are statements about the object rather than about the store: the exact
/// object the workflow pinned is gone, or it no longer matches the `If-Match` it was scheduled
/// with. Neither can be fixed by retrying this activity with this input, so both are
/// `SourceObjectChanged`. Everything else — including an unrecognised status, a signature
/// problem or no response at all — is treated as transient, the same safe direction
/// `artifact::classify_source_error` takes.
fn source_error<E>(context: &str, error: &SdkError<E>) -> ObjectStoreError
where
    E: std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
{
    match status_of(error) {
        Some(404) | Some(412) => ObjectStoreError::SourceChanged(describe(context, error)),
        _ => ObjectStoreError::Unavailable(describe(context, error)),
    }
}

/// Classifies a failure while *publishing*. Every case is retryable: a rejected destination has
/// already been refused deterministically by [`validate_destination`] before any request is
/// made, so anything reaching here is the store or the network.
fn upload_error<E>(context: &str, error: &SdkError<E>) -> ObjectStoreError
where
    E: std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
{
    match status_of(error) {
        // A permission or precondition refusal is the bucket policy that makes a published
        // prefix immutable; it is not a transient store fault, but it is also not something a
        // different attempt prefix cannot get past, so it stays a write failure.
        Some(403) | Some(409) | Some(412) => ObjectStoreError::WriteFailed(describe(context, error)),
        _ => ObjectStoreError::Unavailable(describe(context, error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const PREFIX: &str = "datasets/ds-1/versions/iv-1/attempt-1/";
    const DIGEST: &str = "89e4e0a61728e9776376f7550d09426acba14bd486c68a918e66fb11d437d7de";

    fn lookup(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    /// The canonical form drops the surrounding quotes and nothing else. The `-N` multipart
    /// suffix in particular survives, because it is part of the identity the verifier compares.
    #[test]
    fn canonicalises_etags_by_removing_only_the_surrounding_quotes() {
        assert_eq!(
            canonical_etag(Some("\"d41d8cd98f00b204e9800998ecf8427e\"")).unwrap(),
            "d41d8cd98f00b204e9800998ecf8427e"
        );
        assert_eq!(
            canonical_etag(Some("\"d41d8cd98f00b204e9800998ecf8427e-3\"")).unwrap(),
            "d41d8cd98f00b204e9800998ecf8427e-3",
            "a multipart suffix must survive canonicalisation"
        );
        assert_eq!(
            canonical_etag(Some("d41d8cd98f00b204e9800998ecf8427e")).unwrap(),
            "d41d8cd98f00b204e9800998ecf8427e",
            "an already-canonical value is returned unchanged"
        );
        assert_eq!(canonical_etag(None), None);
        assert_eq!(canonical_etag(Some("")), None);
        assert_eq!(canonical_etag(Some("\"\"")), None);
    }

    /// `If-Match` round-trips back to the quoted wire form without touching the canonical value.
    #[test]
    fn re_quotes_a_canonical_etag_for_the_if_match_header() {
        let quoted = http_entity_tag("d41d8cd98f00b204e9800998ecf8427e-3");
        assert_eq!(quoted, "\"d41d8cd98f00b204e9800998ecf8427e-3\"");
        assert_eq!(
            canonical_etag(Some(&quoted)).unwrap(),
            "d41d8cd98f00b204e9800998ecf8427e-3"
        );
    }

    #[test]
    fn accepts_only_keys_strictly_below_the_attempt_prefix() {
        assert!(validate_destination(PREFIX, &format!("{PREFIX}variants/chrom=1/part-000.parquet")).is_ok());
        assert!(validate_destination(PREFIX, &format!("{PREFIX}variants/chrom=X/part-999.parquet")).is_ok());

        for rejected in [
            "datasets/ds-1/versions/iv-1/attempt-2/variants/chrom=1/part-000.parquet",
            "datasets/ds-2/versions/iv-1/attempt-1/variants/chrom=1/part-000.parquet",
            "datasets/ds-1/manifest.json",
            "datasets/ds-1/versions/iv-1/attempt-1/../attempt-2/part-000.parquet",
            "datasets/ds-1/versions/iv-1/attempt-1/./part-000.parquet",
            "datasets/ds-1/versions/iv-1/attempt-1//part-000.parquet",
            "datasets/ds-1/versions/iv-1/attempt-1/a\\b.parquet",
            "datasets/ds-1/versions/iv-1/attempt-1/a\nb.parquet",
            PREFIX,
            "",
        ] {
            let Err(error) = validate_destination(PREFIX, rejected) else {
                panic!("'{rejected}' must not be accepted as a destination");
            };
            assert_eq!(
                error.failure_type(),
                FailureType::ArtifactValidationFailed,
                "'{rejected}' must be a deterministic refusal"
            );
            assert!(!error.failure_type().is_retryable());
        }
    }

    /// `validate_destination` must refuse a key that never entered the `variants/` segment at
    /// all, matching the TypeScript verifier's `KEY_OUTSIDE_ALLOWED_PREFIX` — defence in depth,
    /// since a bug that dropped the segment would otherwise only surface at publish time.
    #[test]
    fn refuses_a_destination_key_that_omits_the_variants_segment() {
        let key = format!("{PREFIX}part-000.parquet");
        let error = validate_destination(PREFIX, &key)
            .expect_err("a key that never enters 'variants/' must be refused");
        assert_eq!(error.failure_type(), FailureType::ArtifactValidationFailed);
        assert!(!error.failure_type().is_retryable());
    }

    #[test]
    fn refuses_an_attempt_prefix_that_is_not_a_plain_directory_path() {
        for prefix in ["", "no-trailing-slash", "a/../b/", "a//b/", "/"] {
            assert!(
                validate_destination(prefix, &format!("{prefix}x.parquet")).is_err(),
                "'{prefix}' must not be usable as an attempt prefix"
            );
        }
    }

    #[test]
    fn accepts_only_a_lowercase_hex_sha256_as_the_published_digest() {
        assert!(validate_checksum(DIGEST, "k").is_ok());
        for rejected in [
            "",
            "deadbeef",
            &DIGEST.to_ascii_uppercase(),
            &DIGEST[..63],
            &format!("{DIGEST}0"),
            &DIGEST.replace('a', "g"),
        ] {
            let error = validate_checksum(rejected, "k").expect_err("must be refused");
            assert_eq!(error.failure_type(), FailureType::ArtifactValidationFailed);
        }
    }

    /// The identity check must fail on any of the three recorded facts, and always as the
    /// non-retryable `SourceObjectChanged`.
    #[test]
    fn treats_any_change_to_the_recorded_source_identity_as_source_object_changed() {
        let source = SourceObject {
            bucket: "genomic-data".to_string(),
            key: "samples/demo_user.vcf".to_string(),
            etag: "abc123".to_string(),
            version_id: Some("v1".to_string()),
            content_length: 1024,
        };
        let matching = ObservedSource {
            etag: "abc123".to_string(),
            version_id: Some("v1".to_string()),
            content_length: 1024,
        };
        assert!(assert_source_identity(&source, &matching, "before").is_ok());

        let mut wrong_etag = matching.clone();
        wrong_etag.etag = "def456".to_string();
        let mut wrong_version = matching.clone();
        wrong_version.version_id = Some("v2".to_string());
        let mut no_version = matching.clone();
        no_version.version_id = None;
        let mut wrong_size = matching.clone();
        wrong_size.content_length = 2048;
        let mut no_size = matching.clone();
        no_size.content_length = -1;

        for observed in [wrong_etag, wrong_version, no_version, wrong_size, no_size] {
            let error = assert_source_identity(&source, &observed, "before")
                .expect_err("a changed identity must be refused");
            assert_eq!(error.failure_type(), FailureType::SourceObjectChanged);
            assert!(!error.failure_type().is_retryable());
        }
    }

    /// A catalog entry with no version ID must not be invalidated by a bucket that reports one.
    #[test]
    fn ignores_an_observed_version_id_when_the_catalog_recorded_none() {
        let source = SourceObject {
            bucket: "genomic-data".to_string(),
            key: "samples/demo_user.vcf".to_string(),
            etag: "abc123".to_string(),
            version_id: None,
            content_length: 1024,
        };
        let observed = ObservedSource {
            etag: "abc123".to_string(),
            version_id: Some("v9".to_string()),
            content_length: 1024,
        };
        assert!(assert_source_identity(&source, &observed, "before").is_ok());
    }

    #[test]
    fn reads_the_same_environment_variables_as_the_typescript_adapter() {
        let vars = lookup(&[
            ("S3_ENDPOINT", "http://localhost:9000"),
            ("S3_ACCESS_KEY", "admin"),
            ("S3_SECRET_KEY", "password123"),
        ]);
        let config = ObjectStoreConfig::from_lookup(|name| vars.get(name).cloned()).unwrap();
        assert_eq!(config.endpoint, "http://localhost:9000");
        assert_eq!(config.region, DEFAULT_REGION);
        assert!(config.force_path_style, "MinIO addresses buckets by path");

        let explicit = lookup(&[
            ("S3_ENDPOINT", "https://s3.example.com"),
            ("S3_ACCESS_KEY", "admin"),
            ("S3_SECRET_KEY", "password123"),
            ("S3_REGION", "eu-west-1"),
            ("S3_FORCE_PATH_STYLE", "false"),
        ]);
        let config = ObjectStoreConfig::from_lookup(|name| explicit.get(name).cloned()).unwrap();
        assert_eq!(config.region, "eu-west-1");
        assert!(!config.force_path_style);
    }

    /// An `s3://` value is refused rather than rewritten into a public HTTP endpoint.
    #[test]
    fn refuses_an_endpoint_that_is_not_an_absolute_http_url() {
        for endpoint in ["s3://genomic-data", "localhost:9000", "ftp://x", ""] {
            let vars = lookup(&[
                ("S3_ENDPOINT", endpoint),
                ("S3_ACCESS_KEY", "admin"),
                ("S3_SECRET_KEY", "password123"),
            ]);
            let error = ObjectStoreConfig::from_lookup(|name| vars.get(name).cloned())
                .expect_err("must be refused");
            assert_eq!(error.failure_type(), FailureType::ObjectStoreUnavailable);
        }
    }

    #[test]
    fn refuses_incomplete_credentials_rather_than_discovering_them() {
        for missing in ["S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY"] {
            let vars: HashMap<String, String> = lookup(&[
                ("S3_ENDPOINT", "http://localhost:9000"),
                ("S3_ACCESS_KEY", "admin"),
                ("S3_SECRET_KEY", "password123"),
            ])
            .into_iter()
            .filter(|(name, _)| name != missing)
            .collect();
            assert!(
                ObjectStoreConfig::from_lookup(|name| vars.get(name).cloned()).is_err(),
                "a missing {missing} must not be silently defaulted"
            );
        }

        let bad = lookup(&[
            ("S3_ENDPOINT", "http://localhost:9000"),
            ("S3_ACCESS_KEY", "admin"),
            ("S3_SECRET_KEY", "password123"),
            ("S3_FORCE_PATH_STYLE", "yes"),
        ]);
        assert!(ObjectStoreConfig::from_lookup(|name| bad.get(name).cloned()).is_err());
    }

    /// The secret must never reach a log line through a derived `Debug`.
    #[test]
    fn redacts_the_secret_key_from_debug_output() {
        let config = ObjectStoreConfig {
            endpoint: "http://localhost:9000".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: "admin".to_string(),
            secret_access_key: "password123".to_string(),
            force_path_style: true,
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("password123"), "{rendered}");
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn maps_every_variant_onto_the_frozen_failure_taxonomy() {
        let cases = [
            (
                ObjectStoreError::Configuration("x".into()),
                FailureType::ObjectStoreUnavailable,
            ),
            (
                ObjectStoreError::SourceChanged("x".into()),
                FailureType::SourceObjectChanged,
            ),
            (
                ObjectStoreError::Unavailable("x".into()),
                FailureType::ObjectStoreUnavailable,
            ),
            (
                ObjectStoreError::WriteFailed("x".into()),
                FailureType::ArtifactWriteFailed,
            ),
            (
                ObjectStoreError::ValidationFailed("x".into()),
                FailureType::ArtifactValidationFailed,
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(error.failure_type(), expected);
        }
    }

    /// The metadata entry name is frozen; the TypeScript verifier reads exactly this key.
    #[test]
    fn publishes_the_checksum_under_the_frozen_metadata_name() {
        assert_eq!(CHECKSUM_METADATA_KEY, "sha256");
    }
}
