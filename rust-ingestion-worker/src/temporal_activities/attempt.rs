//! Attempt scoping: the two namespaces one Activity attempt owns and may write to, derived from
//! the identifiers rather than taken from the wire.
//!
//! - **In S3**, the immutable prefix `datasets/{id}/versions/{v}/attempt-{n}/`, so no retry can
//!   append to a previous attempt's output.
//! - **On disk**, an [`AttemptWorkspace`] below the staging root, created fresh and removed on
//!   every exit path.
//!
//! Both are built from caller-influenced strings, so both are validated before they become a
//! path: [`is_safe_path_segment`] gates the S3 side and [`sanitize_identifier`] the local one.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::IngestionFailure;
use crate::contracts::{BuildDatasetArtifactInput, FailureType};

/// The single immutable prefix a dataset's artifact version may be written under.
///
/// Derived from the identifiers, never taken from the wire: `target.allowedPrefix` arrives as
/// data, and a widened value such as `datasets/` would satisfy every containment check below.
/// Mirrors `allowedPrefixFor` in `ingestion-contracts.ts`.
pub fn derived_allowed_prefix(dataset_id: &str, artifact_version: &str) -> String {
    format!("datasets/{dataset_id}/versions/{artifact_version}/")
}

/// The prefix one Activity attempt owns, strictly below `allowed_prefix`.
pub fn attempt_prefix_for(allowed_prefix: &str, attempt: u32) -> String {
    format!("{allowed_prefix}attempt-{attempt}/")
}

/// Derives and re-checks this attempt's writable prefix.
///
/// Two refusals, both deterministic:
///
/// 1. `datasetId` and `artifactVersion` must be single safe path segments — they flow verbatim
///    into the prefix, and `..` or a `/` would climb straight out of the dataset's namespace.
/// 2. The declared `target.allowedPrefix` must be exactly the derived one
///    (`ALLOWED_PREFIX_MISMATCH` on the TypeScript side). This is the check that gates: it is
///    what refuses a widened `allowedPrefix` such as `datasets/`, which would otherwise satisfy
///    every containment check downstream.
///
/// What TypeScript enforces as `ATTEMPT_PREFIX_OUTSIDE_ALLOWED_PREFIX` — the attempt prefix
/// sitting strictly below the allowed prefix — holds here *by construction* rather than by
/// check: [`attempt_prefix_for`] extends the value rule 2 just proved correct. A runtime
/// re-check of `prefix.starts_with(&allowed)` on a string built as `format!("{allowed}…")` can
/// never fail, so there is none; the containment is asserted in the tests against the two
/// functions together.
pub fn validated_attempt_prefix(
    input: &BuildDatasetArtifactInput,
    attempt: u32,
) -> Result<String, IngestionFailure> {
    if !is_safe_path_segment(&input.dataset_id) {
        return Err(IngestionFailure::validation(format!(
            "dataset id '{}' is not a single safe path segment",
            input.dataset_id
        )));
    }
    if !is_safe_path_segment(&input.target.artifact_version) {
        return Err(IngestionFailure::validation(format!(
            "artifact version '{}' is not a single safe path segment",
            input.target.artifact_version
        )));
    }

    let allowed = derived_allowed_prefix(&input.dataset_id, &input.target.artifact_version);
    if input.target.allowed_prefix != allowed {
        return Err(IngestionFailure::validation(format!(
            "the input declares allowed prefix '{}' but '{}'/'{}' derives '{allowed}'",
            input.target.allowed_prefix, input.dataset_id, input.target.artifact_version
        )));
    }

    Ok(attempt_prefix_for(&allowed, attempt))
}

/// `^[A-Za-z0-9][A-Za-z0-9._-]*$`, matching `pathSegmentSchema` in `ingestion-contracts.ts`.
/// `=` is excluded on purpose: it appears in a key only in a `chrom=<value>` partition
/// directory, and must not be smuggled into a dataset or version segment.
fn is_safe_path_segment(segment: &str) -> bool {
    let mut characters = segment.chars();
    let Some(first) = characters.next() else {
        return false;
    };
    first.is_ascii_alphanumeric()
        && characters.all(|character| {
            character.is_ascii_alphanumeric()
                || character == '.'
                || character == '_'
                || character == '-'
        })
}

/// A file name for one attempt's workspace, unique per (Workflow, Activity, attempt).
///
/// The Workflow ID is caller-influenced data, so it is reduced to alphanumerics, `-` and `_`
/// before it can become a directory name: a `../` in it must not be able to point the workspace
/// — and therefore the recursive cleanup — anywhere but below the staging root.
pub fn attempt_workspace_name(workflow_id: &str, activity_id: &str, attempt: u32) -> String {
    format!(
        "{}-{}-attempt-{attempt}",
        sanitize_identifier(workflow_id),
        sanitize_identifier(activity_id)
    )
}

/// Longest identifier fragment kept in a workspace name, so a pathological Workflow ID cannot
/// push the path past the filesystem's limit.
const MAX_IDENTIFIER_FRAGMENT: usize = 64;

/// Hex digits of the disambiguating digest appended to every sanitized fragment.
const IDENTIFIER_DIGEST_DIGITS: usize = 12;

/// Reduces a caller-influenced identifier to something usable as a single directory name,
/// without letting two different identifiers land on the same one.
///
/// The character mapping alone is lossy in two ways — it folds every unsafe character onto `_`,
/// and it truncates — so `ingest-ds:1` and `ingest-ds/1` would both become `ingest-ds_1`. Two
/// concurrent Activities whose Workflow IDs collided that way would race for one workspace, and
/// the loser would fail `AttemptWorkspace::create`. That failure is safe (an attempt never
/// reuses a path) but it is a retry nobody needed, so a short digest of the *untruncated raw*
/// identifier is appended and the collision disappears.
///
/// The digest is a name-shortening device, not a security boundary: the sanitized fragment in
/// front of it is what keeps the name from escaping the staging root, and it is still applied.
fn sanitize_identifier(raw: &str) -> String {
    let sanitized: String = raw
        .chars()
        .take(MAX_IDENTIFIER_FRAGMENT)
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect();
    let sanitized = if sanitized.is_empty() {
        "unnamed".to_string()
    } else {
        sanitized
    };
    let digest = hex::encode(Sha256::digest(raw.as_bytes()));
    format!("{sanitized}-{}", &digest[..IDENTIFIER_DIGEST_DIGITS])
}

/// One attempt's private scratch directory: the downloaded source, the staging database and the
/// Parquet export all live inside it, and it is removed on every exit path.
///
/// It is created with `create_dir`, never `create_dir_all`, so an existing directory is a hard
/// error rather than something to reuse. That is both the "an attempt never reuses a path" rule
/// and the guarantee that [`Drop`] can only ever delete a tree this run created.
#[derive(Debug)]
pub struct AttemptWorkspace {
    root: PathBuf,
}

impl AttemptWorkspace {
    pub fn create(staging_root: &Path, name: &str) -> Result<Self, IngestionFailure> {
        std::fs::create_dir_all(staging_root).map_err(|error| {
            IngestionFailure::new(
                FailureType::ArtifactWriteFailed,
                format!(
                    "cannot create the staging root '{}': {error}",
                    staging_root.display()
                ),
            )
        })?;

        let root = staging_root.join(name);
        std::fs::create_dir(&root).map_err(|error| {
            IngestionFailure::new(
                FailureType::ArtifactWriteFailed,
                format!(
                    "cannot create the attempt workspace '{}': {error}; an attempt never reuses a path",
                    root.display()
                ),
            )
        })?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn source_path(&self) -> PathBuf {
        self.root.join("source.vcf")
    }

    pub fn staging_db_path(&self) -> PathBuf {
        self.root.join("staging.duckdb")
    }

    pub fn parquet_dir(&self) -> PathBuf {
        self.root.join("parquet")
    }
}

impl Drop for AttemptWorkspace {
    fn drop(&mut self) {
        // Scoped to this attempt's own tree, which `create` proved did not exist beforehand.
        // Nothing in S3 is ever removed: an abandoned attempt prefix stays orphaned and
        // unqueryable, because no manifest names it.
        if let Err(error) = std::fs::remove_dir_all(&self.root) {
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    path = %self.root.display(),
                    %error,
                    "could not remove the attempt workspace"
                );
            }
        }
    }
}
