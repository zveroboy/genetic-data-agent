//! Frozen `ingestion-v1` wire contracts.
//!
//! This module mirrors `ts-api-agent/src/application/ingestion-contracts.ts` field for
//! field. It is a pure serde layer: no Temporal, no S3, no DuckDB. Rules:
//!
//! - JSON-compatible primitives only, camelCase field names on the wire.
//! - Every wire struct is `deny_unknown_fields`, so an unexpected key is a hard error
//!   rather than a silently dropped field.
//! - Failure names are stable strings: the TypeScript workflow matches them by value in
//!   its non-retryable error list.
//!
//! The golden fixtures under `contracts/fixtures/` are the shared source of truth and are
//! parsed by the tests in both languages. See `contracts/ingestion-v1.md`.

use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

/// Wire contract version carried by every activity input.
pub const CONTRACT_VERSION: u32 = 1;

/// Deserializes a nullable-but-**required** wire field.
///
/// A bare `Option<T>` field is not enough: serde applies an implicit `Default` to a missing
/// `Option`, so an absent key silently becomes `None` even under `deny_unknown_fields`.
/// TypeScript spells these fields `.nullable()` and not `.optional()`, so the key must be
/// present and may only be `null`. Attaching `deserialize_with` suppresses serde's implicit
/// default, which makes an omitted key a `missing field` error.
fn required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// The `contractVersion` envelope field, which only ever holds [`CONTRACT_VERSION`].
///
/// TypeScript declares it `z.literal(1)`. Deserializing a bare `u32` here would accept `2`
/// and leave the version check to a caller that may forget it, so the rejection lives in the
/// deserializer where it cannot be bypassed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ContractVersion(u32);

impl ContractVersion {
    /// The only value that can exist: [`CONTRACT_VERSION`].
    pub const CURRENT: Self = Self(CONTRACT_VERSION);

    pub fn get(self) -> u32 {
        self.0
    }
}

impl<'de> Deserialize<'de> for ContractVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u32::deserialize(deserializer)?;
        if value != CONTRACT_VERSION {
            return Err(serde::de::Error::custom(format!(
                "unsupported contractVersion {value}; this worker implements {CONTRACT_VERSION}"
            )));
        }
        Ok(Self(value))
    }
}

impl fmt::Display for ContractVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Physical artifact layout version (prefix shape, partition directories).
pub const LAYOUT_VERSION: u32 = 1;

/// Key segment separating an attempt prefix from its partition directories:
/// `{attemptPrefix}variants/{relativePath}`.
///
/// It is part of the S3 key only. `relativePath` — the unit the dataset checksum is computed
/// from — is `chrom=<value>/part-NNN.parquet` and never carries this segment, so the
/// checksum stays computable from the processor's local Parquet descriptors alone.
pub const VARIANTS_SEGMENT: &str = "variants/";

/// Logical Parquet column set version.
pub const SCHEMA_VERSION: u32 = 1;

/// Parquet is partitioned by chromosome.
pub const PARTITION_SPEC: [&str; 1] = ["chrom"];

/// Rows are globally ordered by these columns; `chrom` comes from the partition directory.
pub const SORT_ORDER: [&str; 4] = ["chrom", "pos", "ref", "alt"];

/// Canonical description of the physical Parquet file schema. `chrom` is encoded by the
/// `chrom=<value>` directory and is not a physical column.
pub const PARQUET_SCHEMA_COLUMNS: &str =
    "pos:UINTEGER:NOT NULL;rsid:VARCHAR:NULL;ref:VARCHAR:NOT NULL;alt:VARCHAR:NOT NULL;gt_raw:VARCHAR:NOT NULL";

/// SHA-256 of [`PARQUET_SCHEMA_COLUMNS`]; recorded in every published manifest.
pub const PARQUET_SCHEMA_FINGERPRINT: &str =
    "89e4e0a61728e9776376f7550d09426acba14bd486c68a918e66fb11d437d7de";

/// The only datasets that may be ingested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetKey {
    #[serde(rename = "demo-small")]
    DemoSmall,
    #[serde(rename = "na12878-full")]
    Na12878Full,
}

impl DatasetKey {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DemoSmall => "demo-small",
            Self::Na12878Full => "na12878-full",
        }
    }
}

impl fmt::Display for DatasetKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Published artifact format. Only one is supported at layout version 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactFormat {
    #[serde(rename = "parquet-dataset")]
    ParquetDataset,
}

/// Identity of a single S3 object. `SourceObject` and `ParquetObject` repeat these fields
/// rather than flattening this struct, because `serde(flatten)` is incompatible with
/// `deny_unknown_fields`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct S3ObjectRef {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    #[serde(deserialize_with = "required_nullable")]
    pub version_id: Option<String>,
}

/// The source VCF object, resolved from the catalog before the activity is scheduled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceObject {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    #[serde(deserialize_with = "required_nullable")]
    pub version_id: Option<String>,
    pub content_length: u64,
}

/// The reference genome build and versioned annotation snapshot the dataset is pinned to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReferenceSelector {
    pub build: String,
    pub version: String,
}

/// The immutable destination. The activity may only write below `allowed_prefix`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArtifactTarget {
    pub bucket: String,
    pub artifact_version: String,
    pub allowed_prefix: String,
}

/// Input of the `buildDatasetArtifact` activity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildDatasetArtifactInput {
    pub contract_version: ContractVersion,
    pub dataset_id: String,
    pub dataset_key: DatasetKey,
    pub source: SourceObject,
    pub reference: ReferenceSelector,
    pub target: ArtifactTarget,
}

/// One uploaded Parquet object plus the statistics used for partition and row-group pruning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ParquetObject {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    #[serde(deserialize_with = "required_nullable")]
    pub version_id: Option<String>,
    pub chrom: String,
    pub checksum_sha256: String,
    pub byte_size: u64,
    pub row_count: u64,
    pub min_pos: u32,
    pub max_pos: u32,
}

/// Result of the `buildDatasetArtifact` activity. The activity never writes the manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildDatasetArtifactResult {
    pub attempt_prefix: String,
    pub dataset_checksum_sha256: String,
    pub variant_count: u64,
    pub rejected_record_count: u64,
    pub reference_build: String,
    pub processor_version: String,
    pub parquet_objects: Vec<ParquetObject>,
}

/// The published `manifest.json`. Written last, by TypeScript, once every declared object
/// has been verified. A dataset is queryable only when its manifest exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatasetManifest {
    pub dataset_id: String,
    pub artifact_format: ArtifactFormat,
    pub layout_version: u32,
    pub schema_version: u32,
    pub schema_fingerprint: String,
    pub artifact_version: String,
    pub reference_version: String,
    pub partition_spec: Vec<String>,
    pub sort_order: Vec<String>,
    pub attempt_prefix: String,
    pub dataset_checksum_sha256: String,
    pub variant_count: u64,
    pub rejected_record_count: u64,
    pub reference_build: String,
    pub processor_version: String,
    pub parquet_objects: Vec<ParquetObject>,
}

/// Activity progress phases, in the order the worker reports them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IngestionPhase {
    #[serde(rename = "DOWNLOADING_SOURCE")]
    DownloadingSource,
    #[serde(rename = "PARSING")]
    Parsing,
    #[serde(rename = "WRITING_DUCKDB")]
    WritingDuckdb,
    #[serde(rename = "EXPORTING_PARQUET")]
    ExportingParquet,
    #[serde(rename = "UPLOADING_PARTITION")]
    UploadingPartition,
    #[serde(rename = "FINALIZING")]
    Finalizing,
}

/// Heartbeat detail published by the activity at every progress boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IngestionHeartbeat {
    pub phase: IngestionPhase,
    pub processed_bytes: u64,
    pub processed_variants: u64,
    #[serde(deserialize_with = "required_nullable")]
    pub current_partition: Option<String>,
    pub completed_files: u64,
    pub uploaded_bytes: u64,
}

/// Stable failure type names. These strings are the Temporal application failure types the
/// TypeScript workflow matches by value; changing a spelling changes retry behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureType {
    InvalidVcfFormat,
    SourceObjectChanged,
    ObjectStoreUnavailable,
    ArtifactWriteFailed,
    ArtifactValidationFailed,
}

impl FailureType {
    pub const ALL: [FailureType; 5] = [
        Self::InvalidVcfFormat,
        Self::SourceObjectChanged,
        Self::ObjectStoreUnavailable,
        Self::ArtifactWriteFailed,
        Self::ArtifactValidationFailed,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidVcfFormat => "InvalidVcfFormat",
            Self::SourceObjectChanged => "SourceObjectChanged",
            Self::ObjectStoreUnavailable => "ObjectStoreUnavailable",
            Self::ArtifactWriteFailed => "ArtifactWriteFailed",
            Self::ArtifactValidationFailed => "ArtifactValidationFailed",
        }
    }

    /// Deterministic failures cannot succeed on retry and must be raised as non-retryable.
    pub fn is_retryable(self) -> bool {
        match self {
            Self::InvalidVcfFormat
            | Self::SourceObjectChanged
            | Self::ArtifactValidationFailed => false,
            Self::ObjectStoreUnavailable | Self::ArtifactWriteFailed => true,
        }
    }
}

impl fmt::Display for FailureType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use sha2::{Digest, Sha256};
    use std::path::PathBuf;

    /// Golden fixtures live at the repository root and are read verbatim by both languages.
    fn fixture(name: &str) -> String {
        let path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "..", "contracts", "fixtures", name]
            .iter()
            .collect();
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
    }

    fn fixture_value(name: &str) -> Value {
        serde_json::from_str(&fixture(name)).expect("fixture is valid JSON")
    }

    const INPUT: &str = "build-dataset-artifact.input.json";
    const RESULT: &str = "build-dataset-artifact.result.json";
    const MANIFEST: &str = "dataset-manifest.json";

    #[test]
    fn deserializes_the_golden_build_input() {
        let input: BuildDatasetArtifactInput =
            serde_json::from_str(&fixture(INPUT)).expect("golden input deserializes");

        assert_eq!(input.contract_version, ContractVersion::CURRENT);
        assert_eq!(input.contract_version.get(), CONTRACT_VERSION);
        assert_eq!(input.dataset_id, "ds-test-001");
        assert_eq!(input.dataset_key, DatasetKey::DemoSmall);
        assert_eq!(input.source.bucket, "genomic-data");
        assert_eq!(input.source.key, "samples/demo_user.vcf");
        assert_eq!(input.source.etag, "fixture-etag");
        assert_eq!(input.source.version_id, None);
        assert_eq!(input.source.content_length, 1024);
        assert_eq!(input.reference.build, "GRCh38");
        assert_eq!(input.reference.version, "demo-clinvar-grch38-v3");
        assert_eq!(input.target.bucket, "genomic-artifacts");
        assert_eq!(input.target.artifact_version, "iv-test-001");
        assert_eq!(
            input.target.allowed_prefix,
            "datasets/ds-test-001/versions/iv-test-001/"
        );
    }

    #[test]
    fn deserializes_the_golden_build_result() {
        let result: BuildDatasetArtifactResult =
            serde_json::from_str(&fixture(RESULT)).expect("golden result deserializes");

        assert_eq!(
            result.attempt_prefix,
            "datasets/ds-test-001/versions/iv-test-001/attempt-1/"
        );
        assert_eq!(result.variant_count, 1500);
        assert_eq!(result.rejected_record_count, 2);
        assert_eq!(result.reference_build, "GRCh38");
        assert_eq!(result.processor_version, "rust-ingestion-worker/0.1.0");
        assert_eq!(result.parquet_objects.len(), 2);

        // The frozen layout: {attemptPrefix}variants/chrom=<value>/part-NNN.parquet.
        for object in &result.parquet_objects {
            let variants_prefix = format!("{}{VARIANTS_SEGMENT}", result.attempt_prefix);
            let relative_path = object
                .key
                .strip_prefix(&variants_prefix)
                .unwrap_or_else(|| panic!("key '{}' must sit under '{variants_prefix}'", object.key));
            assert_eq!(
                relative_path,
                format!("chrom={}/part-000.parquet", object.chrom),
                "relativePath must not carry the variants/ segment"
            );
        }

        let first = &result.parquet_objects[0];
        assert_eq!(first.chrom, "1");
        assert_eq!(first.version_id, None);
        assert_eq!(first.row_count, 900);
        assert_eq!(first.min_pos, 12345);
        assert_eq!(first.max_pos, 248_900_000);

        let second = &result.parquet_objects[1];
        assert_eq!(second.chrom, "12");
        assert_eq!(
            second.version_id.as_deref(),
            Some("fixture-version-chrom-12")
        );
        assert_eq!(second.byte_size, 15360);
    }

    /// The fingerprint is pinned as a literal in both languages so workflow code can compare
    /// it without hashing. Nothing tied the two constants together until this test: a change
    /// to the column list that left the literal alone would have gone unnoticed.
    #[test]
    fn parquet_schema_fingerprint_is_the_sha256_of_the_column_description() {
        let mut hasher = Sha256::new();
        hasher.update(PARQUET_SCHEMA_COLUMNS.as_bytes());
        assert_eq!(hex::encode(hasher.finalize()), PARQUET_SCHEMA_FINGERPRINT);

        // `chrom` is a partition directory, never a physical column.
        assert!(!PARQUET_SCHEMA_COLUMNS.contains("chrom"));
        assert_eq!(
            PARQUET_SCHEMA_COLUMNS.split(';').count(),
            SORT_ORDER.len() + 1,
            "pos, rsid, ref, alt, gt_raw"
        );
    }

    #[test]
    fn deserializes_the_golden_manifest() {
        let manifest: DatasetManifest =
            serde_json::from_str(&fixture(MANIFEST)).expect("golden manifest deserializes");

        assert_eq!(manifest.dataset_id, "ds-test-001");
        assert_eq!(manifest.artifact_format, ArtifactFormat::ParquetDataset);
        assert_eq!(manifest.layout_version, LAYOUT_VERSION);
        assert_eq!(manifest.schema_version, SCHEMA_VERSION);
        assert_eq!(manifest.schema_fingerprint, PARQUET_SCHEMA_FINGERPRINT);
        assert_eq!(manifest.artifact_version, "iv-test-001");
        assert_eq!(manifest.reference_build, "GRCh38");
        assert_eq!(manifest.reference_version, "demo-clinvar-grch38-v3");
        assert_eq!(manifest.partition_spec, PARTITION_SPEC);
        assert_eq!(manifest.sort_order, SORT_ORDER);
    }

    #[test]
    fn round_trips_every_golden_fixture_without_renaming_a_field() {
        let input: BuildDatasetArtifactInput = serde_json::from_str(&fixture(INPUT)).unwrap();
        assert_eq!(serde_json::to_value(&input).unwrap(), fixture_value(INPUT));

        let result: BuildDatasetArtifactResult = serde_json::from_str(&fixture(RESULT)).unwrap();
        assert_eq!(serde_json::to_value(&result).unwrap(), fixture_value(RESULT));

        let manifest: DatasetManifest = serde_json::from_str(&fixture(MANIFEST)).unwrap();
        assert_eq!(
            serde_json::to_value(&manifest).unwrap(),
            fixture_value(MANIFEST)
        );
    }

    #[test]
    fn publishes_the_build_result_inventory_unchanged_into_the_manifest() {
        let result: BuildDatasetArtifactResult = serde_json::from_str(&fixture(RESULT)).unwrap();
        let manifest: DatasetManifest = serde_json::from_str(&fixture(MANIFEST)).unwrap();

        assert_eq!(manifest.parquet_objects, result.parquet_objects);
        assert_eq!(manifest.attempt_prefix, result.attempt_prefix);
        assert_eq!(
            manifest.dataset_checksum_sha256,
            result.dataset_checksum_sha256
        );
        assert_eq!(manifest.variant_count, result.variant_count);
        assert_eq!(manifest.rejected_record_count, result.rejected_record_count);
        assert_eq!(manifest.reference_build, result.reference_build);
        assert_eq!(manifest.processor_version, result.processor_version);
    }

    #[test]
    fn rejects_unknown_wire_fields() {
        let mut input = fixture_value(INPUT);
        input["localPath"] = json!("/tmp/evil.vcf");
        assert!(serde_json::from_value::<BuildDatasetArtifactInput>(input).is_err());

        let mut nested = fixture_value(INPUT);
        nested["source"]["url"] = json!("https://attacker.example/file.vcf");
        assert!(serde_json::from_value::<BuildDatasetArtifactInput>(nested).is_err());

        let mut result = fixture_value(RESULT);
        result["parquetObjects"][0]["uri"] = json!("s3://genomic-artifacts/anything.parquet");
        assert!(serde_json::from_value::<BuildDatasetArtifactResult>(result).is_err());

        let mut manifest = fixture_value(MANIFEST);
        manifest["publishedAt"] = json!("2026-08-02T00:00:00Z");
        assert!(serde_json::from_value::<DatasetManifest>(manifest).is_err());
    }

    #[test]
    fn s3_object_ref_uses_the_same_names_as_the_composed_payloads() {
        let payload = json!({
            "bucket": "genomic-artifacts",
            "key": "datasets/ds-test-001/manifest.json",
            "etag": "fixture-manifest-etag",
            "versionId": null
        });

        let reference: S3ObjectRef = serde_json::from_value(payload.clone()).unwrap();
        assert_eq!(reference.version_id, None);
        assert_eq!(serde_json::to_value(&reference).unwrap(), payload);

        let mut extra = payload;
        extra["contentLength"] = json!(1024);
        assert!(serde_json::from_value::<S3ObjectRef>(extra).is_err());
    }

    /// A nullable wire field must be *present*. TypeScript uses `.nullable()`, not
    /// `.optional()`; serde's implicit missing-`Option` shortcut would silently accept a
    /// producer that drops the key, so every nullable field opts out of it.
    #[test]
    fn requires_nullable_fields_to_be_present() {
        // S3ObjectRef
        let mut without = json!({"bucket": "b", "key": "k", "etag": "e"});
        assert!(
            serde_json::from_value::<S3ObjectRef>(without.clone()).is_err(),
            "S3ObjectRef must reject an omitted versionId"
        );
        without["versionId"] = Value::Null;
        assert_eq!(
            serde_json::from_value::<S3ObjectRef>(without).unwrap().version_id,
            None
        );

        // SourceObject
        let mut source = fixture_value(INPUT)["source"].clone();
        source.as_object_mut().unwrap().remove("versionId");
        assert!(
            serde_json::from_value::<SourceObject>(source).is_err(),
            "SourceObject must reject an omitted versionId"
        );
        let mut input = fixture_value(INPUT);
        input["source"].as_object_mut().unwrap().remove("versionId");
        assert!(
            serde_json::from_value::<BuildDatasetArtifactInput>(input).is_err(),
            "BuildDatasetArtifactInput must reject a source with an omitted versionId"
        );

        // ParquetObject
        let mut object = fixture_value(RESULT)["parquetObjects"][0].clone();
        object.as_object_mut().unwrap().remove("versionId");
        assert!(
            serde_json::from_value::<ParquetObject>(object).is_err(),
            "ParquetObject must reject an omitted versionId"
        );

        // IngestionHeartbeat
        let heartbeat = json!({
            "phase": "PARSING",
            "processedBytes": 4096,
            "processedVariants": 2500,
            "completedFiles": 3,
            "uploadedBytes": 1048576
        });
        assert!(
            serde_json::from_value::<IngestionHeartbeat>(heartbeat).is_err(),
            "IngestionHeartbeat must reject an omitted currentPartition"
        );
    }

    #[test]
    fn rejects_an_unsupported_contract_version() {
        for version in [0u64, 2, 4_294_967_295] {
            let mut input = fixture_value(INPUT);
            input["contractVersion"] = json!(version);
            assert!(
                serde_json::from_value::<BuildDatasetArtifactInput>(input).is_err(),
                "contractVersion {version} must be rejected"
            );
        }
    }

    #[test]
    fn rejects_snake_case_field_names() {
        let mut input = fixture_value(INPUT);
        let object = input.as_object_mut().unwrap();
        let dataset_key = object.remove("datasetKey").unwrap();
        object.insert("dataset_key".to_string(), dataset_key);
        assert!(serde_json::from_value::<BuildDatasetArtifactInput>(input).is_err());
    }

    #[test]
    fn rejects_an_unknown_dataset_key() {
        for key in ["na12878", "s3://attacker/file.vcf", "demo_small", ""] {
            let mut input = fixture_value(INPUT);
            input["datasetKey"] = json!(key);
            assert!(
                serde_json::from_value::<BuildDatasetArtifactInput>(input).is_err(),
                "dataset key '{key}' must be rejected"
            );
        }
    }

    #[test]
    fn dataset_key_variants_use_the_typescript_strings() {
        assert_eq!(
            serde_json::to_string(&DatasetKey::DemoSmall).unwrap(),
            "\"demo-small\""
        );
        assert_eq!(
            serde_json::to_string(&DatasetKey::Na12878Full).unwrap(),
            "\"na12878-full\""
        );
        assert_eq!(
            serde_json::from_str::<DatasetKey>("\"demo-small\"").unwrap(),
            DatasetKey::DemoSmall
        );
        assert_eq!(
            serde_json::from_str::<DatasetKey>("\"na12878-full\"").unwrap(),
            DatasetKey::Na12878Full
        );
        assert_eq!(DatasetKey::DemoSmall.as_str(), "demo-small");
        assert_eq!(DatasetKey::Na12878Full.as_str(), "na12878-full");
    }

    #[test]
    fn artifact_format_uses_the_typescript_string() {
        assert_eq!(
            serde_json::to_string(&ArtifactFormat::ParquetDataset).unwrap(),
            "\"parquet-dataset\""
        );
    }

    #[test]
    fn failure_type_names_are_stable() {
        let expected = [
            (FailureType::InvalidVcfFormat, "InvalidVcfFormat"),
            (FailureType::SourceObjectChanged, "SourceObjectChanged"),
            (FailureType::ObjectStoreUnavailable, "ObjectStoreUnavailable"),
            (FailureType::ArtifactWriteFailed, "ArtifactWriteFailed"),
            (
                FailureType::ArtifactValidationFailed,
                "ArtifactValidationFailed",
            ),
        ];

        for (failure, name) in expected {
            assert_eq!(failure.as_str(), name);
            assert_eq!(failure.to_string(), name);
            assert_eq!(serde_json::to_string(&failure).unwrap(), format!("\"{name}\""));
        }

        assert_eq!(
            FailureType::ALL.map(|failure| failure.as_str()),
            expected.map(|(_, name)| name)
        );
    }

    #[test]
    fn non_retryable_failures_are_the_deterministic_ones() {
        assert!(!FailureType::InvalidVcfFormat.is_retryable());
        assert!(!FailureType::SourceObjectChanged.is_retryable());
        assert!(!FailureType::ArtifactValidationFailed.is_retryable());
        assert!(FailureType::ObjectStoreUnavailable.is_retryable());
        assert!(FailureType::ArtifactWriteFailed.is_retryable());
    }

    #[test]
    fn ingestion_heartbeat_matches_the_frozen_payload() {
        let payload = json!({
            "phase": "PARSING",
            "processedBytes": 4096,
            "processedVariants": 2500,
            "currentPartition": "12",
            "completedFiles": 3,
            "uploadedBytes": 1048576
        });

        let heartbeat: IngestionHeartbeat = serde_json::from_value(payload.clone()).unwrap();
        assert_eq!(heartbeat.phase, IngestionPhase::Parsing);
        assert_eq!(heartbeat.current_partition.as_deref(), Some("12"));
        assert_eq!(serde_json::to_value(&heartbeat).unwrap(), payload);

        let mut without_partition = payload.clone();
        without_partition["phase"] = json!("DOWNLOADING_SOURCE");
        without_partition["currentPartition"] = Value::Null;
        let downloading: IngestionHeartbeat =
            serde_json::from_value(without_partition.clone()).unwrap();
        assert_eq!(downloading.current_partition, None);
        assert_eq!(
            serde_json::to_value(&downloading).unwrap(),
            without_partition
        );

        let mut unknown_phase = payload.clone();
        unknown_phase["phase"] = json!("DELETING");
        assert!(serde_json::from_value::<IngestionHeartbeat>(unknown_phase).is_err());

        let mut extra = payload;
        extra["stackTrace"] = json!("boom");
        assert!(serde_json::from_value::<IngestionHeartbeat>(extra).is_err());
    }

    #[test]
    fn ingestion_phases_use_the_typescript_strings() {
        let expected = [
            (IngestionPhase::DownloadingSource, "DOWNLOADING_SOURCE"),
            (IngestionPhase::Parsing, "PARSING"),
            (IngestionPhase::WritingDuckdb, "WRITING_DUCKDB"),
            (IngestionPhase::ExportingParquet, "EXPORTING_PARQUET"),
            (IngestionPhase::UploadingPartition, "UPLOADING_PARTITION"),
            (IngestionPhase::Finalizing, "FINALIZING"),
        ];

        for (phase, name) in expected {
            assert_eq!(
                serde_json::to_string(&phase).unwrap(),
                format!("\"{name}\"")
            );
        }
    }
}
