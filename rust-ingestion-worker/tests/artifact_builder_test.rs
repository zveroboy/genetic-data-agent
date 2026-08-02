//! Acceptance tests for the pure Parquet dataset processor.
//!
//! Nothing here touches Temporal or S3: the processor reads a local VCF, stages it in an
//! attempt-local DuckDB and exports a chromosome-partitioned Parquet dataset described by
//! *relative* paths. The S3 mapping layer is a later task's concern.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use duckdb::Connection;
use flate2::write::GzEncoder;
use flate2::Compression;
use rust_ingestion_worker::artifact::{
    build_artifact, canonical_descriptor_block, dataset_checksum_sha256, ArtifactBuildRequest,
    ArtifactError, ArtifactStats, LocalParquetFile, PROCESSOR_VERSION, ROW_GROUP_SIZE,
};
use rust_ingestion_worker::bgzf::live_decompression_threads;
use rust_ingestion_worker::concurrency::ConcurrencyLimits;
use rust_ingestion_worker::contracts::{
    FailureType, PARQUET_SCHEMA_FINGERPRINT, SORT_ORDER, VARIANTS_SEGMENT,
};
use rust_ingestion_worker::models::{NoopProgressSink, ProgressEvent, ProgressSink};
use rust_ingestion_worker::vcf::{
    normalize_chromosome, open_vcf, RejectionReason, VcfRecord, VcfRecordReader,
    SEQUENTIAL_DECOMPRESSION,
};
use serde_json::Value;
use tempfile::TempDir;

// ---------------------------------------------------------------------------------------
// Fixtures and helpers
// ---------------------------------------------------------------------------------------

/// Shape of the bounded-memory acceptance fixture, shared with `write_large_vcf` below.
const ACCEPTANCE_RECORDS: u32 = 100_000;
const ACCEPTANCE_BATCH_SIZE: usize = 1_000;
const ACCEPTANCE_CHROMS: [&str; 5] = ["chr1", "chr2", "chr12", "chrX", "chrM"];

const VCF_HEADER: &str = "##fileformat=VCFv4.2\n\
                          ##source=SyntheticGenomicsTest\n\
                          #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tDEMO_USER\n";

/// The checked-in demo VCF, shared with the TypeScript side.
fn demo_vcf_path() -> PathBuf {
    [env!("CARGO_MANIFEST_DIR"), "..", "tests", "fixtures", "demo_user.vcf"]
        .iter()
        .collect()
}

/// A golden cross-language fixture from `contracts/fixtures/`.
fn contract_fixture(name: &str) -> Value {
    let path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "..", "contracts", "fixtures", name]
        .iter()
        .collect();
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    serde_json::from_str(&text).expect("fixture is valid JSON")
}

fn write_vcf(directory: &Path, name: &str, body: &str) -> PathBuf {
    let path = directory.join(name);
    let mut file = File::create(&path).expect("create VCF");
    file.write_all(VCF_HEADER.as_bytes()).expect("write header");
    file.write_all(body.as_bytes()).expect("write body");
    path
}

/// A well-formed data line with the ten mandatory columns.
fn data_line(chrom: &str, pos: u32, rsid: &str, reference: &str, alt: &str, gt: &str) -> String {
    format!("{chrom}\t{pos}\t{rsid}\t{reference}\t{alt}\t99\tPASS\tGENE=TEST\tGT\t{gt}\n")
}

fn records(path: &Path) -> Vec<VcfRecord> {
    records_with(path, SEQUENTIAL_DECOMPRESSION)
}

/// [`records`] at a chosen BGZF worker count, for the tests that compare the block-parallel
/// decompressor against the sequential decoder over the same bytes.
fn records_with(path: &Path, bgzf_workers: usize) -> Vec<VcfRecord> {
    open_vcf(path, bgzf_workers)
        .expect("open VCF")
        .map(|record| record.expect("record is readable"))
        .collect()
}

fn variants(path: &Path) -> Vec<rust_ingestion_worker::models::UserVariant> {
    records(path)
        .into_iter()
        .filter_map(|record| match record {
            VcfRecord::Variant(variant) => Some(variant),
            VcfRecord::Rejected { .. } => None,
        })
        .collect()
}

fn rejections(path: &Path) -> Vec<RejectionReason> {
    records(path)
        .into_iter()
        .filter_map(|record| match record {
            VcfRecord::Rejected { reason, .. } => Some(reason),
            VcfRecord::Variant(_) => None,
        })
        .collect()
}

/// Builds the artifact under a fresh temp directory, returning the stats and the export root.
///
/// Unwraps the `Option` that says whether the sink interrupted the build: every sink used here
/// but [`InterruptingProgressSink`] always continues, and the one test that does interrupt calls
/// [`build_interruptibly_in`] instead.
fn build_in(directory: &TempDir, source: &Path, sink: &dyn ProgressSink, batch_size: usize) -> Result<(ArtifactStats, PathBuf), ArtifactError> {
    build_with(directory, source, sink, batch_size, ConcurrencyLimits::default())
}

/// [`build_in`] at a chosen set of concurrency bounds, for the tests that compare a sequential
/// run against a parallel one over the same input.
fn build_with(
    directory: &TempDir,
    source: &Path,
    sink: &dyn ProgressSink,
    batch_size: usize,
    concurrency: ConcurrencyLimits,
) -> Result<(ArtifactStats, PathBuf), ArtifactError> {
    build_interruptibly_with(directory, source, sink, batch_size, concurrency).map(|(stats, parquet_dir)| {
        (
            stats.expect("this sink never asks the build to stop"),
            parquet_dir,
        )
    })
}

fn build_interruptibly_in(
    directory: &TempDir,
    source: &Path,
    sink: &dyn ProgressSink,
    batch_size: usize,
) -> Result<(Option<ArtifactStats>, PathBuf), ArtifactError> {
    build_interruptibly_with(directory, source, sink, batch_size, ConcurrencyLimits::default())
}

fn build_interruptibly_with(
    directory: &TempDir,
    source: &Path,
    sink: &dyn ProgressSink,
    batch_size: usize,
    concurrency: ConcurrencyLimits,
) -> Result<(Option<ArtifactStats>, PathBuf), ArtifactError> {
    let parquet_dir = directory.path().join("parquet");
    let request = ArtifactBuildRequest {
        source_path: source.to_path_buf(),
        staging_db_path: directory.path().join("staging.duckdb"),
        parquet_output_dir: parquet_dir.clone(),
        dataset_id: "ds-test-001".to_string(),
        source_etag: "fixture-etag".to_string(),
        reference_build: "GRCh38".to_string(),
        batch_size,
        concurrency,
    };
    build_artifact(&request, sink).map(|stats| (stats, parquet_dir))
}

/// A `ProgressSink` that records every event so a test can inspect the reported batch sizes.
#[derive(Default)]
struct RecordingProgressSink {
    events: Mutex<Vec<ProgressEvent>>,
}

impl RecordingProgressSink {
    fn events(&self) -> Vec<ProgressEvent> {
        self.events.lock().expect("sink lock").clone()
    }
}

impl ProgressSink for RecordingProgressSink {
    fn report(&self, event: &ProgressEvent) -> ControlFlow<()> {
        self.events.lock().expect("sink lock").push(event.clone());
        ControlFlow::Continue(())
    }
}

/// A `ProgressSink` that asks the build to stop at the n-th boundary, and records how many
/// boundaries it was actually offered.
struct InterruptingProgressSink {
    stop_after: usize,
    seen: Mutex<Vec<ProgressEvent>>,
}

impl InterruptingProgressSink {
    fn new(stop_after: usize) -> Self {
        Self {
            stop_after,
            seen: Mutex::new(Vec::new()),
        }
    }

    fn seen(&self) -> Vec<ProgressEvent> {
        self.seen.lock().expect("sink lock").clone()
    }
}

impl ProgressSink for InterruptingProgressSink {
    fn report(&self, event: &ProgressEvent) -> ControlFlow<()> {
        let mut seen = self.seen.lock().expect("sink lock");
        seen.push(event.clone());
        if seen.len() >= self.stop_after {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
}

// ---------------------------------------------------------------------------------------
// A BGZF writer, built from the specification rather than from the reader
// ---------------------------------------------------------------------------------------

/// One BGZF member: a gzip member whose `EXTRA` area carries the `BC` subfield declaring the
/// member's own total size minus one.
///
/// ```text
/// 1f 8b | 08 | 04 | MTIME(4) | XFL | OS | XLEN=6 | 'B' 'C' | 02 00 | BSIZE(2) | deflate | CRC32 | ISIZE
/// ```
///
/// Deliberately hand-assembled here rather than shared with `src/bgzf.rs`: the reader is then
/// checked against an independently written encoder, not against its own constants.
fn bgzf_member(payload: &[u8]) -> Vec<u8> {
    let deflated = {
        let mut encoder = flate2::write::DeflateEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(payload).expect("deflate");
        encoder.finish().expect("finish deflate")
    };
    let total = 12 + 6 + deflated.len() + 8;
    let bsize = u16::try_from(total - 1).expect("a test block fits in BSIZE");

    let mut crc = flate2::Crc::new();
    crc.update(payload);

    let mut member = Vec::with_capacity(total);
    member.extend_from_slice(&[0x1f, 0x8b, 0x08, 0x04]); // magic, deflate, FEXTRA
    member.extend_from_slice(&0u32.to_le_bytes()); // MTIME
    member.extend_from_slice(&[0x00, 0xff]); // XFL, OS
    member.extend_from_slice(&6u16.to_le_bytes()); // XLEN
    member.extend_from_slice(b"BC");
    member.extend_from_slice(&2u16.to_le_bytes()); // SLEN
    member.extend_from_slice(&bsize.to_le_bytes());
    member.extend_from_slice(&deflated);
    member.extend_from_slice(&crc.sum().to_le_bytes());
    member.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    member
}

/// `content` cut into `block_payload`-sized BGZF blocks, terminated by the empty end-of-file
/// block `bgzip` always writes.
fn bgzf_file(content: &[u8], block_payload: usize) -> Vec<u8> {
    let mut file: Vec<u8> = content.chunks(block_payload).flat_map(bgzf_member).collect();
    file.extend_from_slice(&bgzf_member(b""));
    file
}

/// Serialises the tests that start a BGZF decompression pipeline.
///
/// [`live_decompression_threads`] is a process-wide count — useful as a metric, useless as a
/// per-test assertion while another test's pipeline is running. The tests that assert on it take
/// this lock, and so does every other test here that starts a pipeline.
static ONE_BGZF_PIPELINE_AT_A_TIME: Mutex<()> = Mutex::new(());

fn exclusive_bgzf() -> std::sync::MutexGuard<'static, ()> {
    ONE_BGZF_PIPELINE_AT_A_TIME
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Reads one scalar out of DuckDB, independently of the code under test.
fn scalar<T: duckdb::types::FromSql>(connection: &Connection, sql: &str) -> T {
    connection
        .query_row(sql, [], |row| row.get(0))
        .unwrap_or_else(|error| panic!("query failed: {sql}\n{error}"))
}

fn sql_path(path: &Path) -> String {
    path.to_str().expect("UTF-8 path").replace('\'', "''")
}

// ---------------------------------------------------------------------------------------
// The cross-language dataset checksum
// ---------------------------------------------------------------------------------------

/// Grouped so `cargo test artifact_builder` selects exactly these tests.
mod artifact_builder_checksum {
    use super::*;

    /// The single most load-bearing test in the crate: the Rust canonicalisation, reimplemented
    /// from `contracts/ingestion-v1.md`, must reproduce the frozen golden checksum byte for byte.
    #[test]
    fn reproduces_the_golden_dataset_checksum_from_relative_descriptors() {
        let result = contract_fixture("build-dataset-artifact.result.json");
        let attempt_prefix = result["attemptPrefix"].as_str().expect("attemptPrefix");
        let variants_prefix = format!("{attempt_prefix}{VARIANTS_SEGMENT}");

        let files: Vec<LocalParquetFile> = result["parquetObjects"]
            .as_array()
            .expect("parquetObjects")
            .iter()
            .map(|object| {
                let key = object["key"].as_str().expect("key");
                let relative_path = key
                    .strip_prefix(&variants_prefix)
                    .unwrap_or_else(|| panic!("key '{key}' must sit under '{variants_prefix}'"))
                    .to_string();
                assert!(
                    !relative_path.contains(VARIANTS_SEGMENT),
                    "relativePath must never carry the variants/ segment"
                );
                LocalParquetFile {
                    relative_path,
                    chrom: object["chrom"].as_str().expect("chrom").to_string(),
                    checksum_sha256: object["checksumSha256"].as_str().expect("checksum").to_string(),
                    byte_size: object["byteSize"].as_u64().expect("byteSize"),
                    row_count: object["rowCount"].as_u64().expect("rowCount"),
                    min_pos: object["minPos"].as_u64().expect("minPos") as u32,
                    max_pos: object["maxPos"].as_u64().expect("maxPos") as u32,
                    schema_fingerprint: PARQUET_SCHEMA_FINGERPRINT.to_string(),
                }
            })
            .collect();

        // The exact byte block the specification describes, spelled out here rather than
        // regenerated, so a change in the canonicalisation shows up as a diff in this test.
        let expected_block = concat!(
            "1\tchrom=1/part-000.parquet\t",
            "a9c0c80616a32401981426fc9ff39d835437416eaec480f755cf90eac0fac442\t20480\t900\t12345\t248900000\n",
            "12\tchrom=12/part-000.parquet\t",
            "a63f7af57491ba32774f0743f951b0b4d79094215013643caf41520013510464\t15360\t600\t21178615\t133200000\n",
        );
        assert_eq!(canonical_descriptor_block(&files), expected_block);

        assert_eq!(
            dataset_checksum_sha256(&files),
            result["datasetChecksumSha256"].as_str().expect("checksum"),
            "the Rust checksum must equal the frozen golden value"
        );
        assert_eq!(
            dataset_checksum_sha256(&files),
            "524e46eeee672654250c94b3f316937fbc6311fe41829f431b9897fea8d0e923"
        );
    }

    #[test]
    fn dataset_checksum_is_independent_of_the_order_descriptors_arrive_in() {
        let mut files = vec![
            descriptor("2", "chrom=2/part-000.parquet", 10, 5, 7),
            descriptor("1", "chrom=1/part-000.parquet", 20, 1, 3),
            descriptor("10", "chrom=10/part-000.parquet", 30, 2, 4),
        ];
        let forwards = dataset_checksum_sha256(&files);
        files.reverse();
        assert_eq!(dataset_checksum_sha256(&files), forwards);
    }

    /// Byte-wise, not numeric: `"1" < "10" < "12" < "2"`.
    #[test]
    fn canonical_descriptor_block_sorts_chromosomes_byte_wise() {
        let files = vec![
            descriptor("2", "chrom=2/part-000.parquet", 1, 1, 1),
            descriptor("12", "chrom=12/part-000.parquet", 1, 1, 1),
            descriptor("1", "chrom=1/part-000.parquet", 1, 1, 1),
            descriptor("10", "chrom=10/part-000.parquet", 1, 1, 1),
        ];
        let block = canonical_descriptor_block(&files);
        let order: Vec<&str> = block
            .lines()
            .map(|line| line.split('\t').next().expect("chrom column"))
            .collect();
        assert_eq!(order, ["1", "10", "12", "2"]);
    }

    /// Integers render as unpadded base-10 with no sign or separators.
    #[test]
    fn canonical_descriptor_block_renders_integers_unpadded() {
        let files = vec![LocalParquetFile {
            relative_path: "chrom=1/part-000.parquet".to_string(),
            chrom: "1".to_string(),
            checksum_sha256: "0".repeat(64),
            byte_size: 0,
            row_count: 1_000_000,
            min_pos: 0,
            max_pos: 4_294_967_295,
        schema_fingerprint: PARQUET_SCHEMA_FINGERPRINT.to_string(),
        }];
        assert_eq!(
            canonical_descriptor_block(&files),
            format!("1\tchrom=1/part-000.parquet\t{}\t0\t1000000\t0\t4294967295\n", "0".repeat(64))
        );
    }

    fn descriptor(chrom: &str, relative_path: &str, bytes: u64, min_pos: u32, max_pos: u32) -> LocalParquetFile {
        LocalParquetFile {
            relative_path: relative_path.to_string(),
            chrom: chrom.to_string(),
            checksum_sha256: "a".repeat(64),
            byte_size: bytes,
            row_count: 1,
            min_pos,
            max_pos,
            schema_fingerprint: PARQUET_SCHEMA_FINGERPRINT.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------------------
// Streaming VCF parsing
// ---------------------------------------------------------------------------------------

/// Grouped so `cargo test vcf` selects exactly these tests.
mod vcf {
    use super::*;

    #[test]
    fn parses_the_checked_in_demo_vcf() {
        let parsed = variants(&demo_vcf_path());
        assert_eq!(parsed.len(), 4);
        assert_eq!(parsed[0].chrom, "15");
        assert_eq!(parsed[0].pos, 74_749_576);
        assert_eq!(parsed[0].rsid.as_deref(), Some("rs762551"));
        assert_eq!(parsed[0].ref_allele, "A");
        assert_eq!(parsed[0].alt_allele, "C");
        assert_eq!(parsed[0].gt_raw, "1/1");
        assert_eq!(
            parsed.iter().map(|variant| variant.chrom.as_str()).collect::<Vec<_>>(),
            ["15", "2", "12", "16"]
        );
    }

    #[test]
    fn parses_a_gzipped_vcf_exactly_like_the_plain_one() {
        let directory = TempDir::new().expect("temp dir");
        let gzipped = directory.path().join("demo_user.vcf.gz");
        let plain = std::fs::read(demo_vcf_path()).expect("read demo VCF");
        let mut encoder = GzEncoder::new(File::create(&gzipped).expect("create gz"), Compression::default());
        encoder.write_all(&plain).expect("compress");
        encoder.finish().expect("finish gz");

        assert_eq!(variants(&gzipped), variants(&demo_vcf_path()));
    }

    /// A `bgzip`-compressed VCF — which is what a real GIAB or 1000 Genomes file is — must parse
    /// to exactly the same records whether its blocks were inflated on one thread or on many.
    ///
    /// The fixture is assembled from the BGZF layout in this test file rather than by calling
    /// the reader's own helpers, so the two sides of the format are written independently.
    #[test]
    fn parses_a_bgzip_compressed_vcf_identically_at_every_worker_count() {
        let _exclusive = exclusive_bgzf();
        let directory = TempDir::new().expect("temp dir");

        // Enough records that the body spans many 4 KiB blocks: a reassembly that lost or
        // reordered a block could not pass.
        let mut body = String::from(VCF_HEADER);
        for index in 0..20_000u32 {
            body.push_str(&data_line(
                ["1", "2", "12", "X", "MT"][index as usize % 5],
                10_000 + index * 7,
                &format!("rs{index}"),
                "A",
                "C",
                "0/1",
            ));
        }

        let plain = directory.path().join("plain.vcf");
        std::fs::write(&plain, &body).expect("write plain VCF");

        let bgzipped = directory.path().join("bgzipped.vcf.gz");
        std::fs::write(&bgzipped, bgzf_file(body.as_bytes(), 4_096)).expect("write BGZF VCF");

        let expected = records(&plain);
        assert!(expected.len() > 19_000, "the fixture must be substantial");

        for workers in [SEQUENTIAL_DECOMPRESSION, 2, 4, 16] {
            assert_eq!(
                records_with(&bgzipped, workers),
                expected,
                "the BGZF VCF parsed differently at {workers} workers"
            );
        }
    }

    /// Plain gzip must keep working exactly as it did: it is a single deflate stream and cannot
    /// be split, so asking for parallel decompression must fall back rather than fail or change
    /// the records.
    #[test]
    fn plain_gzip_falls_back_to_the_sequential_decoder_unchanged() {
        let _exclusive = exclusive_bgzf();
        let directory = TempDir::new().expect("temp dir");
        let gzipped = directory.path().join("plain_gzip.vcf.gz");
        let plain = std::fs::read(demo_vcf_path()).expect("read demo VCF");
        let mut encoder =
            GzEncoder::new(File::create(&gzipped).expect("create gz"), Compression::default());
        encoder.write_all(&plain).expect("compress");
        encoder.finish().expect("finish gz");

        let expected = records(&demo_vcf_path());
        for workers in [SEQUENTIAL_DECOMPRESSION, 2, 16] {
            assert_eq!(
                records_with(&gzipped, workers),
                expected,
                "plain gzip changed at {workers} workers"
            );
        }
        // And uncompressed input is untouched by the setting too.
        for workers in [SEQUENTIAL_DECOMPRESSION, 16] {
            assert_eq!(records_with(&demo_vcf_path(), workers), expected);
        }
    }

    /// `bgzip` writes a *concatenation* of independent gzip members rather than one stream, so
    /// the reader uses `MultiGzDecoder`. A plain `GzDecoder` stops after the first member and
    /// would silently truncate a BGZF VCF to its first block, which is why this is tested
    /// separately from the single-member case above.
    #[test]
    fn reads_every_member_of_a_multi_member_gzipped_vcf() {
        let directory = TempDir::new().expect("temp dir");
        let path = directory.path().join("multi_member.vcf.gz");

        let first = gzip_member(&format!("{VCF_HEADER}{}", data_line("1", 100, "rs1", "A", "C", "0/1")));
        let second = gzip_member(
            &[
                data_line("2", 200, "rs2", "G", "T", "1|1"),
                data_line("X", 300, "rs3", "C", "A", "0/1"),
            ]
            .concat(),
        );
        std::fs::write(&path, [first, second].concat()).expect("write concatenated members");

        let parsed = variants(&path);
        assert_eq!(
            parsed.iter().map(|variant| variant.chrom.as_str()).collect::<Vec<_>>(),
            ["1", "2", "X"],
            "a second gzip member must not be silently dropped"
        );
        assert_eq!(parsed[2].pos, 300);
    }

    /// Gzip is detected from the magic bytes, not the file extension.
    #[test]
    fn detects_a_gzipped_vcf_without_a_gz_extension() {
        let directory = TempDir::new().expect("temp dir");
        let misnamed = directory.path().join("compressed_but_named.vcf");
        let plain = std::fs::read(demo_vcf_path()).expect("read demo VCF");
        let mut encoder = GzEncoder::new(File::create(&misnamed).expect("create"), Compression::default());
        encoder.write_all(&plain).expect("compress");
        encoder.finish().expect("finish gz");

        assert_eq!(variants(&misnamed).len(), 4);
    }

    #[test]
    fn a_header_only_vcf_yields_no_records() {
        let directory = TempDir::new().expect("temp dir");
        let path = write_vcf(directory.path(), "header_only.vcf", "");
        assert!(records(&path).is_empty());
    }

    #[test]
    fn rejects_a_vcf_record_whose_format_column_has_no_gt() {
        let directory = TempDir::new().expect("temp dir");
        let path = write_vcf(
            directory.path(),
            "no_gt.vcf",
            "1\t100\trs1\tA\tC\t99\tPASS\tGENE=TEST\tDP:AD\t30:12\n",
        );
        assert_eq!(rejections(&path), [RejectionReason::MissingGenotypeField]);
    }

    #[test]
    fn rejects_a_vcf_record_whose_sample_column_omits_the_gt_value() {
        let directory = TempDir::new().expect("temp dir");
        let path = write_vcf(
            directory.path(),
            "short_sample.vcf",
            "1\t100\trs1\tA\tC\t99\tPASS\tGENE=TEST\tDP:GT\t30\n",
        );
        assert_eq!(rejections(&path), [RejectionReason::MissingGenotypeValue]);
    }

    #[test]
    fn rejects_a_vcf_record_with_an_invalid_position() {
        let directory = TempDir::new().expect("temp dir");
        let body = [
            data_line("1", 0, "rs0", "A", "C", "0/1"),
            "1\tnot-a-number\trs1\tA\tC\t99\tPASS\tGENE=TEST\tGT\t0/1\n".to_string(),
            "1\t-5\trs2\tA\tC\t99\tPASS\tGENE=TEST\tGT\t0/1\n".to_string(),
            "1\t4294967296\trs3\tA\tC\t99\tPASS\tGENE=TEST\tGT\t0/1\n".to_string(),
        ]
        .concat();
        let path = write_vcf(directory.path(), "bad_pos.vcf", &body);
        assert_eq!(rejections(&path), [RejectionReason::InvalidPosition; 4]);
    }

    #[test]
    fn rejects_a_vcf_record_with_too_few_columns() {
        let directory = TempDir::new().expect("temp dir");
        let path = write_vcf(directory.path(), "short.vcf", "1\t100\trs1\tA\tC\n");
        assert_eq!(rejections(&path), [RejectionReason::TooFewColumns]);
    }

    #[test]
    fn parses_phased_and_multiallelic_vcf_genotypes() {
        let directory = TempDir::new().expect("temp dir");
        let body = [
            data_line("1", 100, "rs_phased", "A", "C", "0|1"),
            data_line("1", 200, "rs_multi", "A", "C,G", "1/2"),
            "1\t300\trs_offset\tA\tC\t99\tPASS\tGENE=TEST\tDP:GT:AD\t30:0|2:5,6\n".to_string(),
        ]
        .concat();
        let path = write_vcf(directory.path(), "genotypes.vcf", &body);

        let parsed = variants(&path);
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].gt_raw, "0|1");
        assert_eq!(parsed[1].gt_raw, "1/2");
        assert_eq!(parsed[1].alt_allele, "C,G");
        // GT is located by its index in FORMAT, not by assuming it comes first.
        assert_eq!(parsed[2].gt_raw, "0|2");
    }

    #[test]
    fn a_missing_vcf_rsid_becomes_null_rather_than_a_dot() {
        let directory = TempDir::new().expect("temp dir");
        let path = write_vcf(directory.path(), "no_rsid.vcf", &data_line("1", 100, ".", "A", "C", "0/1"));
        assert_eq!(variants(&path)[0].rsid, None);
    }

    #[test]
    fn normalizes_chromosome_names_found_in_vcf_sources() {
        for (raw, expected) in [
            ("1", "1"),
            ("chr1", "1"),
            ("CHR1", "1"),
            ("Chr22", "22"),
            ("chrX", "X"),
            ("y", "Y"),
            ("chrM", "MT"),
            ("MT", "MT"),
            (" chr12 ", "12"),
        ] {
            assert_eq!(
                normalize_chromosome(raw).as_deref(),
                Some(expected),
                "'{raw}' should normalize to '{expected}'"
            );
        }
    }

    /// An unexpected contig must be rejected by the parser, long before it could be interpolated
    /// into a `chrom=<value>` partition directory.
    #[test]
    fn rejects_unsafe_contigs_before_they_can_become_a_partition_path() {
        for hostile in [
            "",
            "chr",
            "0",
            "23",
            "007",
            "../../etc/passwd",
            "chr1/../../escape",
            "1=2",
            "chrom=1",
            "chrUn_GL000220v1",
            "1; DROP TABLE user_variants",
            "1'",
            "X\u{0}",
            "chr1\n2",
        ] {
            assert_eq!(normalize_chromosome(hostile), None, "'{hostile}' must be rejected");
        }

        let directory = TempDir::new().expect("temp dir");
        let path = write_vcf(
            directory.path(),
            "hostile_contig.vcf",
            &data_line("../../escape", 100, "rs1", "A", "C", "0/1"),
        );
        assert_eq!(rejections(&path), [RejectionReason::UnsupportedContig]);
    }

    /// Every value the parser can emit is safe to use verbatim as a partition directory.
    #[test]
    fn every_normalized_vcf_chromosome_is_a_safe_path_segment() {
        let accepted: Vec<String> = (0..300)
            .flat_map(|n| [n.to_string(), format!("chr{n}")])
            .chain(["X", "Y", "M", "MT", "chrX", "chrM"].map(str::to_string))
            .filter_map(|raw| normalize_chromosome(&raw))
            .collect();
        assert!(!accepted.is_empty());
        for value in accepted {
            assert!(
                value.bytes().all(|byte| byte.is_ascii_digit() || byte.is_ascii_uppercase()),
                "'{value}' is not a safe partition segment"
            );
        }
    }

    #[test]
    fn the_vcf_reader_tracks_uncompressed_bytes_read() {
        let directory = TempDir::new().expect("temp dir");
        let body = data_line("1", 100, "rs1", "A", "C", "0/1");
        let path = write_vcf(directory.path(), "bytes.vcf", &body);
        let expected = VCF_HEADER.len() + body.len();

        let mut reader = open_vcf(&path, SEQUENTIAL_DECOMPRESSION).expect("open");
        assert!(reader.next().is_some());
        assert!(reader.next().is_none());
        assert_eq!(reader.bytes_read(), expected as u64);
    }

    #[test]
    fn the_vcf_reader_accepts_any_bufread_source() {
        let text = format!("{VCF_HEADER}{}", data_line("chr7", 42, ".", "G", "T", "1|1"));
        let reader = VcfRecordReader::new(std::io::Cursor::new(text));
        let parsed: Vec<VcfRecord> = reader.map(|record| record.expect("record")).collect();
        match &parsed[..] {
            [VcfRecord::Variant(variant)] => {
                assert_eq!(variant.chrom, "7");
                assert_eq!(variant.pos, 42);
                assert_eq!(variant.rsid, None);
            }
            other => panic!("expected exactly one variant, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------------------
// The artifact builder
// ---------------------------------------------------------------------------------------

/// Grouped so `cargo test artifact_builder` selects exactly these tests.
mod artifact_builder {
    use super::*;

    #[test]
    fn builds_a_chromosome_partitioned_dataset_from_the_demo_vcf() {
        let directory = TempDir::new().expect("temp dir");
        let (stats, parquet_dir) =
            build_in(&directory, &demo_vcf_path(), &RecordingProgressSink::default(), 1_000)
                .expect("build succeeds");

        assert_eq!(stats.variant_count, 4);
        assert_eq!(stats.rejected_record_count, 0);
        assert_eq!(stats.reference_build, "GRCh38");
        assert_eq!(stats.processor_version, PROCESSOR_VERSION);

        // Canonical order is byte-wise by (chrom, relativePath): 12 < 15 < 16 < 2.
        assert_eq!(
            stats
                .local_parquet_files
                .iter()
                .map(|file| file.relative_path.as_str())
                .collect::<Vec<_>>(),
            [
                "chrom=12/part-000.parquet",
                "chrom=15/part-000.parquet",
                "chrom=16/part-000.parquet",
                "chrom=2/part-000.parquet",
            ]
        );

        for file in &stats.local_parquet_files {
            assert_eq!(file.relative_path, format!("chrom={}/part-000.parquet", file.chrom));
            assert!(!file.relative_path.contains(VARIANTS_SEGMENT));
            assert_eq!(file.schema_fingerprint, PARQUET_SCHEMA_FINGERPRINT);
            assert_eq!(file.row_count, 1);

            let absolute = parquet_dir.join(&file.relative_path);
            let bytes = std::fs::read(&absolute).expect("read Parquet file");
            assert_eq!(file.byte_size, bytes.len() as u64);
            assert_eq!(file.checksum_sha256, sha256_hex(&bytes));
        }

        assert_eq!(stats.dataset_checksum_sha256, dataset_checksum_sha256(&stats.local_parquet_files));
    }

    #[test]
    fn counts_malformed_records_instead_of_failing_the_build() {
        let directory = TempDir::new().expect("temp dir");
        let body = [
            data_line("1", 100, "rs1", "A", "C", "0/1"),
            "1\tnope\trs2\tA\tC\t99\tPASS\tGENE=TEST\tGT\t0/1\n".to_string(),
            "1\t101\trs3\tA\tC\n".to_string(),
            data_line("chrUn_GL000220v1", 102, "rs4", "A", "C", "0/1"),
            "1\t103\trs5\tA\tC\t99\tPASS\tGENE=TEST\tDP\t30\n".to_string(),
            data_line("chr2", 200, ".", "G", "T", "1|1"),
        ]
        .concat();
        let source = write_vcf(directory.path(), "mixed.vcf", &body);

        let (stats, _) = build_in(&directory, &source, &RecordingProgressSink::default(), 1_000)
            .expect("malformed records must not abort the build");
        assert_eq!(stats.variant_count, 2);
        assert_eq!(stats.rejected_record_count, 4);
    }

    #[test]
    fn refuses_to_reuse_an_existing_staging_database() {
        let directory = TempDir::new().expect("temp dir");
        std::fs::write(directory.path().join("staging.duckdb"), b"pre-existing").expect("seed");

        let error = build_in(&directory, &demo_vcf_path(), &RecordingProgressSink::default(), 1_000)
            .expect_err("an existing staging path must be refused");
        assert_eq!(error.failure_type(), FailureType::ArtifactWriteFailed);
    }

    #[test]
    fn refuses_to_write_into_an_existing_parquet_directory() {
        let directory = TempDir::new().expect("temp dir");
        std::fs::create_dir(directory.path().join("parquet")).expect("seed");

        let error = build_in(&directory, &demo_vcf_path(), &RecordingProgressSink::default(), 1_000)
            .expect_err("an existing export directory must be refused");
        assert_eq!(error.failure_type(), FailureType::ArtifactWriteFailed);
    }

    #[test]
    fn fails_when_a_vcf_contributes_no_variants_at_all() {
        let directory = TempDir::new().expect("temp dir");
        let source = write_vcf(directory.path(), "empty.vcf", "");

        let error = build_in(&directory, &source, &RecordingProgressSink::default(), 1_000)
            .expect_err("an empty dataset cannot be published");
        assert_eq!(error.failure_type(), FailureType::InvalidVcfFormat);
    }

    /// A source that is not there is an environment problem, not a malformed VCF. Classifying
    /// it as `InvalidVcfFormat` would permanently fail the workflow on a transient fault and
    /// tell the user their file is broken when it is not.
    #[test]
    fn a_missing_source_file_is_a_retryable_write_failure_not_an_invalid_vcf() {
        let directory = TempDir::new().expect("temp dir");
        let missing = directory.path().join("never_downloaded.vcf");

        let error = build_in(&directory, &missing, &RecordingProgressSink::default(), 1_000)
            .expect_err("a missing source cannot produce a dataset");
        assert_eq!(error.failure_type(), FailureType::ArtifactWriteFailed);
        assert!(error.failure_type().is_retryable(), "a missing scratch file must be retryable");
    }

    /// The same argument for a source the process cannot open.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_source_file_is_a_retryable_write_failure() {
        use std::os::unix::fs::PermissionsExt;

        let directory = TempDir::new().expect("temp dir");
        let source = write_vcf(directory.path(), "locked.vcf", &data_line("1", 100, "rs1", "A", "C", "0/1"));
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o000)).expect("chmod");
        if File::open(&source).is_ok() {
            // Running as root, where the mode is not enforced. The unit test over
            // `io::ErrorKind::PermissionDenied` still covers the classification.
            return;
        }

        // Restore permissions on both the success and failure path before asserting: an
        // `expect_err` that panics on an unexpected `Ok` would otherwise skip the restore and
        // leave the locked-down file behind in the `TempDir`.
        let result = build_in(&directory, &source, &RecordingProgressSink::default(), 1_000);
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o644)).expect("restore");
        let error = result.expect_err("an unreadable source cannot produce a dataset");

        assert_eq!(error.failure_type(), FailureType::ArtifactWriteFailed);
        assert!(error.failure_type().is_retryable(), "a permission error must be retryable");
    }

    /// The other direction: a damaged gzip member is a property of the bytes, so it stays a
    /// non-retryable `InvalidVcfFormat` even though it surfaces as an `io::Error`.
    #[test]
    fn a_corrupt_gzip_source_is_a_non_retryable_invalid_vcf() {
        let directory = TempDir::new().expect("temp dir");
        let source = directory.path().join("corrupt.vcf.gz");
        let body: String = (1..200)
            .map(|pos| data_line("1", pos, "rs1", "A", "C", "0/1"))
            .collect();
        std::fs::write(&source, corrupt_gzip(&format!("{VCF_HEADER}{body}"))).expect("write corrupt gz");

        let error = build_in(&directory, &source, &RecordingProgressSink::default(), 1_000)
            .expect_err("a corrupt gzip stream cannot produce a dataset");
        assert_eq!(error.failure_type(), FailureType::InvalidVcfFormat);
        assert!(
            !error.failure_type().is_retryable(),
            "corruption is deterministic; retrying the same bytes cannot help"
        );
    }

    /// The contract's sort order is `(chrom, pos, ref, alt)`, but every other fixture uses
    /// distinct positions, so the `ref`/`alt` tiebreak would be unexercised. These records
    /// share a position, differ only in `ref`/`alt`, and are written in descending key order,
    /// so an export that sorted by `pos` alone would emit them in the wrong order.
    #[test]
    fn orders_records_sharing_a_position_by_the_ref_and_alt_tiebreak() {
        let directory = TempDir::new().expect("temp dir");
        let body = [
            data_line("1", 500, "rs_e", "T", "G", "0/1"),
            data_line("1", 500, "rs_d", "G", "T", "0/1"),
            data_line("1", 500, "rs_c", "A", "T", "0/1"),
            data_line("1", 500, "rs_b", "A", "G", "0/1"),
            data_line("1", 500, "rs_a", "A", "C", "0/1"),
            data_line("1", 100, "rs_first", "C", "A", "0/1"),
        ]
        .concat();
        let source = write_vcf(directory.path(), "tied_positions.vcf", &body);

        let (stats, parquet_dir) =
            build_in(&directory, &source, &RecordingProgressSink::default(), 1_000)
                .expect("build succeeds");
        assert_eq!(stats.variant_count, 6);
        assert_eq!(stats.local_parquet_files.len(), 1);

        let connection = Connection::open_in_memory().expect("connection");
        let quoted = sql_path(&parquet_dir.join(&stats.local_parquet_files[0].relative_path));
        let mut statement = connection
            .prepare(&format!("SELECT pos, ref, alt FROM read_parquet('{quoted}')"))
            .expect("prepare");
        let emitted: Vec<(u32, String, String)> = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .expect("rows")
            .map(|row| row.expect("row"))
            .collect();

        assert_eq!(
            emitted,
            [
                (100, "C".to_string(), "A".to_string()),
                (500, "A".to_string(), "C".to_string()),
                (500, "A".to_string(), "G".to_string()),
                (500, "A".to_string(), "T".to_string()),
                (500, "G".to_string(), "T".to_string()),
                (500, "T".to_string(), "G".to_string()),
            ],
            "rows sharing a position must be ordered by (ref, alt)"
        );
    }

    #[test]
    fn commits_dataset_metadata_to_the_staging_database() {
        let directory = TempDir::new().expect("temp dir");
        let (stats, _) = build_in(&directory, &demo_vcf_path(), &RecordingProgressSink::default(), 2)
            .expect("build succeeds");

        let staging = Connection::open(directory.path().join("staging.duckdb")).expect("open staging");
        let row: (String, String, String, u64, u64, String) = staging
            .query_row(
                "SELECT dataset_id, source_etag, reference_build, variant_count, rejected_record_count, processor_version
                 FROM dataset_metadata",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
            )
            .expect("exactly one metadata row");
        assert_eq!(row, (
            "ds-test-001".to_string(),
            "fixture-etag".to_string(),
            "GRCh38".to_string(),
            stats.variant_count,
            stats.rejected_record_count,
            PROCESSOR_VERSION.to_string(),
        ));

        let staged: i64 = scalar(&staging, "SELECT count(*) FROM user_variants");
        assert_eq!(staged as u64, stats.variant_count);
    }

    // ---------------------------------------------------------------------------------------
    // Bounded-memory acceptance test
    // ---------------------------------------------------------------------------------------


    /// Streams 100,000 records through the processor with `batch_size = 1_000` and proves that
    /// the work stays bounded, that the export is correctly partitioned, ordered, compressed and
    /// chunked, and that no record is lost or duplicated.
    #[test]
    fn streams_100000_vcf_records_into_parquet_without_unbounded_buffering() {
        let directory = TempDir::new().expect("temp dir");
        let source = directory.path().join("large.vcf");
        write_large_vcf(&source, ACCEPTANCE_RECORDS);

        let sink = RecordingProgressSink::default();
        let (stats, parquet_dir) = build_in(&directory, &source, &sink, ACCEPTANCE_BATCH_SIZE)
            .expect("large build succeeds");

        // --- bounded memory -----------------------------------------------------------------
        let events = sink.events();
        let batch_events: Vec<&ProgressEvent> =
            events.iter().filter(|event| event.batch_records > 0).collect();
        assert!(
            batch_events.len() >= (ACCEPTANCE_RECORDS as usize / ACCEPTANCE_BATCH_SIZE),
            "expected at least one progress event per batch, saw {}",
            batch_events.len()
        );
        let largest = batch_events.iter().map(|event| event.batch_records).max().unwrap_or(0);
        assert!(
            largest <= ACCEPTANCE_BATCH_SIZE,
            "a batch of {largest} records exceeds the {ACCEPTANCE_BATCH_SIZE}-record bound"
        );
        assert_eq!(
            batch_events.iter().map(|event| event.batch_records).sum::<usize>(),
            ACCEPTANCE_RECORDS as usize,
            "every accepted record must be reported in exactly one batch"
        );
        let reported: Vec<u64> = batch_events.iter().map(|event| event.processed_variants).collect();
        assert!(reported.windows(2).all(|pair| pair[0] < pair[1]), "progress must advance");
        assert_eq!(*reported.last().expect("a final batch"), ACCEPTANCE_RECORDS as u64);
        assert!(
            events.iter().any(|event| event.processed_bytes > 0),
            "the reader must report the bytes it has consumed"
        );

        // --- inventory ----------------------------------------------------------------------
        assert_eq!(stats.variant_count, ACCEPTANCE_RECORDS as u64);
        assert_eq!(stats.rejected_record_count, 0);
        assert_eq!(stats.local_parquet_files.len(), ACCEPTANCE_CHROMS.len());

        // Canonically ordered by (chrom, relativePath), byte-wise.
        let paths: Vec<&str> =
            stats.local_parquet_files.iter().map(|file| file.relative_path.as_str()).collect();
        assert_eq!(
            paths,
            [
                "chrom=1/part-000.parquet",
                "chrom=12/part-000.parquet",
                "chrom=2/part-000.parquet",
                "chrom=MT/part-000.parquet",
                "chrom=X/part-000.parquet",
            ]
        );
        assert_eq!(stats.dataset_checksum_sha256, dataset_checksum_sha256(&stats.local_parquet_files));

        // --- record conservation ------------------------------------------------------------
        let total: u64 = stats.local_parquet_files.iter().map(|file| file.row_count).sum();
        assert_eq!(total, ACCEPTANCE_RECORDS as u64, "records must be conserved across files");

        let connection = Connection::open_in_memory().expect("validation connection");
        let glob = sql_path(&parquet_dir.join("**").join("*.parquet"));
        let scanned: i64 = scalar(
            &connection,
            &format!("SELECT count(*) FROM read_parquet('{glob}', hive_partitioning = true, hive_types_autocast = 0)"),
        );
        assert_eq!(scanned as u32, ACCEPTANCE_RECORDS);
        let distinct_positions: i64 = scalar(
            &connection,
            &format!("SELECT count(DISTINCT (chrom, pos)) FROM read_parquet('{glob}', hive_partitioning = true, hive_types_autocast = 0)"),
        );
        assert_eq!(distinct_positions as u32, ACCEPTANCE_RECORDS, "no record may be duplicated");

        // --- per-file physical facts ---------------------------------------------------------
        for file in &stats.local_parquet_files {
            let absolute = parquet_dir.join(&file.relative_path);
            let quoted = sql_path(&absolute);

            assert_physical_schema(&connection, &quoted);

            let compressions: Vec<String> = string_column(
                &connection,
                &format!("SELECT DISTINCT compression FROM parquet_metadata('{quoted}') ORDER BY 1"),
            );
            assert_eq!(compressions, ["ZSTD"], "{} must be Zstandard compressed", file.relative_path);

            // Row groups are cut at ROW_GROUP_SIZE, with at most one DuckDB vector of overshoot.
            let groups = row_group_sizes(&connection, &quoted);
            assert!(!groups.is_empty());
            for (index, rows) in groups.iter().enumerate() {
                assert!(
                    *rows <= ROW_GROUP_SIZE as u64 + 2048,
                    "row group {index} of {} holds {rows} rows",
                    file.relative_path
                );
                if index + 1 < groups.len() {
                    assert!(*rows >= ROW_GROUP_SIZE as u64, "row group {index} was cut early at {rows}");
                }
            }
            assert_eq!(groups.iter().sum::<u64>(), file.row_count);

            // Statistics agree with the data and rows are sorted by pos inside the partition.
            let (rows, min_pos, max_pos, sorted): (i64, u32, u32, bool) = connection
                .query_row(
                    &format!(
                        "SELECT count(*), min(pos), max(pos),
                                bool_and(pos >= previous)
                         FROM (SELECT pos, lag(pos, 1, 0::UINTEGER) OVER () AS previous
                               FROM read_parquet('{quoted}'))"
                    ),
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .expect("statistics query");
            assert_eq!(rows as u64, file.row_count);
            assert_eq!(min_pos, file.min_pos);
            assert_eq!(max_pos, file.max_pos);
            assert!(sorted, "{} is not ordered by pos", file.relative_path);

            // Every row in the file really belongs to its partition directory.
            let foreign: i64 = scalar(
                &connection,
                &format!(
                    "SELECT count(*) FROM read_parquet('{quoted}', hive_partitioning = true, hive_types_autocast = 0) WHERE chrom <> '{}'",
                    file.chrom
                ),
            );
            assert_eq!(foreign, 0);
        }

        // The declared sort order is the frozen one.
        assert_eq!(SORT_ORDER, ["chrom", "pos", "ref", "alt"]);
    }

    /// Evidence for the `hive_types_autocast = 0` clause in `contracts/ingestion-v1.md`.
    ///
    /// `hive_partitioning = true` on its own does **not** give `chrom` a stable type: DuckDB
    /// infers it by trying to cast the partition values it actually scanned, so the very same
    /// dataset presents `chrom` as `BIGINT` or `VARCHAR` depending on which files the scan
    /// touched. A consumer that then writes `chrom = 'X'` gets a hard conversion error rather
    /// than an empty result. Passing `hive_types_autocast = 0` pins it to `VARCHAR` in every
    /// case, which is what the contract requires of both languages.
    #[test]
    fn hive_partitioning_alone_gives_chrom_an_unstable_type() {
        let autosomes_only = TempDir::new().expect("temp dir");
        let autosomes = build_partitions(&autosomes_only, &["1", "2", "12"]);
        let with_allosomes = TempDir::new().expect("temp dir");
        let mixed = build_partitions(&with_allosomes, &["1", "2", "12", "X", "Y", "MT"]);

        let connection = Connection::open_in_memory().expect("connection");
        let whole = |root: &Path| sql_path(&root.join("**").join("*.parquet"));
        // A subset scan of the mixed dataset that happens to select only autosomes.
        let autosome_subset = sql_path(&mixed.join("chrom=1*").join("*.parquet"));

        // A single-file scan of one autosome partition of the very same mixed dataset.
        let one_autosome = sql_path(&mixed.join("chrom=12").join("part-000.parquet"));

        // --- without the option: the type follows the data that happened to be scanned -------
        assert_eq!(hive_chrom_type(&connection, &whole(&autosomes), HIVE_ONLY), "BIGINT");
        // The same dataset presents chrom differently depending only on how much of it is read.
        assert_eq!(hive_chrom_type(&connection, &whole(&mixed), HIVE_ONLY), "VARCHAR");
        assert_eq!(hive_chrom_type(&connection, &autosome_subset, HIVE_ONLY), "BIGINT");
        assert_eq!(hive_chrom_type(&connection, &one_autosome, HIVE_ONLY), "BIGINT");

        // --- with the option: VARCHAR everywhere, which is the contract ---------------------
        for target in [whole(&autosomes), whole(&mixed), autosome_subset.clone(), one_autosome] {
            for options in [HIVE_TYPED, HIVE_DECLARED] {
                assert_eq!(
                    hive_chrom_type(&connection, &target, options),
                    "VARCHAR",
                    "'{options}' must pin chrom to VARCHAR for '{target}'"
                );
            }
        }

        // --- the consequence a query layer would actually hit --------------------------------
        // Deliberately a glob (`whole(&autosomes)`), never `one_autosome`: a *single-file* scan
        // with `hive_partitioning = true` (autocast on, so `chrom` infers `BIGINT`) plus a
        // string predicate on `chrom` aborts a DuckDB debug build with
        // `Assertion failed: (root_schema->children.size() > primary_index), function
        // GetColumnStatistics, file parquet_reader.cpp, line 1345` (SIGABRT), which
        // `hive_types_autocast = 0` avoids entirely. Do not swap this back to a single-file
        // target without checking that abort no longer reproduces.
        let predicate = |options: &str| {
            connection
                .query_row(
                    &format!(
                        "SELECT count(*) FROM read_parquet('{}', {options}) WHERE chrom = 'X'",
                        whole(&autosomes)
                    ),
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(|error| error.to_string())
        };
        let failure = predicate(HIVE_ONLY)
            .expect_err("a BIGINT chrom cannot be compared with 'X'");
        assert!(
            failure.contains("Conversion Error") && failure.contains('X'),
            "expected a conversion error, got: {failure}"
        );
        assert_eq!(
            predicate(HIVE_TYPED),
            Ok(0),
            "with the option the same predicate is simply an empty result"
        );
        assert_eq!(predicate(HIVE_DECLARED), Ok(0), "the explicit declaration behaves the same");
    }

    /// A partition larger than `ROW_GROUP_SIZE` is split, proving the setting reached DuckDB
    /// rather than falling back to its larger default.
    #[test]
    fn splits_a_large_partition_into_multiple_row_groups() {
        let rows: u32 = 110_000;
        let directory = TempDir::new().expect("temp dir");
        let source = directory.path().join("single_chrom.vcf");
        {
            let mut writer = BufWriter::new(File::create(&source).expect("create"));
            writer.write_all(VCF_HEADER.as_bytes()).expect("header");
            for position in 1..=rows {
                writer
                    .write_all(data_line("1", position, ".", "A", "C", "0/1").as_bytes())
                    .expect("record");
            }
        }

        let (stats, parquet_dir) =
            build_in(&directory, &source, &RecordingProgressSink::default(), 5_000)
                .expect("build succeeds");
        assert_eq!(stats.local_parquet_files.len(), 1);
        assert_eq!(stats.local_parquet_files[0].row_count, rows as u64);

        let connection = Connection::open_in_memory().expect("connection");
        let quoted = sql_path(&parquet_dir.join(&stats.local_parquet_files[0].relative_path));
        let groups = row_group_sizes(&connection, &quoted);
        assert_eq!(groups.len(), 2, "110,000 rows must not fit in one 100,000-row group: {groups:?}");
        assert!(groups[0] >= ROW_GROUP_SIZE as u64);
        assert_eq!(groups.iter().sum::<u64>(), rows as u64);
    }

    /// Every partition file must be *physically* ordered by the contract's in-file sort key,
    /// for a dataset with several partitions that each span many chunks.
    ///
    /// The regression: the export used to be one `COPY … PARTITION_BY (chrom)`, and DuckDB's
    /// partition writer does not preserve the statement's `ORDER BY` inside the files it
    /// produces — it buffers and flushes each partition independently of the sort. The
    /// existing coverage could not see it, because the demo VCF puts a single row in each
    /// partition and the bounded-memory fixture happens to land on a chunk boundary that
    /// survives. This shape (six partitions, 2 000 rows each) reproduced it on every run.
    ///
    /// Sortedness is asserted against `file_row_number`, the reader's physical row index. A
    /// bare `lag(pos) OVER ()` would describe whatever order the scan happened to emit, which
    /// is not the question being asked.
    #[test]
    fn every_partition_file_is_physically_sorted_across_many_chunks() {
        const CHROMS: [&str; 6] = ["1", "2", "10", "22", "X", "MT"];
        const PER_CHROM: u32 = 2_000;

        let directory = TempDir::new().expect("temp dir");
        let source = directory.path().join("multi_chrom.vcf");
        {
            let mut writer = BufWriter::new(File::create(&source).expect("create"));
            writer.write_all(VCF_HEADER.as_bytes()).expect("header");
            // Descending within each chromosome and interleaved across them, so nothing but the
            // export can be responsible for the order on disk.
            for index in (0..PER_CHROM).rev() {
                for chrom in CHROMS {
                    writer
                        .write_all(
                            data_line(chrom, 10_000 + index * 100, ".", "A", "C", "0/1").as_bytes(),
                        )
                        .expect("record");
                }
            }
        }

        let (stats, parquet_dir) =
            build_in(&directory, &source, &RecordingProgressSink::default(), 1_000)
                .expect("build succeeds");
        assert_eq!(stats.local_parquet_files.len(), CHROMS.len());
        assert_eq!(stats.variant_count, u64::from(PER_CHROM) * CHROMS.len() as u64);

        let connection = Connection::open_in_memory().expect("connection");
        for file in &stats.local_parquet_files {
            let quoted = sql_path(&parquet_dir.join(&file.relative_path));
            let out_of_order: i64 = scalar(
                &connection,
                &format!(
                    "SELECT count(*) FROM (
                       SELECT (pos, ref, alt) AS current,
                              lag((pos, ref, alt)) OVER (ORDER BY file_row_number) AS previous
                       FROM read_parquet('{quoted}', file_row_number = true)
                     ) WHERE previous IS NOT NULL AND current < previous"
                ),
            );
            assert_eq!(
                out_of_order, 0,
                "'{}' is not physically ordered by (pos, ref, alt)",
                file.relative_path
            );
            assert_eq!(file.row_count, u64::from(PER_CHROM));
            assert_eq!(file.min_pos, 10_000);
            assert_eq!(file.max_pos, 10_000 + (PER_CHROM - 1) * 100);
        }
    }

    /// The whole build, end to end, must produce byte-identical output at every concurrency
    /// bound — the dataset checksum above all, because it is frozen across two languages.
    ///
    /// The per-stage tests in `src/artifact/validate.rs` prove each parallel stage preserves its
    /// own ordering; this one proves the pipeline does, against a real multi-partition export.
    #[test]
    fn the_dataset_is_byte_identical_at_every_concurrency_bound() {
        const PER_CHROM: u32 = 2_000;
        const CHROMS: [&str; 6] = ["1", "12", "2", "22", "X", "MT"];

        let source_directory = TempDir::new().expect("temp dir");
        let source = source_directory.path().join("bounds.vcf");
        {
            let mut writer = BufWriter::new(File::create(&source).expect("create"));
            writer.write_all(VCF_HEADER.as_bytes()).expect("header");
            for chrom in CHROMS {
                for index in 0..PER_CHROM {
                    writer
                        .write_all(
                            data_line(chrom, 10_000 + index * 100, ".", "A", "C", "0/1").as_bytes(),
                        )
                        .expect("record");
                }
            }
        }

        let run = |concurrency| {
            let directory = TempDir::new().expect("temp dir");
            let (stats, parquet_dir) = build_with(
                &directory,
                &source,
                &RecordingProgressSink::default(),
                1_000,
                concurrency,
            )
            .expect("build succeeds");
            // The Parquet bytes themselves, not just the descriptors: hashing the descriptor
            // list would pass even if the files differed and the descriptors were stale.
            let bytes: Vec<Vec<u8>> = stats
                .local_parquet_files
                .iter()
                .map(|file| std::fs::read(parquet_dir.join(&file.relative_path)).expect("read"))
                .collect();
            (stats, bytes)
        };

        let (sequential, sequential_bytes) = run(ConcurrencyLimits::SEQUENTIAL);
        assert_eq!(sequential.local_parquet_files.len(), CHROMS.len());

        for validate_files in [2, 4, 16] {
            let (parallel, parallel_bytes) = run(ConcurrencyLimits {
                validate_files,
                ..ConcurrencyLimits::SEQUENTIAL
            });
            assert_eq!(
                parallel.dataset_checksum_sha256, sequential.dataset_checksum_sha256,
                "the dataset checksum changed at validate_files = {validate_files}"
            );
            assert_eq!(
                parallel.local_parquet_files, sequential.local_parquet_files,
                "the descriptor list changed at validate_files = {validate_files}"
            );
            assert_eq!(
                parallel_bytes, sequential_bytes,
                "the Parquet bytes changed at validate_files = {validate_files}"
            );
            assert_eq!(parallel.variant_count, sequential.variant_count);
            assert_eq!(parallel.rejected_record_count, sequential.rejected_record_count);
        }
    }

    /// Parallel BGZF decompression must not turn "one bounded batch in memory" into "as many
    /// blocks as the pipeline can read ahead of the parser".
    ///
    /// The batch bound is asserted exactly as the plain-source acceptance test asserts it, over a
    /// `bgzip`-compressed source with every stage at its default bound, and the decompression
    /// threads must all be gone by the time the build returns.
    #[test]
    fn a_bgzip_source_keeps_the_batch_bound_and_leaves_no_threads_behind() {
        let _exclusive = exclusive_bgzf();
        const RECORDS: u32 = 60_000;
        const BATCH: usize = 1_000;

        let directory = TempDir::new().expect("temp dir");
        let mut body = String::from(VCF_HEADER);
        for index in 0..RECORDS {
            body.push_str(&data_line(
                ["1", "2", "12", "X", "MT"][index as usize % 5],
                10_000 + index * 13,
                ".",
                "A",
                "C",
                "0/1",
            ));
        }
        let source = directory.path().join("bounded.vcf.gz");
        std::fs::write(&source, bgzf_file(body.as_bytes(), 4_096)).expect("write BGZF VCF");

        let before = live_decompression_threads();
        let sink = RecordingProgressSink::default();
        let (stats, _) = build_with(
            &directory,
            &source,
            &sink,
            BATCH,
            ConcurrencyLimits::default(),
        )
        .expect("build succeeds");

        assert_eq!(stats.variant_count, u64::from(RECORDS));
        let batches: Vec<usize> = sink
            .events()
            .iter()
            .map(|event| event.batch_records)
            .filter(|records| *records > 0)
            .collect();
        assert_eq!(
            batches.iter().sum::<usize>(),
            RECORDS as usize,
            "every accepted record must be reported in exactly one batch"
        );
        assert!(
            batches.iter().max().copied().unwrap_or(0) <= BATCH,
            "a batch of {:?} records exceeds the {BATCH}-record bound",
            batches.iter().max()
        );
        assert_eq!(
            live_decompression_threads(),
            before,
            "the build returned with BGZF decompression threads still running"
        );
    }

    /// Cancelling mid-parse must still stop promptly when the parse is being fed by a running
    /// decompression pipeline, and must not leave that pipeline running.
    #[test]
    fn cancelling_a_bgzip_build_stops_it_and_joins_the_decompression_threads() {
        let _exclusive = exclusive_bgzf();
        let directory = TempDir::new().expect("temp dir");
        let mut body = String::from(VCF_HEADER);
        for index in 0..200_000u32 {
            body.push_str(&data_line("1", 10_000 + index * 13, ".", "A", "C", "0/1"));
        }
        let source = directory.path().join("cancelled.vcf.gz");
        std::fs::write(&source, bgzf_file(body.as_bytes(), 4_096)).expect("write BGZF VCF");

        let before = live_decompression_threads();
        // PARSING, then one WRITING_DUCKDB per batch: stopping at the third abandons the build
        // while the decompression pipeline is certainly still filling its read-ahead.
        let sink = InterruptingProgressSink::new(3);
        let (stats, parquet_dir) = build_interruptibly_with(
            &directory,
            &source,
            &sink,
            1_000,
            ConcurrencyLimits::default(),
        )
        .expect("an interrupted build is not a failure");

        assert!(stats.is_none(), "an interrupted build produces no statistics");
        assert_eq!(
            sink.seen().len(),
            3,
            "the build must stop at the boundary that asked it to"
        );
        assert!(!parquet_dir.exists(), "an interrupted build must not have exported");
        assert_eq!(
            live_decompression_threads(),
            before,
            "a cancelled build left BGZF decompression threads running"
        );
    }

    /// A sink that returns `ControlFlow::Break` must actually stop the build at that boundary —
    /// not merely be noted while a whole-genome parse runs to completion — and the result must
    /// be `Ok(None)`, because stopping on request is not a failure.
    ///
    /// This is the seam a cancelled Temporal Activity uses; the adapter's half is covered in
    /// `temporal_activity_test.rs`.
    #[test]
    fn a_sink_that_breaks_stops_the_build_and_produces_no_statistics() {
        const ROWS: u32 = 20_000;

        let directory = TempDir::new().expect("temp dir");
        let source = directory.path().join("interrupt.vcf");
        {
            let mut writer = BufWriter::new(File::create(&source).expect("create"));
            writer.write_all(VCF_HEADER.as_bytes()).expect("header");
            for index in 0..ROWS {
                writer
                    .write_all(data_line("1", 10_000 + index * 100, ".", "A", "C", "0/1").as_bytes())
                    .expect("record");
            }
        }

        // Boundaries are PARSING, then one WRITING_DUCKDB per 1 000-record batch. Stopping at the
        // third means the build is abandoned early in the parse.
        let sink = InterruptingProgressSink::new(3);
        let (stats, parquet_dir) = build_interruptibly_in(&directory, &source, &sink, 1_000)
            .expect("an interrupted build is not a failure");

        assert!(stats.is_none(), "an interrupted build produces no statistics");
        assert_eq!(
            sink.seen().len(),
            3,
            "the build must stop at the boundary that asked it to, not keep reporting"
        );
        assert!(
            !parquet_dir.exists(),
            "an interrupted build must not have reached the Parquet export"
        );

        // The staging database was created and left where it is: removing the attempt's scratch
        // space is the caller's job, and the caller owns the whole directory.
        assert!(directory.path().join("staging.duckdb").exists());
    }
}

// ---------------------------------------------------------------------------------------
// Parquet inspection helpers, kept independent of the code under test
// ---------------------------------------------------------------------------------------

/// The frozen physical schema: `chrom` lives in the directory name, never in the file.
fn assert_physical_schema(connection: &Connection, quoted_path: &str) {
    let mut statement = connection
        .prepare(&format!(
            "SELECT name, duckdb_type FROM parquet_schema('{quoted_path}') WHERE column_id > 0 ORDER BY column_id"
        ))
        .expect("schema query");
    let columns: Vec<(String, String)> = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("schema rows")
        .map(|row| row.expect("schema row"))
        .collect();
    assert_eq!(
        columns,
        [
            ("pos".to_string(), "UINTEGER".to_string()),
            ("rsid".to_string(), "VARCHAR".to_string()),
            ("ref".to_string(), "VARCHAR".to_string()),
            ("alt".to_string(), "VARCHAR".to_string()),
            ("gt_raw".to_string(), "VARCHAR".to_string()),
        ],
        "the physical Parquet schema must match PARQUET_SCHEMA_COLUMNS and exclude chrom"
    );

    // The columns the contract marks NOT NULL must hold no nulls; rsid is the nullable one.
    let nulls: i64 = scalar(
        connection,
        &format!(
            "SELECT count(*) FROM read_parquet('{quoted_path}')
             WHERE pos IS NULL OR ref IS NULL OR alt IS NULL OR gt_raw IS NULL"
        ),
    );
    assert_eq!(nulls, 0, "a NOT NULL column contains a null");
}

fn row_group_sizes(connection: &Connection, quoted_path: &str) -> Vec<u64> {
    let mut statement = connection
        .prepare(&format!(
            "SELECT DISTINCT row_group_id, row_group_num_rows FROM parquet_metadata('{quoted_path}') ORDER BY row_group_id"
        ))
        .expect("row group query");
    statement
        .query_map([], |row| row.get::<_, i64>(1))
        .expect("row group rows")
        .map(|rows| rows.expect("row group row") as u64)
        .collect()
}

fn string_column(connection: &Connection, sql: &str) -> Vec<String> {
    let mut statement = connection.prepare(sql).expect("prepare");
    statement
        .query_map([], |row| row.get::<_, String>(0))
        .expect("rows")
        .map(|value| value.expect("value"))
        .collect()
}

/// The `read_parquet` option sets `contracts/ingestion-v1.md` distinguishes: the one it
/// forbids, the one it mandates, and the explicit declaration it accepts as equivalent.
const HIVE_ONLY: &str = "hive_partitioning = true";
const HIVE_TYPED: &str = "hive_partitioning = true, hive_types_autocast = 0";
const HIVE_DECLARED: &str = "hive_partitioning = true, hive_types = {'chrom': 'VARCHAR'}";

/// Exports a one-record-per-chromosome dataset and returns its export root.
fn build_partitions(directory: &TempDir, chroms: &[&str]) -> PathBuf {
    let body: String = chroms
        .iter()
        .enumerate()
        .map(|(index, chrom)| data_line(chrom, 100 + index as u32, "rs1", "A", "C", "0/1"))
        .collect();
    let source = write_vcf(directory.path(), "partitions.vcf", &body);
    let (_, parquet_dir) = build_in(directory, &source, &NoopProgressSink, 100).expect("build");
    parquet_dir
}

/// The DuckDB type `read_parquet` gives the reconstructed `chrom` column under `options`.
fn hive_chrom_type(connection: &Connection, target: &str, options: &str) -> String {
    let mut statement = connection
        .prepare(&format!("DESCRIBE SELECT * FROM read_parquet('{target}', {options})"))
        .expect("describe");
    statement
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))
        .expect("describe rows")
        .map(|row| row.expect("describe row"))
        .find(|(name, _)| name == "chrom")
        .map(|(_, column_type)| column_type)
        .expect("chrom must be reconstructed from the partition directory")
}

/// One self-contained gzip member. Concatenating several of these is what `bgzip` produces.
fn gzip_member(text: &str) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(text.as_bytes()).expect("compress");
    encoder.finish().expect("finish member")
}

/// A gzip stream with an intact header and a damaged deflate payload: the failure is in the
/// bytes, so it is deterministic and must never be retried.
fn corrupt_gzip(text: &str) -> Vec<u8> {
    let mut bytes = gzip_member(text);
    // The first twelve bytes are the gzip header, which must stay valid so the corruption is
    // discovered mid-stream rather than at open time.
    for byte in bytes.iter_mut().skip(12).take(8) {
        *byte ^= 0xff;
    }
    bytes
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Writes `count` records round-robin across the acceptance chromosomes through a buffered
/// writer, so the generator itself never holds the dataset in memory either.
fn write_large_vcf(path: &Path, count: u32) {
    let mut writer = BufWriter::new(File::create(path).expect("create large VCF"));
    writer.write_all(VCF_HEADER.as_bytes()).expect("header");
    for index in 0..count {
        let chrom = ACCEPTANCE_CHROMS[index as usize % ACCEPTANCE_CHROMS.len()];
        // Descending source order proves the export, not the input, establishes the sort.
        let position = count - index;
        let rsid = if index % 7 == 0 { ".".to_string() } else { format!("rs{index}") };
        writer
            .write_all(data_line(chrom, position, &rsid, "A", "C", "0|1").as_bytes())
            .expect("record");
    }
}
