//! Streaming VCF input: read plain or gzipped text and yield one parsed record at a time.
//!
//! The whole file is never materialised — not as a `Vec<String>` of lines, not as a `Vec` of
//! variants. [`VcfRecordReader`] is an `Iterator`, so the caller decides how much to hold.
//!
//! A malformed record is data, not an error: it is yielded as [`VcfRecord::Rejected`] and
//! counted by the caller. Only an I/O failure aborts the stream — and one property of the
//! *file* rather than of a line: a `#CHROM` header that does not declare exactly one sample.
//! See [`validate_single_sample`] for why that one cannot be a rejected record.
//!
//! No Temporal, no S3, no DuckDB.

use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

use flate2::read::MultiGzDecoder;

use crate::bgzf::{self, BgzfReader};
use crate::models::UserVariant;

/// Columns a VCF data line must have before it can carry a genotype: the eight fixed
/// columns, then `FORMAT`, then at least one sample column.
pub const MIN_DATA_COLUMNS: usize = 10;

/// Index of the first sample column of a `#CHROM` header line: the eight fixed columns, then
/// `FORMAT`. Tied to [`MIN_DATA_COLUMNS`] rather than spelled out again, because the same layout
/// is what makes `columns[9]` the first sample on a data line.
const FIRST_SAMPLE_COLUMN: usize = MIN_DATA_COLUMNS - 1;

/// How many sample names a rejection message spells out before eliding the rest. A 3,202-sample
/// 1000 Genomes callset must identify itself in the failure an operator reads without putting
/// 3,202 names into a Temporal failure payload.
const NAMED_SAMPLES_IN_ERRORS: usize = 3;

/// The highest human autosome accepted as a partition value.
const MAX_AUTOSOME: u8 = 22;

/// The gzip magic number. Detection is by content, never by file extension.
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// Why a data line did not become a variant. Each one increments the rejected-record count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionReason {
    /// Fewer than [`MIN_DATA_COLUMNS`] tab-separated columns.
    TooFewColumns,
    /// A contig that is not a recognised human chromosome, and so must never reach a
    /// `chrom=<value>` partition directory.
    UnsupportedContig,
    /// `POS` is not a 1-based position representable as `u32`.
    InvalidPosition,
    /// `REF` or `ALT` is empty.
    EmptyAllele,
    /// The `FORMAT` column does not declare a `GT` key.
    MissingGenotypeField,
    /// `FORMAT` declares `GT` but the sample column has no value at that index.
    MissingGenotypeValue,
}

/// The outcome of one VCF data line. Header and blank lines are skipped, never yielded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VcfRecord {
    Variant(UserVariant),
    Rejected {
        /// 1-based line number in the uncompressed source, for diagnostics.
        line_number: u64,
        reason: RejectionReason,
    },
}

/// Passed as `bgzf_workers` to keep decompression on the calling thread — the sequential decoder,
/// whatever the file turns out to be.
pub const SEQUENTIAL_DECOMPRESSION: usize = 1;

/// Opens a VCF for streaming, decompressing it transparently.
///
/// Three paths, chosen from the file's own first bytes and never from its extension:
///
/// - **BGZF**, when the first member carries the `BC` block-size subfield and `bgzf_workers > 1`.
///   Blocks are inflated on `bgzf_workers` threads and reassembled in file order; see
///   [`crate::bgzf`]. This is what `bgzip` writes, so it is what a real GIAB or 1000 Genomes VCF
///   is.
/// - **Plain gzip**, for anything else with the gzip magic — including BGZF when the caller asked
///   for sequential decompression. A single deflate stream cannot be split, so this is
///   [`MultiGzDecoder`] on the calling thread, exactly as before.
/// - **Uncompressed**, for everything else.
///
/// Which one was taken is logged, because "why is this file slow" should not require a debugger.
pub fn open_vcf(path: &Path, bgzf_workers: usize) -> io::Result<VcfRecordReader<Box<dyn BufRead>>> {
    let mut buffered = BufReader::new(File::open(path)?);
    let head = buffered.fill_buf()?;
    let gzipped = head.len() >= GZIP_MAGIC.len() && head[..GZIP_MAGIC.len()] == GZIP_MAGIC;
    let bgzf = gzipped && bgzf::is_bgzf(head);

    let reader: Box<dyn BufRead> = if bgzf && bgzf_workers > SEQUENTIAL_DECOMPRESSION {
        tracing::info!(
            source = %path.display(),
            decompression = "bgzf-parallel",
            workers = bgzf_workers,
            "BGZF detected: decompressing blocks in parallel"
        );
        // A second handle rather than `buffered`, because the pipeline reads from byte zero and
        // `buffered` has already consumed part of the first block into its buffer.
        Box::new(BgzfReader::new(File::open(path)?, bgzf_workers)?)
    } else if gzipped {
        tracing::info!(
            source = %path.display(),
            decompression = "gzip-sequential",
            reason = if bgzf { "sequential decompression requested" } else { "not BGZF: no BC block-size subfield" },
            "decompressing on the reading thread"
        );
        Box::new(BufReader::new(MultiGzDecoder::new(buffered)))
    } else {
        tracing::info!(source = %path.display(), decompression = "none", "input is not compressed");
        Box::new(buffered)
    };
    Ok(VcfRecordReader::new(reader))
}

/// How far the stream has got through verifying its own column layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeaderState {
    /// No `#CHROM` line has been seen yet. `##` metadata lines leave this alone.
    Pending,
    /// `#CHROM` declared exactly one sample column, so `columns[9]` of a data line is that
    /// sample and nothing else is being discarded.
    Declared,
    /// The header fault has been yielded and the stream is over.
    Failed,
}

/// An iterator over the data records of a VCF stream.
pub struct VcfRecordReader<R> {
    reader: R,
    /// Reused between lines so the iterator allocates once, not once per record.
    line: String,
    line_number: u64,
    bytes_read: u64,
    header: HeaderState,
}

impl<R: BufRead> VcfRecordReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            line: String::new(),
            line_number: 0,
            bytes_read: 0,
            header: HeaderState::Pending,
        }
    }

    /// Uncompressed bytes consumed so far, including line terminators. For a gzipped source
    /// this is the decompressed size, which is what progress should be measured against.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Ends the stream on a header fault and hands the error back to be yielded once.
    fn fail(&mut self, error: io::Error) -> io::Error {
        self.header = HeaderState::Failed;
        error
    }

    /// End of input. Reaching it while still [`HeaderState::Pending`] means the source never
    /// declared its columns, which would otherwise be indistinguishable from a legitimate
    /// header-only VCF and be reported as an empty dataset instead of an unverifiable file.
    fn finish(&mut self) -> Option<io::Result<VcfRecord>> {
        if self.header == HeaderState::Pending {
            return Some(Err(self.fail(missing_chrom_header_error())));
        }
        None
    }
}

impl<R: BufRead> Iterator for VcfRecordReader<R> {
    type Item = io::Result<VcfRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        // The header fault has already been yielded. Every remaining line has an unverified
        // layout, so carrying on would hand the caller exactly the mis-attributed genotypes the
        // check exists to prevent.
        if self.header == HeaderState::Failed {
            return None;
        }
        loop {
            self.line.clear();
            match self.reader.read_line(&mut self.line) {
                Ok(0) => return self.finish(),
                Ok(bytes) => {
                    self.bytes_read += bytes as u64;
                    self.line_number += 1;
                }
                Err(error) => return Some(Err(error)),
            }

            let trimmed = self.line.trim_end_matches(['\n', '\r']);
            if trimmed.is_empty() {
                continue;
            }
            if let Some(after_hash) = trimmed.strip_prefix('#') {
                // `##` metadata precedes `#CHROM` and says nothing about the column layout — it
                // is skipped on its prefix alone, so a `##INFO` description containing tabs
                // cannot be mistaken for a column declaration. The single-`#` line is the
                // authoritative one.
                if after_hash.starts_with('#') {
                    continue;
                }
                match validate_single_sample(trimmed) {
                    Ok(()) => {
                        self.header = HeaderState::Declared;
                        continue;
                    }
                    Err(error) => return Some(Err(self.fail(error))),
                }
            }
            if self.header == HeaderState::Pending {
                return Some(Err(self.fail(missing_chrom_header_error())));
            }
            return Some(Ok(parse_data_line(trimmed, self.line_number)));
        }
    }
}

/// Accepts a `#CHROM` header line only if it declares exactly one sample column.
///
/// The header is the file's own statement of how many genomes it holds: eight fixed columns,
/// then `FORMAT`, then one column per sample. [`parse_data_line`] reads `columns[9]` and nothing
/// after it, and [`MIN_DATA_COLUMNS`] is a floor, so without this check a joint callset — a
/// 1000 Genomes file carries 3,202 samples — ingests the first sample, silently discards the
/// other 3,201, and publishes a dataset that claims to be one person's genome.
///
/// Sample *selection* is deliberately not the fix: [`UserVariant`] has no sample field, the
/// manifest records no sample identity and the Parquet schema is frozen by a fingerprint, so
/// "sample N was chosen" could not be written down anywhere. The dataset would claim to be about
/// a person it cannot name — wrong data presented as right data, which is worse than a failure.
/// One dataset is one genome; a joint callset is out of scope and the honest answer is a refusal
/// that tells the operator how to proceed.
///
/// Zero samples is rejected for a different reason: a sites-only VCF stops at `INFO` (or carries
/// `FORMAT` with no sample after it) and holds no genotypes at all, so there is nothing here for
/// this pipeline to ingest.
///
/// The failure is an [`io::Error`] with [`io::ErrorKind::InvalidData`] rather than a new error
/// type because that is the seam the caller already has: the reader is an
/// `Iterator<Item = io::Result<VcfRecord>>` and `crate::artifact::staging` funnels both
/// `open_vcf` and per-record errors through one classifier that already treats `InvalidData` as
/// deterministic content — mapping it onto the non-retryable
/// `crate::contracts::FailureType::InvalidVcfFormat`. A bespoke error would need a second
/// classification path to reach the same verdict. A [`VcfRecord::Rejected`] is not an option at
/// all: those only increment a counter and let the ingestion succeed.
fn validate_single_sample(header_line: &str) -> io::Result<()> {
    let columns: Vec<&str> = header_line.split('\t').collect();
    let samples = columns.get(FIRST_SAMPLE_COLUMN..).unwrap_or(&[]);
    if samples.len() == 1 {
        return Ok(());
    }

    let named = if samples.is_empty() {
        "no FORMAT or sample column: a sites-only VCF carries no genotypes".to_string()
    } else {
        let listed = samples[..samples.len().min(NAMED_SAMPLES_IN_ERRORS)].join(", ");
        let elided = if samples.len() > NAMED_SAMPLES_IN_ERRORS { ", ..." } else { "" };
        format!("{listed}{elided}")
    };
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "the #CHROM header declares {} sample columns ({named}); this pipeline ingests \
             exactly one sample per dataset, because one dataset is one person's genome and \
             nothing in the schema can record which sample a genotype came from. Extract a \
             single sample first: `bcftools view -s <SAMPLE> <source> -Oz -o single-sample.vcf.gz`",
            samples.len()
        ),
    ))
}

/// The refusal for a source with no `#CHROM` line at all: same reasoning, one step earlier.
/// Without the header the column layout is undeclared, so the single-sample requirement is a
/// guess about a data line's tenth column rather than a checked property of the file.
fn missing_chrom_header_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "the VCF has no #CHROM header line, so its column layout is undeclared and the number \
         of samples it carries cannot be verified: a data line's tenth column might be the only \
         sample or the first of thousands. Supply a VCF whose #CHROM header names exactly one \
         sample: `bcftools view -s <SAMPLE> <source> -Oz -o single-sample.vcf.gz`",
    )
}

/// Parses one VCF data line. Never panics and never fails: an unusable line becomes a
/// [`VcfRecord::Rejected`].
pub fn parse_data_line(line: &str, line_number: u64) -> VcfRecord {
    let reject = |reason| VcfRecord::Rejected { line_number, reason };

    let columns: Vec<&str> = line.split('\t').collect();
    if columns.len() < MIN_DATA_COLUMNS {
        return reject(RejectionReason::TooFewColumns);
    }

    let Some(chrom) = normalize_chromosome(columns[0]) else {
        return reject(RejectionReason::UnsupportedContig);
    };

    // VCF positions are 1-based, so 0 is as invalid as a non-numeric or out-of-range value.
    let position = match columns[1].parse::<u32>() {
        Ok(0) | Err(_) => return reject(RejectionReason::InvalidPosition),
        Ok(position) => position,
    };

    let (reference, alternate) = (columns[3], columns[4]);
    if reference.is_empty() || alternate.is_empty() {
        return reject(RejectionReason::EmptyAllele);
    }

    // GT is located by its index in FORMAT; it is conventionally first but need not be.
    let Some(genotype_index) = columns[8].split(':').position(|key| key == "GT") else {
        return reject(RejectionReason::MissingGenotypeField);
    };
    let genotype = match columns[9].split(':').nth(genotype_index) {
        Some(value) if !value.is_empty() => value,
        _ => return reject(RejectionReason::MissingGenotypeValue),
    };

    VcfRecord::Variant(UserVariant {
        chrom,
        pos: position,
        rsid: match columns[2] {
            "." | "" => None,
            identifier => Some(identifier.to_string()),
        },
        ref_allele: reference.to_string(),
        alt_allele: alternate.to_string(),
        gt_raw: genotype.to_string(),
    })
}

/// Maps a source contig onto the canonical chromosome name used as a partition value, or
/// `None` if it is not a recognised human chromosome.
///
/// This allowlist is the only thing standing between an attacker-controlled `#CHROM` column
/// and a `chrom=<value>` directory name, so it is a whitelist of shapes rather than a
/// blacklist of characters: every accepted value consists solely of ASCII digits and
/// uppercase letters and can never contain `/`, `..`, `=`, a quote or a control character.
///
/// `chr` prefixes are stripped (UCSC vs Ensembl naming) and the mitochondrion is normalised
/// to `MT`, so `chrM` and `MT` do not become two partitions of the same contig.
pub fn normalize_chromosome(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    let bare = match trimmed.get(..3) {
        Some(prefix) if prefix.eq_ignore_ascii_case("chr") => &trimmed[3..],
        _ => trimmed,
    };
    let upper = bare.to_ascii_uppercase();

    match upper.as_str() {
        "X" => Some("X".to_string()),
        "Y" => Some("Y".to_string()),
        "M" | "MT" => Some("MT".to_string()),
        candidate if is_autosome(candidate) => Some(candidate.to_string()),
        _ => None,
    }
}

/// `1`..=`22`, written without a leading zero.
fn is_autosome(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 2
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && !value.starts_with('0')
        && value.parse::<u8>().is_ok_and(|number| (1..=MAX_AUTOSOME).contains(&number))
}
