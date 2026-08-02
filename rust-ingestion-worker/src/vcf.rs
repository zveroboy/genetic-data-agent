//! Streaming VCF input: read plain or gzipped text and yield one parsed record at a time.
//!
//! The whole file is never materialised — not as a `Vec<String>` of lines, not as a `Vec` of
//! variants. [`VcfRecordReader`] is an `Iterator`, so the caller decides how much to hold.
//!
//! A malformed record is data, not an error: it is yielded as [`VcfRecord::Rejected`] and
//! counted by the caller. Only an I/O failure aborts the stream.
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

/// An iterator over the data records of a VCF stream.
pub struct VcfRecordReader<R> {
    reader: R,
    /// Reused between lines so the iterator allocates once, not once per record.
    line: String,
    line_number: u64,
    bytes_read: u64,
}

impl<R: BufRead> VcfRecordReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            line: String::new(),
            line_number: 0,
            bytes_read: 0,
        }
    }

    /// Uncompressed bytes consumed so far, including line terminators. For a gzipped source
    /// this is the decompressed size, which is what progress should be measured against.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }
}

impl<R: BufRead> Iterator for VcfRecordReader<R> {
    type Item = io::Result<VcfRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.line.clear();
            match self.reader.read_line(&mut self.line) {
                Ok(0) => return None,
                Ok(bytes) => {
                    self.bytes_read += bytes as u64;
                    self.line_number += 1;
                }
                Err(error) => return Some(Err(error)),
            }

            let trimmed = self.line.trim_end_matches(['\n', '\r']);
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            return Some(Ok(parse_data_line(trimmed, self.line_number)));
        }
    }
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
