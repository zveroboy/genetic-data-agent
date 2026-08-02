//! The dataset's content identity: the descriptor of one exported file, their canonical
//! ordering, the canonical descriptor block and the SHA-256 taken over it.
//!
//! This is one half of a cross-language pair. `ts-api-agent/src/application/dataset-checksum.ts`
//! is the other, and the two must agree byte for byte: the canonicalisation is specified in
//! `contracts/ingestion-v1.md` and pinned by a golden fixture in both languages. Keeping the
//! block and the hash alone in one small module is what makes that pair reviewable side by side.

use sha2::{Digest, Sha256};

/// One exported Parquet file, described relative to the export directory.
///
/// This is intentionally *not* a wire type: it has no bucket, key, ETag or version ID.
/// Mapping it onto an S3 object is a later layer's job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalParquetFile {
    /// `chrom=<value>/part-NNN.parquet`, relative to the export directory and never carrying
    /// the `variants/` segment that only exists in S3 keys.
    pub relative_path: String,
    pub chrom: String,
    /// Lowercase hex SHA-256 of the file's bytes.
    pub checksum_sha256: String,
    pub byte_size: u64,
    pub row_count: u64,
    pub min_pos: u32,
    pub max_pos: u32,
    /// Always [`crate::contracts::PARQUET_SCHEMA_FINGERPRINT`]; recorded per file because it is
    /// verified per file rather than assumed.
    pub schema_fingerprint: String,
}

/// Byte-wise ascending by `(chrom, relativePath)`, matching `Ord for str`.
pub(super) fn sort_canonically(files: &mut [LocalParquetFile]) {
    files.sort_by(|left, right| {
        left.chrom
            .as_bytes()
            .cmp(right.chrom.as_bytes())
            .then_with(|| left.relative_path.as_bytes().cmp(right.relative_path.as_bytes()))
    });
}

/// The canonical descriptor block the dataset checksum is taken over.
///
/// One tab-separated line per file, terminated by `\n`, byte-wise sorted by
/// `(chrom, relativePath)`, integers rendered as unpadded base-10. Specified in
/// `contracts/ingestion-v1.md`; `ts-api-agent/src/application/dataset-checksum.ts` is the
/// other implementation.
pub fn canonical_descriptor_block(files: &[LocalParquetFile]) -> String {
    let mut ordered = files.to_vec();
    sort_canonically(&mut ordered);
    ordered
        .iter()
        .map(|file| {
            format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                file.chrom,
                file.relative_path,
                file.checksum_sha256,
                file.byte_size,
                file.row_count,
                file.min_pos,
                file.max_pos
            )
        })
        .collect()
}

/// Deterministic content checksum of a Parquet dataset, independent of any S3 prefix.
pub fn dataset_checksum_sha256(files: &[LocalParquetFile]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(canonical_descriptor_block(files).as_bytes());
    hex::encode(hasher.finalize())
}
