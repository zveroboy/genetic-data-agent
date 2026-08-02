//! The dataset's on-disk layout, shared by the export and the validation: the frozen partition
//! and file-name shapes, the deterministic directory listing both walk, and the SQL literals a
//! path or a chromosome value becomes inside a DuckDB statement.
//!
//! The names here are contract surface, not implementation detail: `relativePath` is built from
//! them, and the cross-language dataset checksum is computed from `relativePath`.

use std::path::{Path, PathBuf};

use super::ArtifactError;

/// The partition directory prefix. `=` appears in an object key only here.
pub(super) const PARTITION_PREFIX: &str = "chrom=";

/// The contract's Parquet file name is `part-NNN.parquet`: this prefix, then exactly
/// [`PART_FILE_DIGITS`] ASCII digits, then this extension.
const PART_FILE_PREFIX: &str = "part-";
const PART_FILE_SUFFIX: &str = ".parquet";
const PART_FILE_DIGITS: usize = 3;

/// One past the largest index `part-NNN` can express. A partition holding more files than this
/// cannot be named under the contract, so it is refused rather than silently widened to
/// `part-1000.parquet`.
const MAX_PARTITION_FILES: usize = 1_000;

/// The contract's `part-NNN.parquet` name for the `index`-th file of a partition.
///
/// `{index:03}` pads but does not truncate, so index 1000 would silently produce
/// `part-1000.parquet` and break the frozen `NNN` shape — and with it `relativePath`, which the
/// cross-language dataset checksum is computed from. DuckDB writes one file per partition
/// today, so this is unreachable; it fails loudly rather than degrading quietly if that ever
/// changes.
pub(super) fn part_file_name(index: usize, partition: &Path) -> Result<String, ArtifactError> {
    if index >= MAX_PARTITION_FILES {
        return Err(ArtifactError::ValidationFailed(format!(
            "partition '{}' produced more than {MAX_PARTITION_FILES} Parquet files; the \
             contract's '{PART_FILE_PREFIX}NNN{PART_FILE_SUFFIX}' name cannot express index {index}",
            partition.display()
        )));
    }
    Ok(format!(
        "{PART_FILE_PREFIX}{index:0width$}{PART_FILE_SUFFIX}",
        width = PART_FILE_DIGITS
    ))
}

/// Whether a file name is exactly the contract's `part-NNN.parquet` shape.
fn is_canonical_part_name(name: &str) -> bool {
    let Some(digits) = name
        .strip_prefix(PART_FILE_PREFIX)
        .and_then(|rest| rest.strip_suffix(PART_FILE_SUFFIX))
    else {
        return false;
    };
    digits.len() == PART_FILE_DIGITS && digits.bytes().all(|byte| byte.is_ascii_digit())
}

/// The `chrom=<value>/part-NNN.parquet` descriptor for one exported file.
///
/// The name is taken from disk but not trusted: `relative_path` is the single string the
/// cross-language dataset checksum is most sensitive to, so its shape is asserted here rather
/// than assumed from the fact that [`super::export::rename_partition_files`] just ran.
pub(super) fn canonical_relative_path(
    directory_name: &str,
    name: &str,
) -> Result<String, ArtifactError> {
    if !is_canonical_part_name(name) {
        return Err(ArtifactError::ValidationFailed(format!(
            "'{directory_name}/{name}' is not a '{PART_FILE_PREFIX}NNN{PART_FILE_SUFFIX}' file; \
             the relative path shape is frozen because the dataset checksum is computed from it"
        )));
    }
    Ok(format!("{directory_name}/{name}"))
}

/// The integer at the end of a file stem, used only to order DuckDB's `data_N` output.
pub(super) fn trailing_index(path: &Path) -> u64 {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.rsplit(['_', '-']).next())
        .and_then(|suffix| suffix.parse::<u64>().ok())
        .unwrap_or(u64::MAX)
}

pub(super) fn read_sorted_dir(directory: &Path) -> Result<Vec<PathBuf>, ArtifactError> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(directory)
        .map_err(|error| {
            ArtifactError::WriteFailed(format!("cannot list '{}': {error}", directory.display()))
        })?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|error| ArtifactError::WriteFailed(format!("cannot list '{}': {error}", directory.display())))
        })
        .collect::<Result<_, _>>()?;
    entries.sort();
    Ok(entries)
}

/// Renders a path as a single-quoted SQL literal, doubling embedded quotes.
pub(super) fn sql_string_literal(path: &Path) -> Result<String, ArtifactError> {
    let text = path
        .to_str()
        .ok_or_else(|| ArtifactError::WriteFailed(format!("non-UTF-8 path '{}'", path.display())))?;
    Ok(sql_text_literal(text))
}

/// Renders a string as a single-quoted SQL literal, doubling embedded quotes.
pub(super) fn sql_text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::contracts::FailureType;

    #[test]
    fn quotes_paths_containing_a_single_quote() {
        assert_eq!(
            sql_string_literal(Path::new("/tmp/it's here/out")).unwrap(),
            "'/tmp/it''s here/out'"
        );
    }

    #[test]
    fn orders_duckdb_partition_files_numerically() {
        assert_eq!(trailing_index(Path::new("/x/data_2.parquet")), 2);
        assert_eq!(trailing_index(Path::new("/x/data_10.parquet")), 10);
        assert_eq!(trailing_index(Path::new("/x/part-007.parquet")), 7);
    }

    /// `part-NNN` cannot express a four-digit index, so it must refuse rather than widen.
    #[test]
    fn part_file_names_are_three_digits_and_refuse_to_widen() {
        let partition = Path::new("/x/chrom=1");
        assert_eq!(part_file_name(0, partition).unwrap(), "part-000.parquet");
        assert_eq!(part_file_name(7, partition).unwrap(), "part-007.parquet");
        assert_eq!(part_file_name(999, partition).unwrap(), "part-999.parquet");

        let error = part_file_name(1_000, partition).expect_err("part-1000 breaks the NNN shape");
        assert_eq!(error.failure_type(), FailureType::ArtifactValidationFailed);
    }

    #[test]
    fn only_the_contract_file_name_shape_becomes_a_relative_path() {
        assert_eq!(
            canonical_relative_path("chrom=X", "part-000.parquet").unwrap(),
            "chrom=X/part-000.parquet"
        );

        for rejected in [
            "data_0.parquet",          // DuckDB's own name, if the rename were ever skipped
            "part-0.parquet",          // unpadded
            "part-0000.parquet",       // widened past NNN
            "part-00a.parquet",        // not digits
            "part-000.parquet.tmp",    // a staging leftover
            "part-000.PARQUET",        // wrong case
            "part-000",                // no extension
            ".part-000.parquet",       // hidden file
            "",
        ] {
            let Err(error) = canonical_relative_path("chrom=1", rejected) else {
                panic!("'{rejected}' must not reach a relativePath");
            };
            assert_eq!(
                error.failure_type(),
                FailureType::ArtifactValidationFailed,
                "'{rejected}' must be a validation failure"
            );
        }
    }
}
