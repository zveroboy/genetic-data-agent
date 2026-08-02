//! The genomic ingestion data plane.
//!
//! [`vcf`] streams and parses VCF input, [`artifact`] stages it in DuckDB and exports a
//! validated, checksummed Parquet dataset, and [`contracts`] holds the frozen cross-language
//! wire types. None of them depends on Temporal or S3; those live in the worker binary.

pub mod artifact;
pub mod contracts;
pub mod models;
pub mod vcf;
