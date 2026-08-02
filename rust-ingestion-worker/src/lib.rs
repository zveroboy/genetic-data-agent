//! The genomic ingestion data plane.
//!
//! [`vcf`] streams and parses VCF input, [`artifact`] stages it in DuckDB and exports a
//! validated, checksummed Parquet dataset, and [`contracts`] holds the frozen cross-language
//! wire types. None of those depends on Temporal or S3.
//!
//! [`object_store`] is the S3/MinIO adapter: it fetches the exact allowlisted source object and
//! publishes the locally enumerated Parquet inventory. It knows nothing about Temporal either;
//! the activity that wires the two together lives in the worker binary.

pub mod artifact;
pub mod contracts;
pub mod models;
pub mod object_store;
pub mod vcf;
