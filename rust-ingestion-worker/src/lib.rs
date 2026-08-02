//! The genomic ingestion data plane.
//!
//! [`vcf`] streams and parses VCF input, [`artifact`] stages it in DuckDB and exports a
//! validated, checksummed Parquet dataset, and [`contracts`] holds the frozen cross-language
//! wire types. None of those depends on Temporal or S3.
//!
//! [`concurrency`] holds the bounds the parallelisable stages run under, and the one ordered,
//! bounded `map` they all go through.
//!
//! [`object_store`] is the S3/MinIO adapter: it fetches the exact allowlisted source object and
//! publishes the locally enumerated Parquet inventory. It knows nothing about Temporal either.
//!
//! [`temporal_activities`] is the only module that does: it wires the processor to the object
//! store, scopes each attempt to its own local workspace and S3 prefix, projects progress onto
//! heartbeats, and maps failures onto the frozen retryability taxonomy. The `temporal_worker`
//! binary is a bootstrap around it and holds no ingestion logic of its own.

pub mod artifact;
pub mod concurrency;
pub mod contracts;
pub mod models;
pub mod object_store;
pub mod temporal_activities;
pub mod vcf;
