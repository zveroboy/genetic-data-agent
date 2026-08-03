//! Unit tests for the Temporal adapter layer: failure mapping, the heartbeat projection,
//! cancellation, attempt-scoped naming and the local→S3 inventory mapping.
//!
//! Nothing here needs a Temporal server, MinIO or DuckDB. The adapter is deliberately built
//! around two seams so it can be driven in isolation:
//!
//! - [`HeartbeatChannel`] stands in for `ActivityContext`, which cannot be constructed outside
//!   a running worker. A recording fake is used instead.
//! - Failures are mapped through [`IngestionFailure`], a plain value, before they ever become a
//!   Temporal `ApplicationFailure`, so retryability is assertable without the SDK's plumbing.
//!
//! The end-to-end proof that the *real* activity works lives in
//! `tests/integration/temporal_rust_probe.test.ts`.

mod temporal_activity {
    use std::ops::ControlFlow;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use rust_ingestion_worker::artifact::{dataset_checksum_sha256, ArtifactError, LocalParquetFile};
    use rust_ingestion_worker::contracts::{
        ArtifactTarget, BuildDatasetArtifactInput, ContractVersion, DatasetKey, FailureType,
        IngestionHeartbeat, IngestionPhase, ParquetObject, ReferenceSelector, SourceObject,
        PARQUET_SCHEMA_FINGERPRINT, VARIANTS_SEGMENT,
    };
    use rust_ingestion_worker::models::{ProgressEvent, ProgressSink};
    use rust_ingestion_worker::object_store::{ObjectStoreError, UploadedObject};
    use rust_ingestion_worker::temporal_activities::{
        assert_inventory_checksum, attempt_prefix_for, attempt_workspace_name,
        derived_allowed_prefix, object_key_for, phase_rank, published_inventory,
        validated_attempt_prefix, AttemptWorkspace, HeartbeatChannel, HeartbeatReporter,
        IngestionFailure, KeepaliveTick, ACTIVITY_TYPE, KEEPALIVE_BUDGET_TICKS, PHASE_ORDER,
        TASK_QUEUE, WORKER_IDENTITY_PREFIX,
    };
    use serde_json::{json, Value};
    use tempfile::TempDir;

    // -----------------------------------------------------------------------------------
    // Fakes
    // -----------------------------------------------------------------------------------

    /// Records every heartbeat the adapter emits and can pretend the activity was cancelled.
    ///
    /// Heartbeats are kept as the serialized JSON *text*, because the field order of the frozen
    /// payload is part of what is under test and `serde_json::Value` would sort it away.
    #[derive(Default)]
    struct RecordingChannel {
        recorded: Mutex<Vec<String>>,
        cancelled: AtomicBool,
    }

    impl RecordingChannel {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn cancel(&self) {
            self.cancelled.store(true, Ordering::SeqCst);
        }

        /// The recorded heartbeats as JSON text, in order.
        fn rendered(&self) -> Vec<String> {
            self.recorded.lock().expect("heartbeat log").clone()
        }

        fn heartbeats(&self) -> Vec<Value> {
            self.rendered()
                .iter()
                .map(|text| serde_json::from_str(text).expect("a heartbeat is valid JSON"))
                .collect()
        }

        fn phases(&self) -> Vec<String> {
            self.heartbeats()
                .iter()
                .map(|value| value["phase"].as_str().expect("a phase string").to_string())
                .collect()
        }
    }

    /// The contract's phase list is spelled "Phases, **in order**", so a consumer polling
    /// heartbeats may read a phase as progress. Every published sequence has to be non-regressing
    /// against that order — not merely drawn from the right *set*, which is what a `Set`
    /// comparison would check and what let `FINALIZING`-before-`UPLOADING_PARTITION` through.
    fn assert_phases_never_regress(phases: &[String]) {
        let ranks: Vec<usize> = phases
            .iter()
            .map(|name| {
                let phase = PHASE_ORDER
                    .iter()
                    .copied()
                    .find(|candidate| {
                        serde_json::to_value(candidate).expect("a phase serializes") == json!(name)
                    })
                    .unwrap_or_else(|| panic!("'{name}' is not a contract phase"));
                phase_rank(phase)
            })
            .collect();

        for window in ranks.windows(2) {
            assert!(
                window[1] >= window[0],
                "the published phase sequence regresses: {phases:?}"
            );
        }
    }

    impl HeartbeatChannel for RecordingChannel {
        fn record(&self, heartbeat: &IngestionHeartbeat) {
            let rendered =
                serde_json::to_string(heartbeat).expect("a heartbeat serializes to JSON");
            self.recorded.lock().expect("heartbeat log").push(rendered);
        }

        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::SeqCst)
        }
    }

    fn reporter() -> (Arc<RecordingChannel>, HeartbeatReporter) {
        let channel = RecordingChannel::new();
        (channel.clone(), HeartbeatReporter::new(channel))
    }

    // -----------------------------------------------------------------------------------
    // Fixtures
    // -----------------------------------------------------------------------------------

    const DIGEST_ONE: &str = "a9c0c80616a32401981426fc9ff39d835437416eaec480f755cf90eac0fac442";
    const DIGEST_TWELVE: &str = "a63f7af57491ba32774f0743f951b0b4d79094215013643caf41520013510464";

    fn input() -> BuildDatasetArtifactInput {
        BuildDatasetArtifactInput {
            contract_version: ContractVersion::CURRENT,
            dataset_id: "ds-test-001".to_string(),
            dataset_key: DatasetKey::DemoSmall,
            source: SourceObject {
                bucket: "genomic-data".to_string(),
                key: "samples/demo_user.vcf".to_string(),
                etag: "fixture-etag".to_string(),
                version_id: None,
                content_length: 1024,
            },
            reference: ReferenceSelector {
                build: "GRCh38".to_string(),
                version: "demo-clinvar-grch38-v3".to_string(),
            },
            target: ArtifactTarget {
                bucket: "genomic-artifacts".to_string(),
                artifact_version: "iv-test-001".to_string(),
                allowed_prefix: "datasets/ds-test-001/versions/iv-test-001/".to_string(),
            },
        }
    }

    fn local_file(chrom: &str, checksum: &str, byte_size: u64, row_count: u64) -> LocalParquetFile {
        LocalParquetFile {
            relative_path: format!("chrom={chrom}/part-000.parquet"),
            chrom: chrom.to_string(),
            checksum_sha256: checksum.to_string(),
            byte_size,
            row_count,
            min_pos: 1,
            max_pos: 1_000,
            schema_fingerprint: PARQUET_SCHEMA_FINGERPRINT.to_string(),
        }
    }

    fn uploaded(bucket: &str, key: &str, etag: &str, version_id: Option<&str>) -> UploadedObject {
        UploadedObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            etag: etag.to_string(),
            version_id: version_id.map(str::to_string),
        }
    }

    // -----------------------------------------------------------------------------------
    // Step 1 — failure mapping
    // -----------------------------------------------------------------------------------

    /// Every deterministic failure must reach Temporal as an `ApplicationFailure` whose *type*
    /// is one of the frozen names the TypeScript workflow lists in `nonRetryableErrorTypes`,
    /// and which is additionally flagged non-retryable so a worker misconfiguration on the
    /// TypeScript side cannot resurrect a retry that can only fail again.
    #[test]
    fn deterministic_failures_become_non_retryable_application_failures() {
        let cases: Vec<(IngestionFailure, &str)> = vec![
            (
                ArtifactError::InvalidVcf("cannot decode /tmp/x.vcf".into()).into(),
                "InvalidVcfFormat",
            ),
            (
                ObjectStoreError::SourceChanged("'b/k' before the download".into()).into(),
                "SourceObjectChanged",
            ),
            (
                ArtifactError::ValidationFailed("'chrom=1/part-000.parquet' is empty".into()).into(),
                "ArtifactValidationFailed",
            ),
            (
                ObjectStoreError::ValidationFailed("key is outside the attempt prefix".into()).into(),
                "ArtifactValidationFailed",
            ),
            (
                IngestionFailure::validation("attempt prefix escapes the allowed prefix"),
                "ArtifactValidationFailed",
            ),
        ];

        for (failure, expected_type) in cases {
            let message = failure.to_string();
            let application = failure.into_application_failure();
            assert_eq!(
                application.type_name(),
                Some(expected_type),
                "the Temporal failure type is matched by value on the TypeScript side"
            );
            assert!(
                application.is_non_retryable(),
                "{expected_type} is deterministic and must not be retried"
            );
            assert_eq!(
                application.to_string(),
                message,
                "the diagnostic must survive the conversion"
            );
        }
    }

    /// An object-store connection failure and a temporary disk failure say nothing about the
    /// input, so both stay retryable: the workflow's `maximumAttempts: 3` is what should decide.
    #[test]
    fn transient_failures_stay_retryable() {
        let cases: Vec<(IngestionFailure, &str)> = vec![
            (
                ObjectStoreError::Unavailable("connection reset by peer".into()).into(),
                "ObjectStoreUnavailable",
            ),
            (
                ObjectStoreError::Configuration("S3_ENDPOINT is not set".into()).into(),
                "ObjectStoreUnavailable",
            ),
            (
                ArtifactError::WriteFailed("cannot create staging database: ENOSPC".into()).into(),
                "ArtifactWriteFailed",
            ),
            (
                ObjectStoreError::WriteFailed("cannot read '/tmp/part-000.parquet'".into()).into(),
                "ArtifactWriteFailed",
            ),
        ];

        for (failure, expected_type) in cases {
            let application = failure.into_application_failure();
            assert_eq!(application.type_name(), Some(expected_type));
            assert!(
                !application.is_non_retryable(),
                "{expected_type} is environmental and must stay retryable"
            );
        }
    }

    /// The adapter must not invent its own retry policy: retryability comes from the frozen
    /// taxonomy, so the two can never disagree.
    #[test]
    fn retryability_is_taken_from_the_frozen_taxonomy() {
        for failure_type in FailureType::ALL {
            let application = IngestionFailure::new(failure_type, "boom").into_application_failure();
            assert_eq!(application.type_name(), Some(failure_type.as_str()));
            assert_eq!(
                application.is_non_retryable(),
                !failure_type.is_retryable(),
                "{failure_type} disagrees with contracts::FailureType::is_retryable"
            );
        }
    }

    // -----------------------------------------------------------------------------------
    // Step 2 — the heartbeat adapter
    // -----------------------------------------------------------------------------------

    /// The frozen payload from `contracts/ingestion-v1.md`, reproduced exactly: six camelCase
    /// keys, no more and no fewer.
    #[test]
    fn heartbeat_payloads_match_the_frozen_shape() {
        let (channel, reporter) = reporter();

        reporter.absorb(&ProgressEvent {
            processed_bytes: 4096,
            processed_variants: 2500,
            current_partition: Some("12".to_string()),
            completed_files: 3,
            ..ProgressEvent::phase(IngestionPhase::Parsing)
        });
        reporter.note_uploaded_bytes(1_048_576);
        reporter.emit(IngestionPhase::Parsing, Some("12"));

        let heartbeats = channel.heartbeats();
        assert_eq!(heartbeats.len(), 1);
        assert_eq!(
            heartbeats[0],
            json!({
                "phase": "PARSING",
                "processedBytes": 4096,
                "processedVariants": 2500,
                "currentPartition": "12",
                "completedFiles": 3,
                "uploadedBytes": 1048576
            })
        );
    }

    /// All six phases are reported, in the contract's order, and `currentPartition` is present
    /// as an explicit `null` whenever no partition is being worked on — TypeScript spells the
    /// field `.nullable()`, not `.optional()`.
    ///
    /// The sequence below is the one the *Activity* drives, in the order it drives it: the
    /// adapter emits `DOWNLOADING_SOURCE`, the processor reports through `ProgressSink` until its
    /// local build is done, and only then does the adapter emit its own `UPLOADING_PARTITION` and
    /// `FINALIZING`. Restating the contract's order by hand would prove nothing about that.
    #[test]
    fn reports_every_phase_and_a_null_partition_when_there_is_none() {
        let (channel, reporter) = reporter();

        reporter.emit(IngestionPhase::DownloadingSource, None);
        for phase in [
            IngestionPhase::Parsing,
            IngestionPhase::WritingDuckdb,
            IngestionPhase::ExportingParquet,
        ] {
            let _ = reporter.report(&ProgressEvent::phase(phase));
        }
        reporter.emit(IngestionPhase::UploadingPartition, Some("1"));
        reporter.emit(IngestionPhase::Finalizing, None);

        assert_eq!(
            channel.phases(),
            [
                "DOWNLOADING_SOURCE",
                "PARSING",
                "WRITING_DUCKDB",
                "EXPORTING_PARQUET",
                "UPLOADING_PARTITION",
                "FINALIZING",
            ]
        );
        assert_phases_never_regress(&channel.phases());

        // The wire text itself, not a re-serialization of it: the frozen payload has exactly
        // these six camelCase keys, in this order.
        for rendered in channel.rendered() {
            let keys: Vec<&str> = rendered
                .trim_matches(|c| c == '{' || c == '}')
                .split(',')
                .map(|field| field.split(':').next().expect("a field").trim_matches('"'))
                .collect();
            assert_eq!(
                keys,
                [
                    "phase",
                    "processedBytes",
                    "processedVariants",
                    "currentPartition",
                    "completedFiles",
                    "uploadedBytes"
                ],
                "the heartbeat payload shape is frozen: {rendered}"
            );
        }

        let heartbeats = channel.heartbeats();
        assert!(heartbeats[0]["currentPartition"].is_null());
        assert_eq!(heartbeats[4]["currentPartition"], json!("1"));
        assert!(heartbeats[5]["currentPartition"].is_null());
    }

    /// The adapter is a projection of the processor's `ProgressEvent`, not a second source of
    /// truth: the counters come straight from the event, and `batchRecords` — which exists only
    /// for the bounded-memory test — never reaches the wire.
    #[test]
    fn projects_processor_progress_events_onto_the_wire_heartbeat() {
        let (channel, reporter) = reporter();

        let _ = reporter.report(&ProgressEvent {
            processed_bytes: 8_192,
            processed_variants: 5_000,
            current_partition: Some("X".to_string()),
            completed_files: 2,
            batch_records: 10_000,
            phase: IngestionPhase::WritingDuckdb,
        });

        assert_eq!(
            channel.heartbeats()[0],
            json!({
                "phase": "WRITING_DUCKDB",
                "processedBytes": 8192,
                "processedVariants": 5000,
                "currentPartition": "X",
                "completedFiles": 2,
                "uploadedBytes": 0
            })
        );
    }

    /// Uploaded bytes accumulate across partitions while the counters the processor already
    /// established stay put: the upload stage adds to the picture rather than resetting it.
    #[test]
    fn accumulates_uploaded_bytes_across_partitions() {
        let (channel, reporter) = reporter();

        let _ = reporter.report(&ProgressEvent {
            processed_bytes: 1_024,
            processed_variants: 1_500,
            completed_files: 2,
            ..ProgressEvent::phase(IngestionPhase::Finalizing)
        });
        reporter.note_uploaded_bytes(20_480);
        reporter.emit(IngestionPhase::UploadingPartition, Some("1"));
        reporter.note_uploaded_bytes(15_360);
        reporter.emit(IngestionPhase::UploadingPartition, Some("12"));

        let heartbeats = channel.heartbeats();
        assert_eq!(heartbeats[1]["uploadedBytes"], json!(20_480));
        assert_eq!(heartbeats[2]["uploadedBytes"], json!(35_840));
        assert_eq!(
            heartbeats[2]["completedFiles"],
            json!(2),
            "the export's file count must not be reset by the upload stage"
        );
        assert_eq!(heartbeats[2]["processedVariants"], json!(1_500));
    }

    /// The regression: the processor's last observation used to arrive with the phase
    /// `FINALIZING`, and the reporter passed it straight to the wire. Uploads then ran and
    /// emitted `UPLOADING_PARTITION`, and the adapter emitted `FINALIZING` again — so a consumer
    /// polling heartbeats saw the run reach the terminal phase, regress, and reach it again.
    ///
    /// `FINALIZING` is the adapter's to publish, exactly once, after the uploads. Whatever the
    /// processor reports, the projection must not let it through as the terminal phase.
    #[test]
    fn the_processor_can_never_drive_the_wire_to_the_terminal_phase() {
        let (channel, reporter) = reporter();

        reporter.emit(IngestionPhase::DownloadingSource, None);
        let _ = reporter.report(&ProgressEvent::phase(IngestionPhase::Parsing));
        // The processor signalling "my local build is complete".
        let _ = reporter.report(&ProgressEvent {
            processed_variants: 1_000,
            completed_files: 6,
            ..ProgressEvent::phase(IngestionPhase::Finalizing)
        });
        reporter.emit(IngestionPhase::UploadingPartition, Some("1"));
        reporter.emit(IngestionPhase::Finalizing, None);

        let phases = channel.phases();
        assert_eq!(
            phases,
            [
                "DOWNLOADING_SOURCE",
                "PARSING",
                "EXPORTING_PARQUET",
                "UPLOADING_PARTITION",
                "FINALIZING",
            ],
            "a processor event must never be published as FINALIZING"
        );
        assert_phases_never_regress(&phases);
        assert_eq!(
            phases.iter().filter(|phase| *phase == "FINALIZING").count(),
            1,
            "FINALIZING is published once, by the adapter, and is terminal"
        );
        // The counters the absorbed event carried are still projected — only the phase is.
        assert_eq!(channel.heartbeats()[2]["completedFiles"], json!(6));
        assert_eq!(channel.heartbeats()[2]["processedVariants"], json!(1_000));
    }

    /// The keepalive re-sends the last observation unchanged, so a long uninterruptible stage —
    /// a multi-gigabyte download, a per-partition DuckDB `COPY` — cannot trip the 15-second
    /// `heartbeatTimeout` the workflow sets.
    #[test]
    fn the_keepalive_re_emits_the_last_observation_unchanged() {
        let (channel, reporter) = reporter();

        reporter.emit(IngestionPhase::DownloadingSource, None);
        assert_eq!(reporter.reemit(), KeepaliveTick::Published);
        assert_eq!(reporter.reemit(), KeepaliveTick::Published);

        let heartbeats = channel.heartbeats();
        assert_eq!(heartbeats.len(), 3);
        assert_eq!(heartbeats[0], heartbeats[1]);
        assert_eq!(heartbeats[1], heartbeats[2]);
    }

    /// …but only for a bounded stretch. An unbounded keepalive re-reports a *stalled* worker
    /// just as faithfully as a busy one, which converts the workflow's 15-second
    /// `heartbeatTimeout` into its 30-minute `startToCloseTimeout`: a hung S3 read, a DuckDB
    /// deadlock in the per-partition `COPY` or a wedged upload would all be invisible until then.
    ///
    /// Once the budget is spent the reporter publishes nothing, the server stops seeing
    /// heartbeats, and its own timeout gets to do its job.
    #[test]
    fn the_keepalive_stops_re_emitting_a_stalled_observation() {
        let (channel, reporter) = reporter();
        reporter.emit(IngestionPhase::WritingDuckdb, None);

        for tick in 0..KEEPALIVE_BUDGET_TICKS {
            assert_eq!(
                reporter.reemit(),
                KeepaliveTick::Published,
                "tick {tick} is still inside the budget"
            );
        }
        for _ in 0..10 {
            assert_eq!(
                reporter.reemit(),
                KeepaliveTick::BudgetSpent,
                "a stalled observation must not be carried past the budget"
            );
        }
        assert_eq!(
            channel.heartbeats().len(),
            1 + KEEPALIVE_BUDGET_TICKS as usize,
            "nothing may reach the wire once the budget is spent"
        );
    }

    /// The bound detects a *stall*, not a long healthy activity: any real progress is a new
    /// observation, and restores the whole budget.
    #[test]
    fn progress_restores_the_keepalive_budget() {
        let (channel, reporter) = reporter();
        reporter.emit(IngestionPhase::DownloadingSource, None);

        for _ in 0..KEEPALIVE_BUDGET_TICKS {
            reporter.reemit();
        }
        assert_eq!(reporter.reemit(), KeepaliveTick::BudgetSpent);

        // One partition finishes uploading: a genuinely new observation.
        reporter.note_uploaded_bytes(4_096);
        reporter.emit(IngestionPhase::UploadingPartition, Some("1"));
        assert_eq!(reporter.reemit(), KeepaliveTick::Published);

        let published = channel.heartbeats().len();
        assert_eq!(published, 1 + KEEPALIVE_BUDGET_TICKS as usize + 2);
        assert_phases_never_regress(&channel.phases());
    }

    // -----------------------------------------------------------------------------------
    // Step 4 — cancellation
    // -----------------------------------------------------------------------------------

    /// The processor is a blocking call, so a progress boundary is the only place it can be
    /// stopped. Once the activity is cancelled the very next boundary must answer
    /// `ControlFlow::Break`, which is how `build_artifact` learns to abandon the run rather than
    /// read or write anything further.
    ///
    /// This replaces an earlier design in which the sink unwound out of the processor with a
    /// private panic payload, because `ProgressSink::report` returned `()`. The seam is
    /// crate-local and not part of the cross-language contract, so it now returns a
    /// `ControlFlow` and there is no panic-as-control-flow left to contain.
    #[test]
    fn cancellation_stops_the_processor_at_the_next_progress_boundary() {
        let (channel, reporter) = reporter();
        let mut observed = Vec::new();

        for step in 0..10u64 {
            observed.push(step);
            let flow = reporter.report(&ProgressEvent {
                processed_variants: step,
                ..ProgressEvent::phase(IngestionPhase::WritingDuckdb)
            });
            if flow.is_break() {
                break;
            }
            if step == 2 {
                channel.cancel();
            }
        }

        assert_eq!(
            observed,
            [0, 1, 2, 3],
            "work must stop at the first boundary after the cancellation, not before or later"
        );
        assert_eq!(
            channel.phases().len(),
            4,
            "the boundary that observes the cancellation still reports the progress it made"
        );
    }

    /// A run that is never cancelled must be completely unaffected: every boundary continues.
    #[test]
    fn an_uncancelled_run_is_never_asked_to_stop() {
        let (_channel, reporter) = reporter();

        for phase in [
            IngestionPhase::Parsing,
            IngestionPhase::WritingDuckdb,
            IngestionPhase::ExportingParquet,
        ] {
            assert_eq!(
                reporter.report(&ProgressEvent::phase(phase)),
                ControlFlow::Continue(())
            );
        }
    }

    // -----------------------------------------------------------------------------------
    // Step 3 — attempt scoping and the local→S3 mapping
    // -----------------------------------------------------------------------------------

    #[test]
    fn the_worker_identity_and_queue_are_the_contracted_ones() {
        assert_eq!(TASK_QUEUE, "genomic-ingestion-rust");
        assert_eq!(ACTIVITY_TYPE, "buildDatasetArtifact");
        assert_eq!(WORKER_IDENTITY_PREFIX, "rust-ingestion-worker@");
    }

    /// The attempt prefix is derived, never taken from the wire, and is always strictly below
    /// the immutable version prefix.
    #[test]
    fn derives_an_attempt_prefix_below_the_allowed_version_prefix() {
        let allowed = derived_allowed_prefix("ds-test-001", "iv-test-001");
        assert_eq!(allowed, "datasets/ds-test-001/versions/iv-test-001/");
        assert_eq!(
            attempt_prefix_for(&allowed, 1),
            "datasets/ds-test-001/versions/iv-test-001/attempt-1/"
        );
        assert_eq!(
            attempt_prefix_for(&allowed, 3),
            "datasets/ds-test-001/versions/iv-test-001/attempt-3/"
        );

        let prefix = validated_attempt_prefix(&input(), 1).expect("the golden input is valid");
        assert_eq!(prefix, "datasets/ds-test-001/versions/iv-test-001/attempt-1/");
        assert!(prefix.starts_with(&allowed) && prefix.len() > allowed.len());
    }

    /// The obligation this task inherited: the Rust adapter only checks that an object key sits
    /// below the attempt prefix it is handed. Nothing checked that the *attempt prefix itself*
    /// sits below `datasets/{datasetId}/versions/{artifactVersion}/`, which is what TypeScript
    /// enforces as `ATTEMPT_PREFIX_OUTSIDE_ALLOWED_PREFIX`. A widened `allowedPrefix` such as
    /// `datasets/` satisfies every containment check downstream.
    ///
    /// The check that gates is the equality against the *derived* prefix, which is what this
    /// test exercises. Containment itself is structural — the attempt prefix is built by
    /// extending the derived value, never by trusting the wire — so a runtime
    /// `starts_with` re-check on a string built as `format!("{allowed}attempt-{n}/")` could not
    /// fail and is not attempted; `derives_an_attempt_prefix_below_the_allowed_version_prefix`
    /// asserts the two functions compose that way.
    #[test]
    fn refuses_an_allowed_prefix_that_is_not_the_derived_one() {
        for widened in [
            "datasets/",
            "datasets/ds-test-001/",
            "datasets/ds-test-001/versions/",
            "datasets/other/versions/iv-test-001/",
            "datasets/ds-test-001/versions/iv-other/",
            "datasets/ds-test-001/versions/iv-test-001",
            "",
        ] {
            let mut widened_input = input();
            widened_input.target.allowed_prefix = widened.to_string();
            let failure = validated_attempt_prefix(&widened_input, 1)
                .expect_err("a widened allowed prefix must be refused");
            assert_eq!(failure.failure_type(), FailureType::ArtifactValidationFailed);
            assert!(!failure.failure_type().is_retryable());
        }
    }

    /// Identifiers reach the prefix verbatim, so anything that is not a single plain path
    /// segment has to be refused before it can climb out of the dataset's namespace.
    #[test]
    fn refuses_identifiers_that_are_not_single_safe_path_segments() {
        for bad in ["../evil", "a/b", "ds=1", "", ".", "..", "with space", "a\\b"] {
            let mut by_dataset = input();
            by_dataset.dataset_id = bad.to_string();
            by_dataset.target.allowed_prefix = derived_allowed_prefix(bad, "iv-test-001");
            assert!(
                validated_attempt_prefix(&by_dataset, 1).is_err(),
                "dataset id '{bad}' must be refused"
            );

            let mut by_version = input();
            by_version.target.artifact_version = bad.to_string();
            by_version.target.allowed_prefix = derived_allowed_prefix("ds-test-001", bad);
            assert!(
                validated_attempt_prefix(&by_version, 1).is_err(),
                "artifact version '{bad}' must be refused"
            );
        }
    }

    /// The `variants/` segment exists only in the S3 key. It is contributed here, by the
    /// mapping layer, precisely so `relativePath` — and therefore the dataset checksum — stays
    /// independent of any attempt's prefix.
    #[test]
    fn composes_object_keys_with_the_variants_segment() {
        let prefix = "datasets/ds-test-001/versions/iv-test-001/attempt-1/";
        assert_eq!(
            object_key_for(prefix, "chrom=12/part-000.parquet"),
            "datasets/ds-test-001/versions/iv-test-001/attempt-1/variants/chrom=12/part-000.parquet"
        );
        assert!(object_key_for(prefix, "chrom=1/part-000.parquet")
            .starts_with(&format!("{prefix}{VARIANTS_SEGMENT}")));
    }

    /// The published inventory is the local descriptor list plus the identity S3 gave back —
    /// and nothing else. In particular `byteSize` comes from the local file, because
    /// `UploadedObject` does not carry one and the TypeScript verifier compares it against
    /// `head.contentLength`.
    #[test]
    fn maps_local_descriptors_and_upload_receipts_onto_the_wire_inventory() {
        let prefix = "datasets/ds-test-001/versions/iv-test-001/attempt-1/";
        let files = [
            local_file("1", DIGEST_ONE, 20_480, 900),
            local_file("12", DIGEST_TWELVE, 15_360, 600),
        ];
        let receipts = [
            uploaded(
                "genomic-artifacts",
                &object_key_for(prefix, &files[0].relative_path),
                "etag-1",
                None,
            ),
            uploaded(
                "genomic-artifacts",
                &object_key_for(prefix, &files[1].relative_path),
                "etag-12",
                Some("v12"),
            ),
        ];

        let inventory = published_inventory(prefix, &files, &receipts).expect("a valid mapping");
        assert_eq!(
            inventory,
            vec![
                ParquetObject {
                    bucket: "genomic-artifacts".to_string(),
                    key: format!("{prefix}variants/chrom=1/part-000.parquet"),
                    etag: "etag-1".to_string(),
                    version_id: None,
                    chrom: "1".to_string(),
                    checksum_sha256: DIGEST_ONE.to_string(),
                    byte_size: 20_480,
                    row_count: 900,
                    min_pos: 1,
                    max_pos: 1_000,
                },
                ParquetObject {
                    bucket: "genomic-artifacts".to_string(),
                    key: format!("{prefix}variants/chrom=12/part-000.parquet"),
                    etag: "etag-12".to_string(),
                    version_id: Some("v12".to_string()),
                    chrom: "12".to_string(),
                    checksum_sha256: DIGEST_TWELVE.to_string(),
                    byte_size: 15_360,
                    row_count: 600,
                    min_pos: 1,
                    max_pos: 1_000,
                },
            ]
        );
    }

    /// A receipt that does not name the key the mapping composed means a descriptor and an
    /// upload were paired up wrongly — the one way an attempt could publish another attempt's
    /// object. It is a deterministic refusal.
    #[test]
    fn refuses_an_upload_receipt_that_does_not_match_the_composed_key() {
        let prefix = "datasets/ds-test-001/versions/iv-test-001/attempt-1/";
        let files = [local_file("1", DIGEST_ONE, 20_480, 900)];

        for wrong_key in [
            "datasets/ds-test-001/versions/iv-test-001/attempt-2/variants/chrom=1/part-000.parquet",
            "datasets/ds-test-001/versions/iv-test-001/attempt-1/chrom=1/part-000.parquet",
            "datasets/ds-test-001/versions/iv-test-001/attempt-1/variants/chrom=12/part-000.parquet",
        ] {
            let receipts = [uploaded("genomic-artifacts", wrong_key, "etag", None)];
            let failure = published_inventory(prefix, &files, &receipts)
                .expect_err("a mismatched receipt must be refused");
            assert_eq!(failure.failure_type(), FailureType::ArtifactValidationFailed);
        }

        let failure = published_inventory(prefix, &files, &[])
            .expect_err("a missing receipt must be refused");
        assert_eq!(failure.failure_type(), FailureType::ArtifactValidationFailed);
    }

    /// The checksum is re-derived from the *published* keys, by stripping
    /// `{attemptPrefix}variants/` exactly as `dataset-checksum.ts` does. That is what proves the
    /// mapping layer did not disturb the content identity the processor computed locally.
    #[test]
    fn re_derives_the_dataset_checksum_from_the_published_inventory() {
        let files = [
            local_file("1", DIGEST_ONE, 20_480, 900),
            local_file("12", DIGEST_TWELVE, 15_360, 600),
        ];
        let expected = dataset_checksum_sha256(&files);

        // The same content published under two different attempt prefixes must hash the same.
        for attempt in [1u32, 7] {
            let prefix = attempt_prefix_for(
                &derived_allowed_prefix("ds-test-001", "iv-test-001"),
                attempt,
            );
            let receipts: Vec<UploadedObject> = files
                .iter()
                .map(|file| {
                    uploaded(
                        "genomic-artifacts",
                        &object_key_for(&prefix, &file.relative_path),
                        "etag",
                        None,
                    )
                })
                .collect();
            let inventory = published_inventory(&prefix, &files, &receipts).expect("mapping");
            assert_inventory_checksum(&prefix, &inventory, &expected)
                .expect("the published inventory must reproduce the processor's checksum");
        }
    }

    #[test]
    fn refuses_an_inventory_that_does_not_reproduce_the_declared_checksum() {
        let prefix = "datasets/ds-test-001/versions/iv-test-001/attempt-1/";
        let files = [local_file("1", DIGEST_ONE, 20_480, 900)];
        let receipts = [uploaded(
            "genomic-artifacts",
            &object_key_for(prefix, &files[0].relative_path),
            "etag",
            None,
        )];
        let mut inventory = published_inventory(prefix, &files, &receipts).expect("mapping");
        inventory[0].row_count += 1;

        let failure = assert_inventory_checksum(prefix, &inventory, &dataset_checksum_sha256(&files))
            .expect_err("a disturbed inventory must not be published");
        assert_eq!(failure.failure_type(), FailureType::ArtifactValidationFailed);
    }

    // -----------------------------------------------------------------------------------
    // Attempt-local workspace
    // -----------------------------------------------------------------------------------

    /// Two attempts of the same activity never share a staging database or an export
    /// directory, and neither can be steered out of the staging root by a Workflow ID.
    #[test]
    fn attempt_workspaces_are_unique_and_cannot_escape_the_staging_root() {
        let first = attempt_workspace_name("wf-1", "activity-1", 1);
        let second = attempt_workspace_name("wf-1", "activity-1", 2);
        assert_ne!(first, second);
        assert_ne!(
            attempt_workspace_name("wf-1", "activity-1", 1),
            attempt_workspace_name("wf-2", "activity-1", 1)
        );

        for hostile in ["../../etc", "a/b", ".", "..", "", "a\0b"] {
            let name = attempt_workspace_name(hostile, hostile, 1);
            assert!(!name.contains('/'), "'{name}' must stay a single segment");
            assert!(!name.contains('\\'), "'{name}' must stay a single segment");
            assert!(!name.contains(".."), "'{name}' must not climb out");
            assert!(!name.contains('\0'), "'{name}' must be a usable file name");
        }
    }

    /// Sanitizing an identifier is lossy twice over — every unsafe character folds onto `_`, and
    /// the fragment is truncated — so two different Workflow IDs could name one workspace. The
    /// loser of that race fails `AttemptWorkspace::create`, which is *safe* (an attempt never
    /// reuses a path) but costs a retry nobody needed. Distinct identifiers must produce
    /// distinct names.
    #[test]
    fn workspace_names_do_not_collide_when_identifiers_sanitize_alike() {
        let colliding = [
            "ingest-ds:1",
            "ingest-ds/1",
            "ingest-ds 1",
            "ingest-ds.1",
            "ingest-ds_1",
        ];
        let names: Vec<String> = colliding
            .iter()
            .map(|workflow_id| attempt_workspace_name(workflow_id, "2", 1))
            .collect();
        let distinct: std::collections::BTreeSet<&String> = names.iter().collect();
        assert_eq!(
            distinct.len(),
            colliding.len(),
            "identifiers that sanitize alike must still get their own workspace: {names:?}"
        );

        // Truncation is lossy in the same way, and must be disambiguated too.
        let long = "w".repeat(200);
        assert_ne!(
            attempt_workspace_name(&long, "2", 1),
            attempt_workspace_name(&format!("{long}-other"), "2", 1)
        );

        // The activity id half is sanitized the same way.
        assert_ne!(
            attempt_workspace_name("wf-1", "a:1", 1),
            attempt_workspace_name("wf-1", "a/1", 1)
        );

        // Still a single, usable path segment, and still attempt-scoped.
        for name in &names {
            assert!(!name.contains('/') && !name.contains(".."), "{name}");
            assert!(name.ends_with("-attempt-1"), "{name}");
        }
    }

    /// The workspace is created, used, and removed again — on every exit path. It refuses a
    /// directory it did not create itself, which is both the "an attempt never reuses a path"
    /// rule and the guarantee that its `Drop` can only ever delete its own tree.
    #[test]
    fn the_attempt_workspace_creates_and_removes_only_its_own_directory() {
        let root = TempDir::new().expect("temp dir");
        let sibling = root.path().join("someone-elses-attempt");
        std::fs::create_dir(&sibling).expect("seed a neighbour");

        let path = {
            let workspace = AttemptWorkspace::create(root.path(), "wf-1-act-1-attempt-1")
                .expect("a fresh workspace");
            let path = workspace.root().to_path_buf();
            assert!(path.starts_with(root.path()));
            assert!(path.is_dir());

            std::fs::write(workspace.source_path(), b"##fileformat=VCFv4.2\n").expect("write");
            assert!(workspace.source_path().exists());
            assert_eq!(workspace.parquet_dir().parent(), Some(path.as_path()));
            assert_eq!(workspace.staging_db_path().parent(), Some(path.as_path()));

            AttemptWorkspace::create(root.path(), "wf-1-act-1-attempt-1")
                .expect_err("a directory that already exists is never reused");
            path
        };

        assert!(!path.exists(), "the workspace must be removed when it is dropped");
        assert!(sibling.exists(), "nothing outside the workspace may be touched");
        assert!(root.path().exists());
    }
}
