//! Deterministic coverage for two `download_exact`/`upload_file` branches that a real MinIO
//! cannot exercise without a race: what happens when the *second* (post-download) `HEAD` fails
//! outright, and what happens when it succeeds but reports a changed object. Also covers the
//! `upload_file` 403/409/412 -> `ArtifactWriteFailed` mapping.
//!
//! Rather than replacing an object mid-transfer against a real MinIO (inherently racy — the
//! window between the `GET` finishing and the second `HEAD` firing is a handful of
//! microseconds), this drives `S3ObjectStore` against a hand-rolled, single-purpose HTTP/1.1
//! stub server. It understands just enough of the protocol to answer the AWS SDK's `HEAD`/
//! `GET`/`PUT` requests in a scripted order: it does not validate signatures, `If-Match`, or
//! request paths, because the identity/precondition behaviour of a *real* store is already
//! covered by `minio_object_store_test.rs` (in particular `the_store_enforces_if_match_on_a_get`
//! and the ETag/version/size mismatch tests). What only a scriptable stub can prove
//! deterministically is what `download_exact` itself does with the *result* of that second
//! `HEAD` — succeed-but-changed, or fail outright.
//!
//! No new dependency: `tokio`'s existing `net`/`io-util` features (already pulled in via the
//! `full` feature in `Cargo.toml`) are enough to speak raw HTTP/1.1.
//!
//! Hermetic and fast — not `#[ignore]`d, so it runs as part of the default `cargo test`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rust_ingestion_worker::contracts::{FailureType, SourceObject};
use rust_ingestion_worker::object_store::{ObjectStoreConfig, S3ObjectStore, UploadRequest};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ---------------------------------------------------------------------------------------
// A minimal, scriptable HTTP/1.1 stub server
// ---------------------------------------------------------------------------------------

/// One canned response, consumed in order as requests arrive. When the script runs out, the
/// last step repeats for any further request — which is exactly what a retried request (the AWS
/// SDK's own retry policy) needs: a transient-failure script keeps failing every retry, and a
/// terminal-success script keeps answering the same way.
#[derive(Clone)]
enum ScriptStep {
    /// A `200` with the given (already-quoted-on-the-wire) ETag and declared content length, no
    /// body — valid for both `HEAD` and `GET` responses to a request with no body of its own.
    Head { etag: String, content_length: u64 },
    /// A `200` with a body — used for the `GET`.
    Get { etag: String, body: Vec<u8> },
    /// An S3-shaped error response with the given status and a generic `<Error>` XML body, so
    /// the SDK parses it as a service error rather than failing to parse the response at all.
    ErrorStatus { status: u16 },
    /// The connection is closed the moment the request has been fully read, without writing
    /// anything — simulating a store that drops the connection mid-request.
    DropConnection,
}

/// Starts the stub on an OS-assigned loopback port and returns it. The server task is spawned
/// on the current (per-test) Tokio runtime and is torn down when that runtime is dropped at the
/// end of the test; nothing needs to await its shutdown explicitly.
async fn start_stub(steps: Vec<ScriptStep>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the stub listener on an OS-assigned port");
    let port = listener.local_addr().expect("stub listener has a local address").port();
    let steps = Arc::new(steps);
    let counter = Arc::new(AtomicUsize::new(0));

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            let steps = Arc::clone(&steps);
            let counter = Arc::clone(&counter);
            tokio::spawn(async move {
                handle_one_request(stream, &steps, &counter).await;
            });
        }
    });

    port
}

async fn handle_one_request(mut stream: TcpStream, steps: &[ScriptStep], counter: &AtomicUsize) {
    // Read (and fully drain) exactly one request before deciding how to answer, so a dropped
    // connection is never mistaken by the client for a request that was never sent.
    if read_request(&mut stream).await.is_none() {
        return;
    }

    let index = counter.fetch_add(1, Ordering::SeqCst);
    let step = match steps.get(index) {
        Some(step) => step.clone(),
        None => steps.last().expect("a script has at least one step").clone(),
    };

    match step {
        ScriptStep::Head { etag, content_length } => {
            write_response(&mut stream, 200, "OK", &[("ETag", quote(&etag))], content_length, &[]).await;
        }
        ScriptStep::Get { etag, body } => {
            write_response(&mut stream, 200, "OK", &[("ETag", quote(&etag))], body.len() as u64, &body).await;
        }
        ScriptStep::ErrorStatus { status } => {
            let body = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>Stub</Code>\
                <Message>stub-injected failure</Message><RequestId>stub</RequestId></Error>"
                .to_vec();
            write_response(&mut stream, status, reason_phrase(status), &[], body.len() as u64, &body).await;
        }
        ScriptStep::DropConnection => {
            // Deliberately write nothing: dropping `stream` here closes the socket, which the
            // client sees as a connection failure while awaiting the response.
        }
    }
}

fn quote(etag: &str) -> String {
    format!("\"{etag}\"")
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        403 => "Forbidden",
        409 => "Conflict",
        412 => "Precondition Failed",
        _ => "Stub Error",
    }
}

/// Reads request headers up to the blank line, answers a `100-continue` expectation (the SDK
/// may send one ahead of a `PutObject` body), then drains exactly `Content-Length` further body
/// bytes so the connection is left in a clean state for the response. Returns `None` if the
/// peer closed before sending a complete request.
async fn read_request(stream: &mut TcpStream) -> Option<()> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return if buf.is_empty() { None } else { Some(()) };
        }
        buf.extend_from_slice(&chunk[..read]);
        if let Some(position) = find(&buf, b"\r\n\r\n") {
            break position;
        }
    };

    let header_text = String::from_utf8_lossy(&buf[..header_end]).to_string();

    if header_value(&header_text, "Expect").as_deref() == Some("100-continue") {
        stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await.ok()?;
    }

    let content_length: usize = header_value(&header_text, "Content-Length")
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let already_read = buf.len() - (header_end + 4);
    if already_read < content_length {
        let mut remainder = vec![0u8; content_length - already_read];
        stream.read_exact(&mut remainder).await.ok()?;
    }

    Some(())
}

fn header_value(header_text: &str, name: &str) -> Option<String> {
    header_text.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim().eq_ignore_ascii_case(name).then(|| value.trim().to_string())
    })
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    headers: &[(&str, String)],
    content_length: u64,
    body: &[u8],
) {
    let mut head = format!("HTTP/1.1 {status} {reason}\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str(&format!("Content-Length: {content_length}\r\n"));
    head.push_str("Connection: close\r\n\r\n");
    let _ = stream.write_all(head.as_bytes()).await;
    if !body.is_empty() {
        let _ = stream.write_all(body).await;
    }
    let _ = stream.flush().await;
    let _ = stream.shutdown().await;
}

fn stub_config(port: u16) -> ObjectStoreConfig {
    ObjectStoreConfig {
        endpoint: format!("http://127.0.0.1:{port}"),
        region: "us-east-1".to_string(),
        access_key_id: "stub-access-key".to_string(),
        secret_access_key: "stub-secret-key".to_string(),
        force_path_style: true,
    }
}

fn expect_failure(error: &rust_ingestion_worker::object_store::ObjectStoreError, expected: FailureType) {
    assert_eq!(error.failure_type(), expected, "'{error}' must map onto {expected}");
    assert_eq!(
        error.failure_type().is_retryable(),
        expected.is_retryable(),
        "retryability comes from the frozen contract"
    );
}

// ---------------------------------------------------------------------------------------
// Finding 1: the post-download HEAD's error path must clean up
// ---------------------------------------------------------------------------------------

/// A transport failure on the post-download `HEAD` (MinIO drops the connection) must be
/// classified as a retryable `ObjectStoreUnavailable`, exactly like the pre-download `HEAD` and
/// the `GET` failures already are. Before the fix, that branch propagated the error with a bare
/// `?` and skipped `remove_partial`, leaving the fully-written file at `destination` behind for
/// a deterministically-keyed retry to trip over as `ArtifactWriteFailed`.
#[tokio::test]
async fn a_transport_failure_on_the_post_download_head_does_not_leave_a_partial_file() {
    let etag = "match-etag-1".to_string();
    let body = b"##fileformat=VCFv4.2\n#CHROM\tPOS\tID\tREF\tALT\n1\t100\t.\tA\tG\n".to_vec();
    let source = SourceObject {
        bucket: "stub-bucket".to_string(),
        key: "samples/demo_user.vcf".to_string(),
        etag: etag.clone(),
        version_id: None,
        content_length: body.len() as u64,
    };

    let port = start_stub(vec![
        ScriptStep::Head { etag: etag.clone(), content_length: source.content_length },
        ScriptStep::Get { etag: etag.clone(), body: body.clone() },
        ScriptStep::DropConnection,
    ])
    .await;

    let store = S3ObjectStore::new(&stub_config(port)).await;
    let scratch = TempDir::new().expect("temp dir");
    let destination = scratch.path().join("source.vcf");

    let error = store
        .download_exact(&source, &destination)
        .await
        .expect_err("a dropped connection on the post-download HEAD must be reported as a failure");

    expect_failure(&error, FailureType::ObjectStoreUnavailable);
    assert!(
        !destination.exists(),
        "a post-download HEAD failure must remove the fully-written file, not leave it for a \
         deterministically-keyed retry to trip over as ArtifactWriteFailed"
    );
}

// ---------------------------------------------------------------------------------------
// Finding 2: the post-download rejection branch
// ---------------------------------------------------------------------------------------

/// The post-download `HEAD` succeeds, but reports a different ETag than the one observed before
/// the transfer and declared by the caller: the object changed while the bytes were in flight.
/// This is the only place `SourceObjectChanged` can be produced from the *second* `HEAD`, and it
/// was previously the only non-retryable producer of that error with no test coverage at all.
#[tokio::test]
async fn a_source_that_changed_between_the_two_heads_is_reported_as_source_object_changed() {
    let original_etag = "match-etag-2".to_string();
    let body = b"the allowlisted bytes\n".to_vec();
    let source = SourceObject {
        bucket: "stub-bucket".to_string(),
        key: "samples/demo_user.vcf".to_string(),
        etag: original_etag.clone(),
        version_id: None,
        content_length: body.len() as u64,
    };

    let port = start_stub(vec![
        ScriptStep::Head { etag: original_etag.clone(), content_length: source.content_length },
        ScriptStep::Get { etag: original_etag.clone(), body: body.clone() },
        // The second HEAD reports a different ETag: the object was replaced mid-transfer.
        ScriptStep::Head { etag: "somebody-elses-etag".to_string(), content_length: source.content_length },
    ])
    .await;

    let store = S3ObjectStore::new(&stub_config(port)).await;
    let scratch = TempDir::new().expect("temp dir");
    let destination = scratch.path().join("source.vcf");

    let error = store
        .download_exact(&source, &destination)
        .await
        .expect_err("a source changed between the two HEADs must be refused");

    expect_failure(&error, FailureType::SourceObjectChanged);
    assert!(
        !destination.exists(),
        "a source that changed after the transfer must not leave a downloaded file behind either"
    );
}

// ---------------------------------------------------------------------------------------
// upload_file: 403/409/412 -> ArtifactWriteFailed
// ---------------------------------------------------------------------------------------

/// A permission or precondition refusal on the `PutObject` itself — the bucket policy that makes
/// a published prefix immutable — must map onto the retryable `ArtifactWriteFailed`, not the
/// non-retryable `SourceObjectChanged` or `ArtifactValidationFailed`: a different attempt prefix
/// can get past it.
#[tokio::test]
async fn upload_maps_forbidden_conflict_and_precondition_responses_to_a_retryable_write_failure() {
    for status in [403u16, 409, 412] {
        let port = start_stub(vec![ScriptStep::ErrorStatus { status }]).await;
        let store = S3ObjectStore::new(&stub_config(port)).await;

        let scratch = TempDir::new().expect("temp dir");
        let bytes = b"parquet-ish".to_vec();
        let local_path = scratch.path().join("part-000.parquet");
        std::fs::write(&local_path, &bytes).expect("write local fixture");

        let Err(error) = store
            .upload_file(&UploadRequest {
                bucket: "stub-bucket",
                attempt_prefix: "datasets/ds-1/versions/iv-1/attempt-1/",
                key: "datasets/ds-1/versions/iv-1/attempt-1/variants/chrom=1/part-000.parquet",
                local_path: &local_path,
                checksum_sha256: "89e4e0a61728e9776376f7550d09426acba14bd486c68a918e66fb11d437d7de",
            })
            .await
        else {
            panic!("a stubbed {status} response must fail the upload");
        };

        expect_failure(&error, FailureType::ArtifactWriteFailed);
    }
}
