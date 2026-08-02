# The genomic ingestion data plane, as a runtime image.
#
# Two stages, and the split is the point: the builder has a Rust toolchain, a C++ compiler and
# `protoc`, the runtime has none of them. What ships is one binary plus the CA bundle its TLS
# client needs — no Cargo, no source, no package index left behind to fetch something at run time.
#
# Build from the repository root, so the workspace `Cargo.toml`/`Cargo.lock` are in context:
#
#   docker build -f rust-ingestion-worker/Dockerfile -t genomic-rust-worker .
#
# The image runs `temporal_worker`, which polls the activity-only `genomic-ingestion-rust` task
# queue. It is never invoked by the TypeScript control plane as a subprocess: the only thing that
# crosses the language boundary is a Temporal Activity payload naming S3 objects.

# The toolchain is pinned rather than `rust:latest`: the Temporal Rust SDK is a `=0.5.0` Public
# Preview pin, and a floating compiler is exactly how a reproducible build stops being one.
FROM rust:1.97-bookworm AS builder

# `protoc` is a build-time requirement of the Temporal SDK's `prost-wkt-types` dependency, which
# compiles the Temporal gRPC protos from source. `libprotobuf-dev` comes with it because those
# protos import the well-known types (`google/protobuf/timestamp.proto`), which ship as headers
# rather than inside the compiler. Both exist only in this stage.
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Job count, measured rather than guessed, on a 16-CPU / 8.3 GB Docker Desktop VM (native
# aarch64, no emulation). `cargo`'s default is the CPU count, and it also drives `NUM_JOBS` for
# build scripts, so the `aws-lc-sys` and bundled-DuckDB C/C++ builds fan out with it:
#
#   16 (default) → `aws_sdk_s3` killed with SIGKILL
#    2           → "cannot allocate memory" during the same crate
#    4           → completes, and is what is set here
#
# The 2-job failure is why this is not simply "use half the cores": peak `rustc` RSS on a few
# large crates, not the job count, is what runs the VM out of memory, and the two runs differed
# in what else was competing for the VM at the time. 4 is the value that has completed
# repeatedly on this machine; if a builder OOMs, lower it and record what was observed.
ENV CARGO_BUILD_JOBS=4 \
    CARGO_NET_RETRY=5

COPY Cargo.toml Cargo.lock ./
COPY rust-ingestion-worker ./rust-ingestion-worker

# Two BuildKit cache mounts, and they replace what used to be a "compile the dependencies from a
# stub `main.rs` first" layer. That trick only survives while `Cargo.toml` and `Cargo.lock` are
# byte-identical; a single dependency bump throws away the bundled-DuckDB compile — twenty
# minutes — for no reason. A cache mount is keyed by path, not by layer, so DuckDB is compiled
# once per machine and survives both a manifest change and a `--no-cache` build of everything
# else. Measured on the VM described above:
#
#   stub layer, 1 job, cold                     ~25 min
#   cache mounts, 4 jobs, --no-cache            7 min  (419 s)
#   cache mounts, 4 jobs, source file changed   21 s
#   cache mounts, 4 jobs, Cargo.toml changed    21 s   ← the stub layer's worst case
#
#
# The binary is copied to `/out` **inside the same `RUN`**: a cache mount is not part of the
# resulting layer, so anything left under `/src/target` is invisible to the `COPY --from=builder`
# below. Getting that wrong fails at image-export time rather than at build time.
#
# `--locked` fails the build rather than silently updating `Cargo.lock`: the S3 SDK and Temporal
# SDK versions are frozen cross-language contract surface.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --release --locked --bin temporal_worker \
 && mkdir -p /out \
 && cp target/release/temporal_worker /out/temporal_worker \
 && strip /out/temporal_worker

FROM debian:bookworm-slim AS runtime

# `ca-certificates` only: the worker speaks TLS to S3 and gRPC to Temporal and needs a trust
# store to do it. Nothing else is installed, and the apt lists are removed, so the runtime image
# carries no package index to install from.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# An unprivileged user with a staging directory of its own. Ingestion stages a DuckDB database
# and a Parquet export per attempt under `INGESTION_STAGING_ROOT`; `docker-compose.prod.yml`
# mounts a size-capped tmpfs there, so a runaway ingest fills a bounded mount instead of a disk.
RUN useradd --system --create-home --uid 10001 ingestion \
 && mkdir -p /var/lib/ingestion \
 && chown ingestion:ingestion /var/lib/ingestion

COPY --from=builder /out/temporal_worker /usr/local/bin/temporal_worker

USER ingestion
WORKDIR /var/lib/ingestion

ENV INGESTION_STAGING_ROOT=/var/lib/ingestion \
    TEMPORAL_ADDRESS=temporal:7233 \
    TEMPORAL_NAMESPACE=default \
    RUST_LOG=info

# Exec form, no entrypoint script: the container *is* the worker process, so `SIGTERM` from
# `docker stop` reaches it directly and Temporal sees a clean shutdown rather than a dead worker.
ENTRYPOINT ["/usr/local/bin/temporal_worker"]
