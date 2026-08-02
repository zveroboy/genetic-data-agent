# The TypeScript control plane, as a runtime image.
#
# One image, two roles, selected by the container's command:
#
#   node src/index.ts               → the HTTP API (`/api/…`, `/ask`)
#   node src/application/worker.ts  → the Temporal Worker on `genomic-control-plane`
#
# They share every adapter and every contract module, so a change to one cannot silently give
# the other a different idea of what a manifest is. What they do not share is a task queue: the
# Worker serves `genomic-control-plane` only, and `buildDatasetArtifact` is not registered in
# TypeScript at all.
#
# Build from the repository root (workspace manifests and the committed reference TSV are in
# context):
#
#   docker build -f ts-api-agent/Dockerfile -t genomic-ts-api .
#
# Two things are baked in at build time so that a cold container with no Internet can still
# answer a question:
#
#   1. the DuckDB `httpfs` extension, matched to the engine the pinned binding embeds;
#   2. the versioned ClinVar coordinate snapshot, built from the TSV tracked in this repository.
#
# At run time the serving session forces `autoinstall_known_extensions` and
# `autoload_known_extensions` off, so neither is ever fetched on a user's first request.
#
# Requires BuildKit: the `ARG TARGETARCH` used below to pick the `@swc/core` platform binding is
# only populated by BuildKit's default builder. Build with `DOCKER_BUILDKIT=1` or `docker buildx`,
# not the legacy builder (`DOCKER_BUILDKIT=0`), which leaves `TARGETARCH` empty and fails the
# `unsupported TARGETARCH ''` guard below.

FROM node:25-bookworm-slim AS build

WORKDIR /app

# Manifests first: dependency installation is cached until the lockfile actually changes.
COPY package.json package-lock.json ./
COPY ts-api-agent/package.json ./ts-api-agent/

# `npm ci` installs exactly the lockfile — a floating `@duckdb/node-api` would pair the runtime
# with an engine the preinstalled `httpfs` below cannot load. `--omit=dev` keeps TypeScript and
# the Temporal test harness out of the runtime image; Node runs the `.ts` sources directly.
RUN npm ci --omit=dev

# `@temporalio/worker` bundles Workflow code at startup with webpack + `swc-loader`, and
# `@swc/core` reaches its compiler through a per-platform native package installed as an
# *optional* dependency. This repository's `package-lock.json` was generated on macOS and — as
# npm does since v9 — records only the optional packages matching the machine that produced it
# (`@swc/core-darwin-arm64`). `npm ci` cannot install an entry the lockfile does not contain, so
# a Linux image built from it starts, bundles, and dies with "Failed to load native binding" the
# moment the Worker registers a Workflow. The API half of the image never notices, which is
# exactly how this would reach production unseen.
#
# So the matching binding is installed explicitly, at the version the locked `@swc/core` itself
# asks for — read out of the installed tree rather than written here, so it cannot drift from the
# lockfile — and for the architecture actually being built. `--no-save` keeps the lockfile
# authoritative for everything else.
ARG TARGETARCH
RUN set -eu; \
    version="$(node -p "require('/app/node_modules/@swc/core/package.json').version")"; \
    case "${TARGETARCH}" in \
      arm64) binding="@swc/core-linux-arm64-gnu" ;; \
      amd64) binding="@swc/core-linux-x64-gnu" ;; \
      *) echo "unsupported TARGETARCH '${TARGETARCH}'" >&2; exit 1 ;; \
    esac; \
    npm install --no-save --no-audit --no-fund "${binding}@${version}"; \
    node -e "require('@swc/core'); console.log('[swc] native binding loads for ${TARGETARCH}')"

COPY ts-api-agent ./ts-api-agent
COPY tests/fixtures ./tests/fixtures

# The only network access the runtime image's contents ever require, taken here rather than on a
# user's first `/ask`. `$HOME` is fixed so the extension lands where the runtime user will look.
ENV HOME=/home/node
RUN node ts-api-agent/scripts/install_duckdb_extensions.ts

# Derived from the committed TSV; no network, no user data.
RUN node ts-api-agent/scripts/build_reference_snapshot.ts

FROM node:25-bookworm-slim AS runtime

# `ca-certificates` for TLS to S3 and Temporal. Nothing else: no compiler, no package index, and
# in particular no Cargo — this image never builds or launches the Rust worker.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Scopes `docker image prune -f --filter label=...` (used by the deploy workflow) to images this
# project built, so a prune after deploy never touches another project's dangling image.
LABEL com.genomic-ingestion.project="genomic-ingestion"

WORKDIR /app/ts-api-agent
ENV NODE_ENV=production \
    HOME=/home/node \
    PORT=3000

# Everything is owned by the unprivileged `node` user the image already provides, including the
# extension cache under `$HOME/.duckdb`. The application writes nothing at run time: user
# variants stay in S3 and are read through ranged HTTP requests.
COPY --from=build --chown=node:node /app/node_modules /app/node_modules
COPY --from=build --chown=node:node /app/package.json /app/package-lock.json /app/
COPY --from=build --chown=node:node /app/ts-api-agent /app/ts-api-agent
COPY --from=build --chown=node:node /app/tests/fixtures /app/tests/fixtures
COPY --from=build --chown=node:node /app/data/reference /app/data/reference
COPY --from=build --chown=node:node /home/node/.duckdb /home/node/.duckdb

USER node

EXPOSE 3000

# `fetch` rather than curl/wget: neither is installed, and adding one to run a health check would
# also add it to whatever else the container could be made to do.
HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=6 \
  CMD ["node", "-e", "fetch(`http://127.0.0.1:${process.env.PORT||3000}/health`).then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"]

CMD ["node", "src/index.ts"]
