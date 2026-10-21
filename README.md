# microservice-artifacts

The artifacts service: the write-through store of run trees and the one writer
of both object stores.

This repository is one component of
[The Test Cabinet](https://github.com/TheClockwyrks/TheTestCabinet). What it is
and how it fits with the others is stated by the documentation site, which
lives in the superrepo at `apps/docs/src/content/docs/`. Its Repositories page,
`apps/docs/src/content/docs/development/repositories.md`, draws the edges
between the repositories.

## The service

`crates/artifacts` is `tcab-artifacts`, the write-through store of run trees.
The driver uploads each run's produced tree to it when the run finishes, the
console reads that tree through it to play and review a run before it is
published, and the backend writes the public dataset through it when a run
publishes. It is the one writer of two object stores, the private Azure Blob
container of run trees and the public Cloudflare R2 bucket, and no other
component holds a credential for either. It holds no state of its own, so a
replica is interchangeable with any other, and it binds `0.0.0.0:8790` by
default.

What the service must do is stated by the superrepo's
`apps/docs/src/content/docs/components/artifacts/overview.md`. Its
configuration is entirely environment variables, documented in
`crates/artifacts/src/config.rs` and listed with placeholder values in
`.env.artifacts.example`. To run it on its own, against the Azurite emulator or
the stores your environment names, copy that file to `.env.artifacts`, fill it
in and run:

```sh
cargo run --bin tcab-artifacts
```

The service reads `.env.artifacts` from the working directory itself, and a
variable already exported takes precedence over the file.

The manifests that deploy it (its `Deployment` and `Service`, its
`ServiceAccount` federated to the environment's managed identity, the secrets
naming its stores) and the deploy sets pinning its image live in the
superrepo, under `deployments/k8s/`. A deployment pins this repository's image
at the commit the superrepo's submodule pointer names.

## Getting started

Open the superrepo in its devcontainer, which carries every toolchain the
repositories need, and work from this repository's root.
A build here inherits the superrepo's Cargo configuration, which redirects
every dependency on another repository to the sibling checkout, so nothing is
fetched from a mirror.

One command runs every gate:

```sh
make gate
```

Each gate is a file under `ci/gates/` whose stem is the gate's id.
`uv run --quiet --project ci gate list` names them all, and
`azure-pipelines.yml` runs each one as a step of its own on every push, inside
the CI images the superrepo builds.

## The image

The `Dockerfile` at the root is the repository's image, built from the root
with `.dockerignore` as its allowlist. On every gated push to `master` and
`staging` the pipeline builds it natively for amd64 and arm64
(`scripts/ci/image.sh`) and fuses the two into the multi-arch
`testcabinet.azurecr.io/microservice-artifacts:<commit>` a deployment pins
(`scripts/ci/manifest.sh`).
The `build-context` gate holds every `COPY` to the allowlist and the Rust pin
to `rust-toolchain.toml`. The kit wrote the `Dockerfile` once, and it is the
repository's to grow.

`cargo build-portable-artifacts` builds a fully static `tcab-artifacts`
through the musl target, for a host without the generic loader;
`.cargo/config.toml` lists what it needs.
