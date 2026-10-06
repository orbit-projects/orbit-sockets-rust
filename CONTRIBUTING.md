# Contributing to Orbit Realtime

Contributions are welcome. Read the [package overview](README.md), [architecture guide](docs/architecture/overview.md), and [operations guide](docs/operations/README.md) before changing public behavior. Keep changes within this package's documented contract and preserve the Orbit Core and capability boundaries.

## Development

Use the package-specific [development guide](docs/development/README.md). The following commands are the baseline local checks for this repository:

```bash
python -m pip install -e '.[dev]'
pytest
ruff check src tests
ruff format --check src tests
mypy
```

## Change requirements

Add focused regression coverage for behavior changes. Update user-facing docs when a public contract, configuration option, security property, lifecycle rule, or operational assumption changes. State which Python/runtime and live provider versions were exercised; deterministic fakes do not substitute for live-provider evidence.

Keep credentials, personal data, request bodies, raw provider errors, local environments, caches, and build artifacts out of source control. Preserve Apache-2.0 file headers where required and use the repository's configured formatter and type checker.

## Pull requests

Explain the behavior change, owning package boundary, compatibility impact, validation commands, and remaining deployment assumptions. Keep changes focused and do not claim hosted CI, publication, provenance, or production readiness without recorded evidence. Contributions are licensed under Apache-2.0.
# Contributing to Orbit Sockets Rust

Keep changes scoped to this Rust implementation. Read the package [architecture](docs/architecture/overview.md),
[security](docs/security/overview.md), and `SECURITY.md` before changing transport or TLS behavior.
Use deterministic loopback tests and avoid logging payload data or certificates/private keys.

Before proposing a change, run:

```sh
cargo fmt --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo build --release --locked
```

Update docs and the changelog with user-visible changes. Do not commit, publish, or change repository
settings as part of a local implementation task.
