# Orbit Sockets Rust

`orbit-sockets-rust` is the standalone Rust WebSocket service option for Orbit. It uses Tokio and
[`tokio-tungstenite`](https://docs.rs/tokio-tungstenite/) to perform the WebSocket handshake and
message processing. It is a complete listener implementation, not a Core process plugin or a Rust
SDK that forwards socket work to Python.

## Use

The application binds a private TCP listener, supplies a bounded `ServerConfig`, and passes a
handler to `serve`. Terminate TLS at a trusted ingress, configure exact allowed browser origins, and
signal shutdown through a Tokio oneshot channel. `serve` caps connections before upgrade, enforces
route and origin checks, message and frame sizes, disables compression, applies async backpressure,
and drains tasks within the configured close timeout.

## Operational limits

WebSocket connections are process-local. Shared fanout, presence, durable replay, user identity,
authorization, and cross-instance coordination belong to the application and optional Orbit
capability packages. The server does not claim application-level message delivery or acknowledgments.
Payload data is kept private from `Debug` output; errors use sanitized categories.

## Documentation and checks

See the [architecture](docs/architecture/overview.md), [operations](docs/operations/README.md),
[security](docs/security/overview.md), and [development](docs/development/README.md) guides.

```sh
cargo fmt --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo build --release --locked
```

This package is pre-alpha. Hosted CI, Autobahn conformance, ingress interoperability, representative
load and soak testing, and release provenance remain release gates.

Licensed under Apache-2.0.
