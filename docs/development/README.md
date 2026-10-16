# Development

Requires Rust 1.85 or later and Cargo. CI runs formatting, tests, lint, and release builds on Rust
1.85 and stable. Run `cargo fmt --check`, `cargo test --locked --all-targets`,
`cargo clippy --locked --all-targets --all-features -- -D warnings`, and
`cargo build --release --locked`.

Tests should cover handshake rejection, message/frame limits, cancellation, close and drain
behavior, overload admission, concurrent send/receive, and payload-safe diagnostics. Release
validation additionally needs Autobahn conformance, ingress tests, and realistic connection load and
soak measurements.

On 2026-10-10, formatting, all **3 integration tests**, Clippy with warnings denied, and locked
release builds passed on Rust 1.85.0 and Rust 1.99.0. Hosted CI, Autobahn conformance, ingress
interoperability, and representative load remain release checks.
