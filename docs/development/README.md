# Development

Requires stable Rust and Cargo. Run `cargo fmt --check`, `cargo test --locked --all-targets`,
`cargo clippy --locked --all-targets --all-features -- -D warnings`, and
`cargo build --release --locked`.

Tests should cover handshake rejection, message/frame limits, cancellation, close and drain
behavior, overload admission, concurrent send/receive, and payload-safe diagnostics. Release
validation additionally needs Autobahn conformance, ingress tests, and realistic connection load and
soak measurements.
