# Roadmap

## Implemented

- Tokio standalone WebSocket server using `tokio-tungstenite`.
- Bounded connection admission, exact origin and path validation, subprotocol negotiation, frame and
  message limits, compression disabled, concurrent backpressured I/O, and shutdown drain.

## Release gates

- Hosted Rust CI and dependency/security review.
- Autobahn WebSocket conformance and ingress interoperability.
- Multi-instance connection load, memory, tail-latency, and drain measurements.
- Signed artifacts and provenance.
