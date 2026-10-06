# Architecture

This crate implements a standalone Tokio WebSocket listener using `tokio-tungstenite`. It accepts a
provided listener, validates the configured request path and exact `Origin`, selects only configured
subprotocols, and applies bounds before dispatch. Connection admission is controlled by a semaphore;
messages and frames have independent limits; compression is disabled.

The service runs behind ingress and does not use Core's process-plugin gRPC protocol. Core's gRPC
host is for trusted local plugins, not general app microservice communication. The Rust process does
the complete socket serving work itself.
