# 0001: Keep WebSocket listeners independent of the Core process host

- Status: accepted
- Date: 2026-10-06

## Context

Core's gRPC process host supervises local plugins. It is not the application's distributed RPC
system and does not define a browser-connection stream contract.

## Decision

This package is a standalone Rust WebSocket service deployed behind a WebSocket-capable ingress.
Applications select it explicitly. It applies bounded session and message limits, origin/path
validation, transport backpressure, cancellation-aware tasks, and deadline-bound shutdown.

## Consequences

Deployments own addressing, ingress, TLS termination, and horizontal routing. Shared conformance
tests are required before declaring Rust and Python implementations interchangeable. Core's local
process-plugin transport remains separate.
