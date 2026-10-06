# Operations

Terminate TLS at a trusted ingress and bind the service on a private interface. Forward the Origin
header unchanged, permit WebSocket upgrades, and configure proxy idle/drain timeouts for long-lived
connections. Set exact browser origins and application subprotocols.

Choose connection, message, and frame bounds from measured memory and traffic needs. Use the
oneshot shutdown signal during process termination; the server rejects new accepts and sends a
going-away close frame to active sessions before the drain deadline. Cross-instance broadcast and
presence require a separate shared backend.
