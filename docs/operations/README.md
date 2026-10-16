# Operations

Terminate TLS at a trusted ingress and bind the service on a private interface. Forward the Origin
header unchanged, permit WebSocket upgrades, and configure proxy idle/drain timeouts for long-lived
connections. Set exact browser origins and application subprotocols.

Choose connection, message, and frame bounds from measured memory and traffic needs. Use the
oneshot shutdown signal during process termination; the server rejects new accepts and sends a
going-away close frame to active sessions before the drain deadline. Cross-instance broadcast and
presence require a separate shared backend.

`allowed_origins` contains exact scheme/authority origins and rejects paths, queries, userinfo,
duplicates, and more than 256 entries. Empty origin policy accepts clients without an `Origin`
header and rejects browser clients that provide one. `subprotocols` supports up to 64 unique HTTP
token values, each at most 128 bytes; the server selects the first configured protocol offered by
the client. It does not invent a protocol when the client offers none.

`max_connections` applies before WebSocket upgrade; excess TCP clients receive a small HTTP 503 and
are closed. During shutdown, active handlers are cancelled and sessions are closed within
`close_timeout`; handlers must observe cancellation and release upstream subscriptions for drain
to complete. A timeout is reported as a sanitized transport failure.
