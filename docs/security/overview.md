# Security

Origin validation reduces cross-site WebSocket hijacking risk for browser clients but does not
authenticate users. Configure an exact origin allowlist, authenticate connections through the
application, and authorize each message or subscription. Keep the listener private behind TLS
ingress, bound connections and message sizes, and use ingress and application deadlines.

`Message` and `Close` redact payload and reason from `Debug`. Transport failures are mapped to
fixed error categories and close reasons. Do not log raw message data or bearer credentials.
