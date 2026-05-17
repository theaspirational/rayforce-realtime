# rayforce-realtime

`rayforce-realtime` is a transport-agnostic invalidation layer for datom-backed
applications.

It provides:

- subscription registration with query specs and dependency keys
- commit-result matching against touched scopes, entities, and attributes
- delivery-time authorization filtering
- permission-revocation invalidations for client cache purge behavior

The crate does not open sockets. Applications wire invalidations to SSE,
WebSocket, or another transport.

## License

MIT
