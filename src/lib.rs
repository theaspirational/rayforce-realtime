//! `rayforce-realtime` — transport-agnostic query subscriptions.
//!
//! Responsibilities:
//!
//! * Hold a registry of active `Subscription`s — each one carrying a
//!   `QuerySpec`, a `TouchedKey`-pattern dependency set, and the
//!   `AuthContext` it was created under.
//! * On every `CommitResult`, match `TouchedKeys` against subscription
//!   dependencies and emit one `Invalidation` per affected subscription.
//! * Filter emitted invalidations at delivery time using the current
//!   `AuthContext` — losing access to a scope must stop further events
//!   for that scope AND produce a cache-purge signal.
//!
//! The core is sync and transport-agnostic. App code wires it to SSE,
//! WebSocket, or any other transport by draining `Invalidation`s from the
//! returned iterator and pushing them down its preferred channel.

pub mod bus;
pub mod subscription;

pub use bus::{Invalidation, RealtimeBus};
pub use subscription::{Dependency, QuerySpec, Subscription, SubscriptionId};
