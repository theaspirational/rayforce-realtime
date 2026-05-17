//! `Subscription`, `QuerySpec`, `Dependency`, `SubscriptionId`.

use serde::{Deserialize, Serialize};

use ray_datom::scope::ScopeId;
use ray_datom::value::EntityId;
use ray_transactor::auth::{AuthContext, Permission};

/// Opaque subscription handle. Stable across the bus's lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SubscriptionId(pub u64);

/// Application-defined query identifier. The bus stores it verbatim and
/// hands it back with each `Invalidation` so the client can rerun the
/// matching secured query.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QuerySpec {
    pub view: String,
    pub args: serde_json::Value,
}

impl QuerySpec {
    pub fn new(view: impl Into<String>, args: serde_json::Value) -> Self {
        QuerySpec {
            view: view.into(),
            args,
        }
    }
}

/// A pattern that a subscription cares about. Matches the `TouchedKeys`
/// emitted with each commit.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Dependency {
    Scope(ScopeId),
    Entity(EntityId),
    Attr(String),
    View(String),
}

#[derive(Clone, Debug)]
pub struct Subscription {
    pub id: SubscriptionId,
    pub spec: QuerySpec,
    pub deps: Vec<Dependency>,
    pub auth: AuthContext,
    /// Permission required to *read* the data the subscription targets.
    /// Used at delivery time to filter scopes the subscriber has lost.
    pub read_permission: Permission,
}
