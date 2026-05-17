//! `RealtimeBus` — sync, in-memory subscription registry and invalidation
//! matcher.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use ray_datom::scope::ScopeId;
use ray_transactor::auth::AuthContext;
use ray_transactor::command::{CommitResult, TouchedKeys};

use crate::subscription::{Dependency, QuerySpec, Subscription, SubscriptionId};

/// Event delivered to a subscriber.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Invalidation {
    pub subscription: SubscriptionId,
    pub spec: QuerySpec,
    pub reason: InvalidationReason,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InvalidationReason {
    /// A commit touched a key the subscription depends on.
    Touched { tx_id: u64, scopes: Vec<ScopeId> },
    /// Permission was revoked; the subscriber should purge any cached
    /// results scoped to `scopes`.
    PermissionRevoked { scopes: Vec<ScopeId> },
}

#[derive(Default)]
pub struct RealtimeBus {
    next_id: u64,
    subscriptions: HashMap<SubscriptionId, Subscription>,
}

impl RealtimeBus {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(
        &mut self,
        auth: AuthContext,
        spec: QuerySpec,
        deps: Vec<Dependency>,
        read_permission: ray_transactor::auth::Permission,
    ) -> SubscriptionId {
        let id = SubscriptionId(self.next_id);
        self.next_id += 1;
        self.subscriptions.insert(
            id,
            Subscription {
                id,
                spec,
                deps,
                auth,
                read_permission,
            },
        );
        id
    }

    pub fn unsubscribe(&mut self, id: SubscriptionId) {
        self.subscriptions.remove(&id);
    }

    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    /// Match a commit against every subscription. Returns one `Invalidation`
    /// per subscription whose `Dependency` set overlaps the commit's
    /// `TouchedKeys` AND whose subscriber is currently authorized for at
    /// least one of the touched scopes.
    pub fn match_commit(&self, result: &CommitResult) -> Vec<Invalidation> {
        let mut out = Vec::new();
        for sub in self.subscriptions.values() {
            if !overlaps(&sub.deps, &result.touched) {
                continue;
            }
            let allowed: Vec<ScopeId> = result
                .touched
                .scopes
                .iter()
                .filter(|s| sub.auth.authorized(s, &sub.read_permission))
                .cloned()
                .collect();
            if !result.touched.scopes.is_empty() && allowed.is_empty() {
                continue;
            }
            out.push(Invalidation {
                subscription: sub.id,
                spec: sub.spec.clone(),
                reason: InvalidationReason::Touched {
                    tx_id: result.tx_id.0,
                    scopes: allowed,
                },
            });
        }
        out
    }

    /// Re-evaluate every subscription against an updated auth context for
    /// `principal`. Emits a `PermissionRevoked` invalidation for any scope
    /// the subscriber previously had access to but no longer does.
    ///
    /// `previous_auth` is the auth context the subscription was created
    /// under (or last refreshed with); `new_auth` is the post-change snapshot.
    pub fn match_permission_revoke(
        &self,
        principal: &ray_datom::tx::PrincipalId,
        previous_auth: &AuthContext,
        new_auth: &AuthContext,
    ) -> Vec<Invalidation> {
        let mut out = Vec::new();
        for sub in self.subscriptions.values() {
            if &sub.auth.principal != principal {
                continue;
            }
            let was = previous_auth.readable_scopes(&sub.read_permission);
            let now = new_auth.readable_scopes(&sub.read_permission);
            let lost: Vec<ScopeId> = was.difference(&now).cloned().collect();
            if lost.is_empty() {
                continue;
            }
            out.push(Invalidation {
                subscription: sub.id,
                spec: sub.spec.clone(),
                reason: InvalidationReason::PermissionRevoked { scopes: lost },
            });
        }
        out
    }
}

fn overlaps(deps: &[Dependency], touched: &TouchedKeys) -> bool {
    for dep in deps {
        match dep {
            Dependency::Scope(s) => {
                if touched.scopes.contains(s) {
                    return true;
                }
            }
            Dependency::Entity(e) => {
                if touched.entities.contains(e) {
                    return true;
                }
            }
            Dependency::Attr(a) => {
                if touched.attrs.contains(a) {
                    return true;
                }
            }
            Dependency::View(_) => {
                // View-level deps are application-defined: bus consumers
                // who want view invalidation should also add the relevant
                // scope/entity/attr deps.
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use ray_datom::tx::{ActorKind, PrincipalId, TxId};
    use ray_datom::value::EntityId;
    use ray_transactor::auth::{Grant, Permission};

    fn alice_with(scopes: &[&str], perm: &str) -> AuthContext {
        let grants = scopes.iter().map(|s| Grant {
            principal: PrincipalId::new("principal/alice"),
            scope: ScopeId::new(*s),
            permission: Permission::new(perm),
        });
        AuthContext::new(PrincipalId::new("principal/alice"), ActorKind::Human).with_grants(grants)
    }

    fn touched(scopes: &[&str], entities: &[&str], attrs: &[&str]) -> TouchedKeys {
        let mut t = TouchedKeys::default();
        t.scopes.extend(scopes.iter().map(|s| ScopeId::new(*s)));
        t.entities
            .extend(entities.iter().map(|e| EntityId::new(*e)));
        t.attrs.extend(attrs.iter().map(|a| a.to_string()));
        t
    }

    fn commit(scopes: &[&str], entities: &[&str], attrs: &[&str]) -> CommitResult {
        CommitResult {
            tx_id: TxId(42),
            datoms: Vec::new(),
            touched: touched(scopes, entities, attrs),
            idempotent_replay: false,
        }
    }

    #[test]
    fn touched_scope_invalidates_subscription() {
        let mut bus = RealtimeBus::new();
        let auth = alice_with(&["scope/A"], "read");
        let id = bus.subscribe(
            auth,
            QuerySpec::new("task-board", serde_json::json!({"project": "P1"})),
            vec![Dependency::Scope(ScopeId::new("scope/A"))],
            Permission::new("read"),
        );

        let evs = bus.match_commit(&commit(&["scope/A"], &[], &[]));
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].subscription, id);
        match &evs[0].reason {
            InvalidationReason::Touched { scopes, .. } => {
                assert_eq!(scopes, &vec![ScopeId::new("scope/A")]);
            }
            other => panic!("unexpected reason: {other:?}"),
        }
    }

    #[test]
    fn unauthorized_scope_filtered_out() {
        let mut bus = RealtimeBus::new();
        let auth = alice_with(&["scope/A"], "read");
        bus.subscribe(
            auth,
            QuerySpec::new("task-board", serde_json::json!({})),
            vec![Dependency::Scope(ScopeId::new("scope/B"))],
            Permission::new("read"),
        );

        // Commit touches scope/B which subscription depends on, but alice
        // has no grant for it — should NOT emit.
        let evs = bus.match_commit(&commit(&["scope/B"], &[], &[]));
        assert!(evs.is_empty());
    }

    #[test]
    fn entity_and_attr_deps_match() {
        let mut bus = RealtimeBus::new();
        let auth = alice_with(&["scope/A"], "read");
        bus.subscribe(
            auth.clone(),
            QuerySpec::new("task-detail", serde_json::json!({"task": "T1"})),
            vec![Dependency::Entity(EntityId::new("task/T1"))],
            Permission::new("read"),
        );
        bus.subscribe(
            auth,
            QuerySpec::new("by-state", serde_json::json!({})),
            vec![Dependency::Attr("task/state".into())],
            Permission::new("read"),
        );

        // No scopes touched so the bus skips the scope-readability gate.
        let evs = bus.match_commit(&commit(&[], &["task/T1"], &["task/state"]));
        assert_eq!(evs.len(), 2);
    }

    #[test]
    fn permission_revoke_emits_purge() {
        let mut bus = RealtimeBus::new();
        let was = alice_with(&["scope/A", "scope/B"], "read");
        let now = alice_with(&["scope/A"], "read");
        bus.subscribe(
            was.clone(),
            QuerySpec::new("board", serde_json::json!({})),
            vec![Dependency::Scope(ScopeId::new("scope/B"))],
            Permission::new("read"),
        );

        let evs = bus.match_permission_revoke(&PrincipalId::new("principal/alice"), &was, &now);
        assert_eq!(evs.len(), 1);
        match &evs[0].reason {
            InvalidationReason::PermissionRevoked { scopes } => {
                assert_eq!(scopes, &vec![ScopeId::new("scope/B")]);
            }
            other => panic!("unexpected reason: {other:?}"),
        }
    }
}
