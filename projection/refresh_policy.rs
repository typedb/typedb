/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Per-projection refresh policies.
//!
//! Each projection can be assigned a [`RefreshPolicy`] that determines
//! when it should be re-materialized:
//!
//! - [`Eager`](RefreshPolicy::Eager) — refresh immediately after every
//!   data commit that *might* affect the projection's source types.
//! - [`Periodic`](RefreshPolicy::Periodic) — refresh on a fixed interval
//!   (e.g., every 30 seconds).
//! - [`Manual`](RefreshPolicy::Manual) — never refresh automatically;
//!   only on explicit API call.
//!
//! The [`ProjectionRefreshConfig`] struct pairs a projection name with
//! its policy and tracks the last-seen WAL sequence number for
//! incremental change detection.

use std::time::Duration;

// ── Refresh policy ─────────────────────────────────────────────────

/// Policy governing when a projection is re-materialized.
#[derive(Debug, Clone, PartialEq)]
pub enum RefreshPolicy {
    /// Refresh immediately after every relevant data commit.
    Eager,
    /// Refresh on a fixed interval.
    Periodic(Duration),
    /// Only refresh when explicitly requested.
    Manual,
}

impl RefreshPolicy {
    /// Whether this policy requires automatic refresh.
    pub fn is_automatic(&self) -> bool {
        !matches!(self, Self::Manual)
    }

    /// Whether this policy is eager (immediate).
    pub fn is_eager(&self) -> bool {
        matches!(self, Self::Eager)
    }

    /// Return the periodic interval, if applicable.
    pub fn interval(&self) -> Option<Duration> {
        match self {
            Self::Periodic(d) => Some(*d),
            _ => None,
        }
    }
}

impl Default for RefreshPolicy {
    /// Default policy is periodic with a 60-second interval.
    fn default() -> Self {
        Self::Periodic(Duration::from_secs(60))
    }
}

impl std::fmt::Display for RefreshPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eager => write!(f, "eager"),
            Self::Periodic(d) => write!(f, "periodic({}s)", d.as_secs()),
            Self::Manual => write!(f, "manual"),
        }
    }
}

// ── Per-projection refresh configuration ───────────────────────────

/// Tracks refresh policy and versioning state for a single projection.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionRefreshConfig {
    /// Projection name (matches the `ProjectionDefinition::name()`).
    projection_name: String,
    /// Refresh policy.
    policy: RefreshPolicy,
    /// Last WAL sequence number processed for this projection.
    /// `0` means never refreshed.
    last_sequence_number: u64,
    /// Number of times this projection has been refreshed.
    refresh_count: u64,
}

impl ProjectionRefreshConfig {
    /// Create a new config for a projection with no refresh history.
    pub fn new(projection_name: impl Into<String>, policy: RefreshPolicy) -> Self {
        Self { projection_name: projection_name.into(), policy, last_sequence_number: 0, refresh_count: 0 }
    }

    /// Create a config with default (periodic 60s) policy.
    pub fn with_default_policy(projection_name: impl Into<String>) -> Self {
        Self::new(projection_name, RefreshPolicy::default())
    }

    // ── Accessors ──────────────────────────────────────────────

    pub fn projection_name(&self) -> &str {
        &self.projection_name
    }

    pub fn policy(&self) -> &RefreshPolicy {
        &self.policy
    }

    pub fn last_sequence_number(&self) -> u64 {
        self.last_sequence_number
    }

    pub fn refresh_count(&self) -> u64 {
        self.refresh_count
    }

    /// Whether this projection has ever been refreshed.
    pub fn has_been_refreshed(&self) -> bool {
        self.refresh_count > 0
    }

    // ── Mutators ───────────────────────────────────────────────

    /// Update the policy.
    pub fn set_policy(&mut self, policy: RefreshPolicy) {
        self.policy = policy;
    }

    /// Record that a refresh completed at the given sequence number.
    pub fn record_refresh(&mut self, sequence_number: u64) {
        self.last_sequence_number = sequence_number;
        self.refresh_count += 1;
    }

    /// Whether a refresh is needed given the current WAL position.
    ///
    /// Returns `true` if:
    /// - The projection has never been refreshed, OR
    /// - There are new commits after `last_sequence_number`
    pub fn needs_refresh(&self, current_sequence_number: u64) -> bool {
        self.refresh_count == 0 || current_sequence_number > self.last_sequence_number
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── RefreshPolicy ────────────────────────────────────────────

    #[test]
    fn eager_is_automatic() {
        assert!(RefreshPolicy::Eager.is_automatic());
    }

    #[test]
    fn periodic_is_automatic() {
        assert!(RefreshPolicy::Periodic(Duration::from_secs(30)).is_automatic());
    }

    #[test]
    fn manual_is_not_automatic() {
        assert!(!RefreshPolicy::Manual.is_automatic());
    }

    #[test]
    fn eager_is_eager() {
        assert!(RefreshPolicy::Eager.is_eager());
    }

    #[test]
    fn periodic_is_not_eager() {
        assert!(!RefreshPolicy::Periodic(Duration::from_secs(10)).is_eager());
    }

    #[test]
    fn manual_is_not_eager() {
        assert!(!RefreshPolicy::Manual.is_eager());
    }

    #[test]
    fn interval_for_periodic() {
        let d = Duration::from_secs(45);
        assert_eq!(RefreshPolicy::Periodic(d).interval(), Some(d));
    }

    #[test]
    fn interval_for_eager_is_none() {
        assert_eq!(RefreshPolicy::Eager.interval(), None);
    }

    #[test]
    fn interval_for_manual_is_none() {
        assert_eq!(RefreshPolicy::Manual.interval(), None);
    }

    #[test]
    fn default_is_periodic_60s() {
        let policy = RefreshPolicy::default();
        assert_eq!(policy, RefreshPolicy::Periodic(Duration::from_secs(60)));
    }

    #[test]
    fn display_eager() {
        assert_eq!(RefreshPolicy::Eager.to_string(), "eager");
    }

    #[test]
    fn display_periodic() {
        assert_eq!(RefreshPolicy::Periodic(Duration::from_secs(30)).to_string(), "periodic(30s)");
    }

    #[test]
    fn display_manual() {
        assert_eq!(RefreshPolicy::Manual.to_string(), "manual");
    }

    #[test]
    fn policy_is_cloneable() {
        let p = RefreshPolicy::Periodic(Duration::from_secs(10));
        assert_eq!(p.clone(), p);
    }

    // ── ProjectionRefreshConfig ──────────────────────────────────

    #[test]
    fn new_config_defaults() {
        let cfg = ProjectionRefreshConfig::new("people", RefreshPolicy::Eager);
        assert_eq!(cfg.projection_name(), "people");
        assert_eq!(cfg.policy(), &RefreshPolicy::Eager);
        assert_eq!(cfg.last_sequence_number(), 0);
        assert_eq!(cfg.refresh_count(), 0);
        assert!(!cfg.has_been_refreshed());
    }

    #[test]
    fn with_default_policy_is_periodic() {
        let cfg = ProjectionRefreshConfig::with_default_policy("tasks");
        assert_eq!(cfg.policy(), &RefreshPolicy::Periodic(Duration::from_secs(60)));
    }

    #[test]
    fn record_refresh_updates_state() {
        let mut cfg = ProjectionRefreshConfig::new("X", RefreshPolicy::Eager);
        cfg.record_refresh(42);
        assert_eq!(cfg.last_sequence_number(), 42);
        assert_eq!(cfg.refresh_count(), 1);
        assert!(cfg.has_been_refreshed());
    }

    #[test]
    fn record_refresh_increments_count() {
        let mut cfg = ProjectionRefreshConfig::new("X", RefreshPolicy::Manual);
        cfg.record_refresh(10);
        cfg.record_refresh(20);
        cfg.record_refresh(30);
        assert_eq!(cfg.refresh_count(), 3);
        assert_eq!(cfg.last_sequence_number(), 30);
    }

    #[test]
    fn set_policy_changes_policy() {
        let mut cfg = ProjectionRefreshConfig::new("X", RefreshPolicy::Manual);
        cfg.set_policy(RefreshPolicy::Eager);
        assert_eq!(cfg.policy(), &RefreshPolicy::Eager);
    }

    #[test]
    fn needs_refresh_when_never_refreshed() {
        let cfg = ProjectionRefreshConfig::new("X", RefreshPolicy::Eager);
        assert!(cfg.needs_refresh(0));
        assert!(cfg.needs_refresh(100));
    }

    #[test]
    fn needs_refresh_when_new_commits() {
        let mut cfg = ProjectionRefreshConfig::new("X", RefreshPolicy::Eager);
        cfg.record_refresh(10);
        assert!(cfg.needs_refresh(11));
        assert!(cfg.needs_refresh(100));
    }

    #[test]
    fn no_refresh_needed_when_caught_up() {
        let mut cfg = ProjectionRefreshConfig::new("X", RefreshPolicy::Eager);
        cfg.record_refresh(10);
        assert!(!cfg.needs_refresh(10));
        assert!(!cfg.needs_refresh(5)); // even older
    }

    #[test]
    fn config_is_cloneable() {
        let cfg = ProjectionRefreshConfig::new("X", RefreshPolicy::Eager);
        assert_eq!(cfg.clone(), cfg);
    }

    #[test]
    fn config_is_debuggable() {
        let cfg = ProjectionRefreshConfig::new("X", RefreshPolicy::Eager);
        let debug = format!("{:?}", cfg);
        assert!(debug.contains("X"));
    }
}
