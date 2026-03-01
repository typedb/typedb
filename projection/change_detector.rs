/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Change detection abstraction over the TypeDB write-ahead log.
//!
//! The TypeDB WAL has no built-in subscription mechanism — change
//! detection is pull-based via `iter_any_from(last_seen_seq)`. This
//! module provides a [`ChangeDetector`] trait that abstracts over the
//! polling model so that the [`BackgroundRefresher`](crate::background_refresher)
//! can detect new commits without depending on internal storage types.
//!
//! # Server integration
//!
//! The server layer implements [`ChangeDetector`] by wrapping the
//! `DurabilityClient::iter_type_from::<CommitRecord>()` API. Each
//! [`ChangeEvent`] maps to a committed WAL record whose
//! `StatusRecord.was_committed == true`.

// ── Change event ───────────────────────────────────────────────────

/// A single committed data change detected from the WAL.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeEvent {
    /// WAL sequence number of the commit.
    pub sequence_number: u64,
    /// Whether this was a schema change (`true`) or data change (`false`).
    pub is_schema_change: bool,
}

impl ChangeEvent {
    pub fn data(sequence_number: u64) -> Self {
        Self { sequence_number, is_schema_change: false }
    }

    pub fn schema(sequence_number: u64) -> Self {
        Self { sequence_number, is_schema_change: true }
    }
}

// ── Change detector trait ──────────────────────────────────────────

/// Abstraction over WAL polling for change detection.
///
/// Implementations scan the WAL for committed records since a given
/// sequence number and return them as [`ChangeEvent`]s.
///
/// The server layer implements this by wrapping
/// `DurabilityClient::iter_type_from::<CommitRecord>()` and filtering
/// for successfully committed records (matching `StatusRecord`).
pub trait ChangeDetector: Send + Sync {
    /// Poll for new changes since `after_sequence_number`.
    ///
    /// Returns all committed changes with sequence numbers strictly
    /// greater than `after_sequence_number`, ordered by sequence number.
    ///
    /// Returns an empty vec if no new changes exist.
    ///
    /// # Errors
    ///
    /// Returns a human-readable error if the WAL cannot be read.
    fn poll_changes(&self, after_sequence_number: u64) -> Result<Vec<ChangeEvent>, String>;

    /// Return the current WAL watermark (highest fully-applied sequence number).
    ///
    /// This is the safe read point — all commits at or below this
    /// number are guaranteed to be visible.
    fn current_watermark(&self) -> Result<u64, String>;
}

// ── Change summary ─────────────────────────────────────────────────

/// Summary of changes detected in a polling cycle.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeSummary {
    /// All change events in the polling cycle.
    pub events: Vec<ChangeEvent>,
    /// Highest sequence number seen (for bookkeeping).
    pub max_sequence_number: u64,
}

impl ChangeSummary {
    /// Build a summary from a set of events.
    pub fn from_events(events: Vec<ChangeEvent>) -> Self {
        let max_sequence_number = events.iter().map(|e| e.sequence_number).max().unwrap_or(0);
        Self { events, max_sequence_number }
    }

    /// Whether there are no changes.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Number of changes.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Number of data-only changes.
    pub fn data_change_count(&self) -> usize {
        self.events.iter().filter(|e| !e.is_schema_change).count()
    }

    /// Number of schema changes.
    pub fn schema_change_count(&self) -> usize {
        self.events.iter().filter(|e| e.is_schema_change).count()
    }

    /// Whether any schema changes occurred (may require projection re-validation).
    pub fn has_schema_changes(&self) -> bool {
        self.events.iter().any(|e| e.is_schema_change)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── ChangeEvent ──────────────────────────────────────────────

    #[test]
    fn data_event_construction() {
        let e = ChangeEvent::data(42);
        assert_eq!(e.sequence_number, 42);
        assert!(!e.is_schema_change);
    }

    #[test]
    fn schema_event_construction() {
        let e = ChangeEvent::schema(99);
        assert_eq!(e.sequence_number, 99);
        assert!(e.is_schema_change);
    }

    #[test]
    fn event_equality() {
        assert_eq!(ChangeEvent::data(1), ChangeEvent::data(1));
        assert_ne!(ChangeEvent::data(1), ChangeEvent::data(2));
        assert_ne!(ChangeEvent::data(1), ChangeEvent::schema(1));
    }

    #[test]
    fn event_is_cloneable() {
        let e = ChangeEvent::data(5);
        assert_eq!(e.clone(), e);
    }

    #[test]
    fn event_is_debuggable() {
        let e = ChangeEvent::schema(10);
        let debug = format!("{:?}", e);
        assert!(debug.contains("10"));
        assert!(debug.contains("true"));
    }

    // ── ChangeSummary ────────────────────────────────────────────

    #[test]
    fn empty_summary() {
        let s = ChangeSummary::from_events(vec![]);
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
        assert_eq!(s.max_sequence_number, 0);
        assert_eq!(s.data_change_count(), 0);
        assert_eq!(s.schema_change_count(), 0);
        assert!(!s.has_schema_changes());
    }

    #[test]
    fn summary_with_data_events() {
        let events = vec![ChangeEvent::data(10), ChangeEvent::data(15), ChangeEvent::data(20)];
        let s = ChangeSummary::from_events(events);
        assert!(!s.is_empty());
        assert_eq!(s.len(), 3);
        assert_eq!(s.max_sequence_number, 20);
        assert_eq!(s.data_change_count(), 3);
        assert_eq!(s.schema_change_count(), 0);
        assert!(!s.has_schema_changes());
    }

    #[test]
    fn summary_with_mixed_events() {
        let events = vec![ChangeEvent::data(10), ChangeEvent::schema(15), ChangeEvent::data(20)];
        let s = ChangeSummary::from_events(events);
        assert_eq!(s.len(), 3);
        assert_eq!(s.max_sequence_number, 20);
        assert_eq!(s.data_change_count(), 2);
        assert_eq!(s.schema_change_count(), 1);
        assert!(s.has_schema_changes());
    }

    #[test]
    fn summary_with_only_schema_events() {
        let events = vec![ChangeEvent::schema(5), ChangeEvent::schema(10)];
        let s = ChangeSummary::from_events(events);
        assert_eq!(s.data_change_count(), 0);
        assert_eq!(s.schema_change_count(), 2);
        assert!(s.has_schema_changes());
    }

    #[test]
    fn summary_single_event() {
        let events = vec![ChangeEvent::data(42)];
        let s = ChangeSummary::from_events(events);
        assert_eq!(s.len(), 1);
        assert_eq!(s.max_sequence_number, 42);
    }

    #[test]
    fn summary_max_finds_highest_sequence() {
        let events = vec![ChangeEvent::data(50), ChangeEvent::data(10), ChangeEvent::data(30)];
        let s = ChangeSummary::from_events(events);
        assert_eq!(s.max_sequence_number, 50);
    }

    #[test]
    fn summary_is_cloneable() {
        let s = ChangeSummary::from_events(vec![ChangeEvent::data(1)]);
        assert_eq!(s.clone(), s);
    }

    // ── Mock ChangeDetector ──────────────────────────────────────

    struct MockDetector {
        events: Vec<ChangeEvent>,
        watermark: u64,
    }

    impl ChangeDetector for MockDetector {
        fn poll_changes(&self, after_sequence_number: u64) -> Result<Vec<ChangeEvent>, String> {
            Ok(self.events.iter().filter(|e| e.sequence_number > after_sequence_number).cloned().collect())
        }

        fn current_watermark(&self) -> Result<u64, String> {
            Ok(self.watermark)
        }
    }

    #[test]
    fn mock_detector_returns_events_after_seq() {
        let detector = MockDetector {
            events: vec![ChangeEvent::data(5), ChangeEvent::data(10), ChangeEvent::data(15)],
            watermark: 15,
        };
        let changes = detector.poll_changes(5).unwrap();
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].sequence_number, 10);
        assert_eq!(changes[1].sequence_number, 15);
    }

    #[test]
    fn mock_detector_returns_empty_when_caught_up() {
        let detector = MockDetector { events: vec![ChangeEvent::data(5)], watermark: 5 };
        let changes = detector.poll_changes(5).unwrap();
        assert!(changes.is_empty());
    }

    #[test]
    fn mock_detector_returns_all_when_starting_from_zero() {
        let detector = MockDetector {
            events: vec![ChangeEvent::data(1), ChangeEvent::schema(2), ChangeEvent::data(3)],
            watermark: 3,
        };
        let changes = detector.poll_changes(0).unwrap();
        assert_eq!(changes.len(), 3);
    }

    #[test]
    fn mock_detector_watermark() {
        let detector = MockDetector { events: vec![], watermark: 42 };
        assert_eq!(detector.current_watermark().unwrap(), 42);
    }
}
