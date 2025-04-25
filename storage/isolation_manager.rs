/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// TODO: Check atomic Ordering constraints. We're using SeqCst where we don't have to
// TODO: Benchmark with many small commits to see if the read-write locks affect latency.

use std::{
    cmp::max,
    collections::{HashMap, VecDeque},
    error::Error,
    fmt,
    io::Read,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        Arc, OnceLock, RwLock,
    },
};

use durability::DurabilityRecordType;
use logger::result::ResultExt;
use primitive::maybe_owns::MaybeOwns;
use resource::constants::storage::TIMELINE_WINDOW_SIZE;
use serde::{Deserialize, Serialize};

use crate::{
    durability_client::{
        DurabilityClient, DurabilityClientError, DurabilityRecord, SequencedDurabilityRecord,
        UnsequencedDurabilityRecord,
    },
    sequence_number::SequenceNumber,
    snapshot::{buffer::OperationsBuffer, lock::LockType, write::Write},
    write_batches::WriteBatches,
};

#[derive(Debug)]
pub(crate) struct IsolationManager {
    initial_sequence_number: SequenceNumber,
    timeline: Timeline,
    highest_validated_sequence_number: AtomicU64,
}

impl fmt::Display for IsolationManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Timeline[windows={}, watermark={}]", self.timeline.window_count(), self.watermark())
    }
}

impl IsolationManager {
    pub(crate) fn new(next_sequence_number: SequenceNumber) -> IsolationManager {
        IsolationManager {
            initial_sequence_number: next_sequence_number,
            timeline: Timeline::new(next_sequence_number),
            highest_validated_sequence_number: AtomicU64::new(next_sequence_number.number() - 1),
        }
    }

    pub(crate) fn opened_for_read(&self, sequence_number: SequenceNumber) {
        debug_assert!(
            sequence_number <= self.watermark(),
            "assertion `{} <= {}` failed",
            sequence_number,
            self.watermark()
        );
        self.timeline.record_reader(sequence_number);
    }

    pub(crate) fn closed_for_read(&self, sequence_number: SequenceNumber) {
        self.timeline.remove_reader(sequence_number);
    }

    pub(crate) fn applied(&self, sequence_number: SequenceNumber) -> Result<(), ExpectedWindowError> {
        self.timeline
            .try_get_window(sequence_number)
            .ok_or(ExpectedWindowError { sequence_number })?
            .set_applied(sequence_number);
        self.timeline.may_increment_watermark(sequence_number);
        Ok(())
    }

    pub(crate) fn load_validated(&self, sequence_number: SequenceNumber, commit_record: CommitRecord) {
        let window = self.timeline.get_or_create_window(sequence_number);
        window.insert_pending(sequence_number, commit_record);
        window.set_validated(sequence_number);
        drop(window);
        self.timeline.may_increment_watermark(sequence_number);
    }

    pub(crate) fn load_aborted(&self, sequence_number: SequenceNumber) {
        let window = self.timeline.get_or_create_window(sequence_number);
        window.set_aborted(sequence_number);
        drop(window);
        self.timeline.may_increment_watermark(sequence_number);
    }

    pub(crate) fn validate_commit(
        &self,
        sequence_number: SequenceNumber,
        commit_record: CommitRecord,
        durability_client: &impl DurabilityClient,
    ) -> Result<ValidatedCommit, DurabilityClientError> {
        let window = self.timeline.get_or_create_window(sequence_number);
        window.insert_pending(sequence_number, commit_record);
        let CommitStatus::Pending(commit_record) = window.get_status(sequence_number) else { unreachable!() };
        let isolation_conflict = self.validate_all_concurrent(sequence_number, &commit_record, durability_client)?;
        if isolation_conflict.is_none() {
            window.set_validated(sequence_number);
            // We can't increment watermark here till the status is "applied", but we do update the latest validated number
            self.highest_validated_sequence_number.fetch_max(sequence_number.number(), Ordering::SeqCst);
        } else {
            window.set_aborted(sequence_number);
            self.timeline.may_increment_watermark(sequence_number);
        }
        self.timeline.remove_reader(commit_record.open_sequence_number);
        match isolation_conflict {
            Some(conflict) => Ok(ValidatedCommit::Conflict(conflict)),
            None => {
                let commit_record = match window.get_status(sequence_number) {
                    CommitStatus::Validated(commit_record) | CommitStatus::Applied(commit_record) => commit_record,
                    _ => panic!("get_commit_record called on uncommitted record"), // TODO: Do we want to be able to apply on pending?
                };
                Ok(ValidatedCommit::Write(WriteBatches::from_operations(sequence_number, commit_record.operations())))
            }
        }
    }

    fn validate_all_concurrent(
        &self,
        commit_sequence_number: SequenceNumber,
        commit_record: &CommitRecord,
        durability_client: &impl DurabilityClient,
    ) -> Result<Option<IsolationConflict>, DurabilityClientError> {
        // TODO: decide if we should block until all predecessors finish, allow out of order (non-Calvin model/traditional model)
        //       We could also validate against all predecessors even if they are validating and fail eagerly.
        // TODO: Should we validate from the timeline before going to disk?

        // Pre-collect all the ARCs so we can validate against them.
        let (windows, first_sequence_number_in_memory) =
            self.timeline.collect_concurrent_windows(commit_record.open_sequence_number, commit_sequence_number);
        if commit_record.open_sequence_number().next() < first_sequence_number_in_memory {
            if let Some(conflict) =
                self.validate_concurrent_from_disk(commit_record, first_sequence_number_in_memory, durability_client)?
            {
                return Ok(Some(conflict));
            }
        }

        Ok(self.validate_concurrent_from_windows(
            commit_record,
            commit_sequence_number,
            &windows,
            first_sequence_number_in_memory,
        ))
    }

    fn validate_concurrent_from_disk(
        &self,
        commit_record: &CommitRecord,
        stop_sequence_number: SequenceNumber,
        durability_client: &impl DurabilityClient,
    ) -> Result<Option<IsolationConflict>, DurabilityClientError> {
        for commit_status_result in Self::iterate_commit_status_from_disk(
            durability_client,
            commit_record.open_sequence_number.next(),
            stop_sequence_number,
        )? {
            if let Ok((_, commit_status)) = commit_status_result {
                let commit_dependency = match commit_status {
                    CommitStatus::Aborted => CommitDependency::Independent,
                    CommitStatus::Applied(predecessor_record) => commit_record.compute_dependency(&predecessor_record),
                    CommitStatus::Pending(_) => {
                        unreachable!("Evicted records cannot be pending")
                    }
                    CommitStatus::Empty | CommitStatus::Validated(_) => unreachable!(),
                };
                if let Some(conflict) = handle_dependency(commit_dependency) {
                    return Ok(Some(conflict));
                }
            } else if let Err(err) = commit_status_result {
                return Err(err);
            }
        }
        Ok(None)
    }

    fn validate_concurrent_from_windows(
        &self,
        commit_record: &CommitRecord,
        commit_sequence_number: SequenceNumber,
        windows: &[Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>],
        first_window_sequence_number: SequenceNumber,
    ) -> Option<IsolationConflict> {
        let start_validation_index = max(commit_record.open_sequence_number.next(), first_window_sequence_number);
        debug_assert!(start_validation_index <= first_window_sequence_number + TIMELINE_WINDOW_SIZE);
        let mut window_index = 0;
        for validate_against in start_validation_index.number()..commit_sequence_number.number() {
            let validate_against = SequenceNumber::from(validate_against);
            let window = &windows[window_index];
            debug_assert!(window_index < windows.len());
            if let Some(conflict) = resolve_concurrent(commit_record, validate_against, window) {
                return Some(conflict);
            }
            if validate_against + 1 >= window.end() {
                window_index += 1;
            }
        }
        None
    }

    pub(crate) fn iterate_commit_status_from_disk(
        durability_client: &impl DurabilityClient,
        start_sequence_number: SequenceNumber,
        stop_sequence_number: SequenceNumber,
    ) -> Result<
        impl Iterator<Item = Result<(SequenceNumber, CommitStatus<'_>), DurabilityClientError>>,
        DurabilityClientError,
    > {
        let mut is_committed = HashMap::new();
        for record in durability_client.iter_unsequenced_type_from::<StatusRecord>(start_sequence_number)? {
            let record = record?;
            // We can't stop early because status records may be out-of-order
            is_committed.insert(record.commit_record_sequence_number(), record.was_committed());
        }

        Ok(durability_client.iter_sequenced_type_from::<CommitRecord>(start_sequence_number)?.map_while(
            move |result| match result {
                Ok((commit_sequence_number, commit_record)) => {
                    if commit_sequence_number >= stop_sequence_number {
                        None
                    } else {
                        let status = match is_committed.get(&commit_sequence_number) {
                            None => CommitStatus::Pending(MaybeOwns::Owned(commit_record)),
                            Some(true) => CommitStatus::Applied(MaybeOwns::Owned(commit_record)),
                            Some(false) => CommitStatus::Aborted,
                        };
                        Some(Ok((commit_sequence_number, status)))
                    }
                }
                Err(err) => Some(Err(err)),
            },
        ))
    }

    pub(crate) fn watermark(&self) -> SequenceNumber {
        self.timeline.watermark()
    }

    pub(crate) fn highest_validated_sequence_number(&self) -> SequenceNumber {
        SequenceNumber::new(self.highest_validated_sequence_number.load(Ordering::SeqCst))
    }

    pub fn reset(&mut self) {
        self.timeline = Timeline::new(self.initial_sequence_number)
    }
}

pub(crate) enum ValidatedCommit {
    Conflict(IsolationConflict),
    Write(WriteBatches),
}

fn resolve_concurrent(
    commit_record: &CommitRecord,
    predecessor_sequence_number: SequenceNumber,
    predecessor_window: &TimelineWindow<TIMELINE_WINDOW_SIZE>,
) -> Option<IsolationConflict> {
    while matches!(predecessor_window.get_status(predecessor_sequence_number), CommitStatus::Empty) {
        // Race condition
        std::hint::spin_loop();
    }
    let commit_dependency = match predecessor_window.get_status(predecessor_sequence_number) {
        CommitStatus::Empty => unreachable!("A concurrent status should never be empty at commit time"),
        CommitStatus::Pending(predecessor_record) => match commit_record.compute_dependency(&predecessor_record) {
            CommitDependency::Independent => CommitDependency::Independent,
            result => {
                if predecessor_window.await_pending_status_commits(predecessor_sequence_number) {
                    result
                } else {
                    CommitDependency::Independent
                }
            }
        },
        CommitStatus::Validated(predecessor_record) | CommitStatus::Applied(predecessor_record) => {
            commit_record.compute_dependency(&predecessor_record)
        }
        CommitStatus::Aborted => CommitDependency::Independent,
    };
    handle_dependency(commit_dependency)
}

fn handle_dependency(commit_dependency: CommitDependency) -> Option<IsolationConflict> {
    match commit_dependency {
        CommitDependency::Independent => (),
        CommitDependency::DependentPuts { puts } => puts.into_iter().for_each(DependentPut::apply),
        CommitDependency::Conflict(conflict) => return Some(conflict),
    }
    None
}

#[derive(Debug, Clone)]
enum DependentPut {
    Deleted { reinsert: Arc<AtomicBool> },
    Inserted { reinsert: Arc<AtomicBool> },
}

impl DependentPut {
    fn apply(self) {
        match self {
            DependentPut::Deleted { reinsert } => reinsert.store(true, Ordering::Release),
            DependentPut::Inserted { reinsert } => reinsert.store(false, Ordering::Release),
        }
    }
}

#[derive(Debug)]
enum CommitDependency {
    Independent,
    DependentPuts { puts: Vec<DependentPut> },
    Conflict(IsolationConflict),
}

#[derive(Debug, Clone)]
pub enum IsolationConflict {
    DeletingRequiredKey,
    RequireDeletedKey,
    ExclusiveLock,
}

impl fmt::Display for IsolationConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IsolationConflict::DeletingRequiredKey => write!(f, "Transaction data a concurrent commit requires."),
            IsolationConflict::RequireDeletedKey => write!(f, "Transaction uses data a concurrent commit deletes."),
            IsolationConflict::ExclusiveLock => write!(f, "Transaction uses a lock held by a concurrent commit."),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ExpectedWindowError {
    sequence_number: SequenceNumber,
}

impl fmt::Display for ExpectedWindowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Unexpected internal error: could not find timeline window containing sequence number {}",
            self.sequence_number
        )
    }
}

impl Error for ExpectedWindowError {}

/// Timeline concept:
///   Timeline is made of Windows. Each Window stores a number of Slots.
///   Conceptually the timeline is one sequence of Slots, but we cut it into Windows for more efficient allocation/clean up/search.
///   Having windows also allows us to hold write-locks less often. If we decide we want a fixed
///   The timeline should not clean up old windows while a 'reader' (ie open snapshot) is open on a window or an older window.
///
///   On commit, we
///     1) notify the commit is pending, writing the commit record into the Slot for its commit sequence number.
///     2) when validation has finished, record into the Slot for its commit sequence number whether
///         it is sucessfully 'validated' or must be 'aborted'.
///
#[derive(Debug)]
struct Timeline {
    // We can adjust the Window size to amortise the cost of the read-write locks to maintain the timeline
    windows: RwLock<VecDeque<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>>>,
    watermark: AtomicU64,
}

impl Timeline {
    // The whole of the timeline uses the underlying u64
    fn new(next_sequence_number: SequenceNumber) -> Timeline {
        let windows = VecDeque::from([Arc::new(TimelineWindow::new(next_sequence_number))]);
        Timeline { windows: RwLock::new(windows), watermark: AtomicU64::new(next_sequence_number.number() - 1) }
    }

    fn may_free_windows(&self) {
        let watermark = self.watermark();
        let can_free_some =
            self.windows.read().unwrap_or_log().front().is_some_and(|f| f.get_readers() == 0 && watermark >= f.end());
        if can_free_some {
            let windows = &mut *self.windows.write().unwrap_or_log();
            while watermark >= windows.front().unwrap().end() && windows.front().unwrap().get_readers() == 0 {
                windows.pop_front();
            }
        }
    }

    fn may_increment_watermark(&self, sequence_number: SequenceNumber) {
        if self.watermark() != sequence_number - 1 {
            return;
        }

        let mut candidate_watermark = sequence_number;
        {
            let mut window = self.try_get_window(sequence_number);
            while window.is_some() {
                let should_update = window.as_ref().is_some_and(|window| {
                    matches!(window.get_status(candidate_watermark), CommitStatus::Aborted | CommitStatus::Applied(_))
                });
                if should_update
                    && self
                        .watermark
                        .compare_exchange(
                            (candidate_watermark - 1).number(),
                            candidate_watermark.number(),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                {
                    candidate_watermark += 1;
                    if candidate_watermark >= window.as_ref().unwrap().end() {
                        drop(window.take());
                        window = self.try_get_window(candidate_watermark);
                    }
                } else {
                    break;
                }
            }
        }

        let watermark = candidate_watermark - 1; // Invaraint
        if let Some(watermark_window_end) = { self.try_get_window(sequence_number - 1).map(|w| w.end()) } {
            if watermark >= watermark_window_end {
                self.may_free_windows();
            }
        }
    }

    fn watermark(&self) -> SequenceNumber {
        SequenceNumber::from(self.watermark.load(Ordering::SeqCst))
    }

    fn record_reader(&self, sequence_number: SequenceNumber) {
        if let Some(window) = self.try_get_window(sequence_number) {
            window.increment_readers();
        }
    }

    fn remove_reader(&self, sequence_number: SequenceNumber) {
        if let Some(window) = self.try_get_window(sequence_number) {
            if window.decrement_readers() == 0 {
                drop(window);
                self.may_free_windows();
            }
        };
    }

    fn collect_concurrent_windows(
        &self,
        open_sequence_number: SequenceNumber,
        commit_sequence_number: SequenceNumber,
    ) -> (Vec<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>>, SequenceNumber) {
        let windows = &*self.windows.read().unwrap_or_log();
        let first_concurrent_window_index = Self::resolve_window(windows, open_sequence_number.next()).unwrap_or(0);
        let last_concurrent_window_index =
            Self::resolve_window(windows, commit_sequence_number.previous()).unwrap_or(0);
        let mut concurrent_windows: Vec<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>> = Vec::new();
        (first_concurrent_window_index..=last_concurrent_window_index).for_each(|window_index| {
            concurrent_windows.push(windows.get(window_index).unwrap().clone());
        });
        let start_index_of_first_concurrent_window = windows.get(first_concurrent_window_index).unwrap().start();
        (concurrent_windows, start_index_of_first_concurrent_window)
    }

    fn try_get_window(&self, sequence_number: SequenceNumber) -> Option<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>> {
        let windows = self.windows.read().unwrap_or_log();
        let window_index = Self::resolve_window(&windows, sequence_number)?;
        Some(windows.get(window_index).unwrap().clone())
    }

    fn get_or_create_window(&self, sequence_number: SequenceNumber) -> Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>> {
        let end = self.windows.read().unwrap_or_log().back().unwrap().end();
        if sequence_number >= end {
            self.create_windows_to(sequence_number);
        }
        self.try_get_window(sequence_number).unwrap()
    }

    fn create_windows_to(&self, sequence_number: SequenceNumber) {
        let windows = &mut *self.windows.write().unwrap_or_log();
        loop {
            let end = windows.back().unwrap().end();
            if sequence_number >= end {
                let shared_new_window = Arc::new(TimelineWindow::new(end));
                windows.push_back(shared_new_window.clone());
            } else {
                break;
            }
        }
    }

    fn window_count(&self) -> usize {
        self.windows.read().unwrap_or_log().len()
    }

    fn resolve_window(
        windows: &VecDeque<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>>,
        to_resolve: SequenceNumber,
    ) -> Option<usize> {
        let start = windows.front().unwrap().start();
        let end = windows.back().unwrap().end();
        if to_resolve >= start && to_resolve < end {
            let offset = to_resolve - start;
            Some(offset / TIMELINE_WINDOW_SIZE)
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct TimelineWindow<const SIZE: usize> {
    start: SequenceNumber,
    slot_status: [AtomicU8; SIZE],
    commit_records: [OnceLock<CommitRecord>; SIZE],
    readers: AtomicU64,
}

impl<const SIZE: usize> TimelineWindow<SIZE> {
    fn new(start: SequenceNumber) -> TimelineWindow<SIZE> {
        let commit_records = [const { OnceLock::new() }; SIZE];
        let slot_status = [const { AtomicU8::new(0) }; SIZE];
        debug_assert_eq!(slot_status[0].load(Ordering::SeqCst), SlotMarker::Empty.as_u8());

        TimelineWindow { start, slot_status, commit_records, readers: AtomicU64::new(0) }
    }

    fn start(&self) -> SequenceNumber {
        self.start
    }

    fn end(&self) -> SequenceNumber {
        self.start + TIMELINE_WINDOW_SIZE
    }

    fn insert_pending(&self, sequence_number: SequenceNumber, commit_record: CommitRecord) {
        let index = sequence_number - self.start;
        self.commit_records[index].set(commit_record).unwrap_or_log();
        self.slot_status[index].store(SlotMarker::Pending.as_u8(), Ordering::SeqCst);
    }

    fn set_validated(&self, sequence_number: SequenceNumber) {
        let index = sequence_number - self.start;
        self.slot_status[index].store(SlotMarker::Validated.as_u8(), Ordering::SeqCst);
    }

    fn set_aborted(&self, sequence_number: SequenceNumber) {
        let index = sequence_number - self.start;
        self.slot_status[index].store(SlotMarker::Aborted.as_u8(), Ordering::SeqCst);
    }

    fn set_applied(&self, sequence_number: SequenceNumber) {
        let index = sequence_number - self.start;
        self.slot_status[index].store(SlotMarker::Applied.as_u8(), Ordering::SeqCst);
    }

    fn get_status(&self, sequence_number: SequenceNumber) -> CommitStatus<'_> {
        let index = sequence_number - self.start;
        let status = SlotMarker::from(self.slot_status[index].load(Ordering::SeqCst));
        if let SlotMarker::Empty = status {
            CommitStatus::Empty
        } else {
            let record = self.commit_records[index].get().unwrap();
            match status {
                SlotMarker::Empty => unreachable!(),
                SlotMarker::Pending => CommitStatus::Pending(MaybeOwns::Borrowed(record)),
                SlotMarker::Validated => CommitStatus::Validated(MaybeOwns::Borrowed(record)),
                SlotMarker::Applied => CommitStatus::Applied(MaybeOwns::Borrowed(record)),
                SlotMarker::Aborted => CommitStatus::Aborted,
            }
        }
    }

    fn await_pending_status_commits(&self, sequence_number: SequenceNumber) -> bool {
        debug_assert!(!matches!(self.get_status(sequence_number), CommitStatus::Empty));
        loop {
            match self.get_status(sequence_number) {
                CommitStatus::Empty => unreachable!("Illegal state - commit status cannot move from pending to empty"),
                CommitStatus::Pending(_) => {
                    // TODO: we can improve the spin lock with async/await
                    // Note we only expect to have long waits in long chains of overlapping transactions that would conflict
                    // could also do a little sleep in the spin lock, for example if the validating is still far away
                    std::hint::spin_loop();
                }
                CommitStatus::Validated(_) | CommitStatus::Applied(_) => return true,
                CommitStatus::Aborted => return false,
            }
        }
    }

    fn get_readers(&self) -> u64 {
        self.readers.load(Ordering::Relaxed)
    }

    fn increment_readers(&self) {
        self.readers.fetch_add(1, Ordering::Relaxed);
    }

    fn decrement_readers(&self) -> u64 {
        self.readers.fetch_sub(1, Ordering::Relaxed) - 1 // Return the resulting number of readers
    }
}

#[derive(Debug)]
pub(crate) enum CommitStatus<'a> {
    Empty,
    Pending(MaybeOwns<'a, CommitRecord>),
    Validated(MaybeOwns<'a, CommitRecord>),
    Applied(MaybeOwns<'a, CommitRecord>),
    Aborted,
}

#[derive(Debug)]
enum SlotMarker {
    Empty,
    Pending,
    Validated,
    Applied,
    Aborted,
}

impl SlotMarker {
    const fn as_u8(&self) -> u8 {
        match self {
            SlotMarker::Empty => 0,
            SlotMarker::Pending => 1,
            SlotMarker::Validated => 2,
            SlotMarker::Applied => 3,
            SlotMarker::Aborted => 4,
        }
    }

    const fn from(value: u8) -> Self {
        match value {
            0 => SlotMarker::Empty,
            1 => SlotMarker::Pending,
            2 => SlotMarker::Validated,
            3 => SlotMarker::Applied,
            4 => SlotMarker::Aborted,
            _ => unreachable!(),
        }
    }
}

// TODO: move out of isolation manager
#[derive(Serialize, Deserialize)]
pub struct CommitRecord {
    // TODO: this could read-through to the WAL if we have to save memory?
    operations: OperationsBuffer,
    open_sequence_number: SequenceNumber,
    commit_type: CommitType,
}

impl fmt::Debug for CommitRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitRecord")
            .field("open_sequence_number", &self.open_sequence_number)
            .field("commit_type", &self.commit_type)
            .field("operations", &self.operations)
            .finish()
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub enum CommitType {
    Data,
    Schema,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StatusRecord {
    pub(crate) commit_record_sequence_number: SequenceNumber,
    pub(crate) was_committed: bool,
}

impl CommitRecord {
    pub(crate) fn new(
        operations: OperationsBuffer,
        open_sequence_number: SequenceNumber,
        commit_type: CommitType,
    ) -> CommitRecord {
        CommitRecord { operations, open_sequence_number, commit_type }
    }

    pub fn operations(&self) -> &OperationsBuffer {
        &self.operations
    }

    pub fn into_operations(self) -> OperationsBuffer {
        self.operations
    }

    pub fn commit_type(&self) -> CommitType {
        self.commit_type
    }

    pub fn open_sequence_number(&self) -> SequenceNumber {
        self.open_sequence_number
    }

    fn deserialise_from(record_type: DurabilityRecordType, reader: impl Read)
    where
        Self: Sized,
    {
        assert_eq!(Self::RECORD_TYPE, record_type);
        // TODO: handle error with a better message
        bincode::deserialize_from(reader).unwrap_or_log()
    }

    fn compute_dependency(&self, predecessor: &CommitRecord) -> CommitDependency {
        // TODO: this can be optimised by some kind of bit-wise AND of two bloom filter-like data
        // structures first, since we assume few clashes this should mostly succeed
        // TODO: can be optimised with an intersection of two sorted iterators instead of iterate + gets

        let mut puts_to_update = Vec::new();

        // we check self operations against predecessor operations.
        //   if our buffer contains a delete, we check the predecessor doesn't have an Existing lock on it
        // We check

        let locks = self.operations().locks();
        let predecessor_locks = predecessor.operations().locks();
        for (write_buffer, pred_write_buffer) in self.operations().write_buffers().zip(predecessor.operations()) {
            let writes = write_buffer.writes();
            let predecessor_writes = pred_write_buffer.writes();

            for (key, write) in writes.iter() {
                if let Some(predecessor_write) = predecessor_writes.get(key) {
                    match (predecessor_write, write) {
                        (Write::Insert { .. } | Write::Put { .. }, Write::Put { reinsert, .. }) => {
                            puts_to_update.push(DependentPut::Inserted { reinsert: reinsert.clone() });
                        }
                        (Write::Delete, Write::Put { reinsert, .. }) => {
                            puts_to_update.push(DependentPut::Deleted { reinsert: reinsert.clone() });
                        }
                        _ => (),
                    }
                }
                if matches!(write, Write::Delete) && matches!(predecessor_locks.get(key), Some(LockType::Unmodifiable))
                {
                    return CommitDependency::Conflict(IsolationConflict::DeletingRequiredKey);
                }
            }

            // TODO: this is ineffecient since we loop over all locks each time - should we locks into keyspaces?
            //    Investigate
            for (key, lock) in locks.iter() {
                if matches!(lock, LockType::Unmodifiable) {
                    if let Some(Write::Delete) = predecessor_writes.get(key) {
                        return CommitDependency::Conflict(IsolationConflict::RequireDeletedKey);
                    }
                }
            }
        }

        for (key, lock) in locks.iter() {
            if matches!(lock, LockType::Exclusive) && matches!(predecessor_locks.get(key), Some(LockType::Exclusive)) {
                return CommitDependency::Conflict(IsolationConflict::ExclusiveLock);
            }
        }

        if puts_to_update.is_empty() {
            CommitDependency::Independent
        } else {
            CommitDependency::DependentPuts { puts: puts_to_update }
        }
    }
}

impl DurabilityRecord for CommitRecord {
    const RECORD_TYPE: DurabilityRecordType = 0;

    const RECORD_NAME: &'static str = "commit_record";

    fn serialise_into(&self, writer: &mut impl std::io::Write) -> bincode::Result<()> {
        debug_assert_eq!(
            bincode::serialize(
                &bincode::deserialize::<CommitRecord>(bincode::serialize(&self).as_ref().unwrap()).unwrap()
            )
            .unwrap(),
            bincode::serialize(self).unwrap()
        );
        bincode::serialize_into(writer, &self)
    }

    fn deserialise_from(reader: &mut impl Read) -> bincode::Result<Self> {
        // https://github.com/bincode-org/bincode/issues/633
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        bincode::deserialize(&buf)
    }
}

impl SequencedDurabilityRecord for CommitRecord {}

impl StatusRecord {
    pub(crate) fn new(sequence_number: SequenceNumber, committed: bool) -> StatusRecord {
        StatusRecord { commit_record_sequence_number: sequence_number, was_committed: committed }
    }

    pub(crate) fn was_committed(&self) -> bool {
        self.was_committed
    }

    pub(crate) fn commit_record_sequence_number(&self) -> SequenceNumber {
        self.commit_record_sequence_number
    }
}

impl DurabilityRecord for StatusRecord {
    const RECORD_TYPE: DurabilityRecordType = 1;
    const RECORD_NAME: &'static str = "status_record";

    fn serialise_into(&self, writer: &mut impl std::io::Write) -> bincode::Result<()> {
        debug_assert_eq!(
            bincode::serialize(
                &bincode::deserialize::<StatusRecord>(bincode::serialize(&self).as_ref().unwrap()).unwrap()
            )
            .unwrap(),
            bincode::serialize(self).unwrap()
        );
        bincode::serialize_into(writer, &self)
    }

    fn deserialise_from(reader: &mut impl Read) -> bincode::Result<Self> {
        // https://github.com/bincode-org/bincode/issues/633
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        bincode::deserialize(&buf)
    }
}

impl UnsequencedDurabilityRecord for StatusRecord {}

#[cfg(test)]
mod tests {
    use std::{
        array,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        thread::{self, JoinHandle},
    };

    use crate::{
        isolation_manager::{CommitRecord, CommitStatus, CommitType, Timeline, TIMELINE_WINDOW_SIZE},
        keyspace::{KeyspaceId, KeyspaceSet},
        sequence_number::SequenceNumber,
        snapshot::buffer::OperationsBuffer,
    };

    macro_rules! test_keyspace_set {
        {$($variant:ident => $id:literal : $name: literal),* $(,)?} => {
            #[derive(Clone, Copy)]
            enum TestKeyspaceSet { $($variant),* }
            impl KeyspaceSet for TestKeyspaceSet {
                fn iter() -> impl Iterator<Item = Self> { [$(Self::$variant),*].into_iter() }
                fn id(&self) -> KeyspaceId {
                    match *self { $(Self::$variant => KeyspaceId($id)),* }
                }
                fn name(&self) -> &'static str {
                    match *self { $(Self::$variant => $name),* }
                }
            }
        };
    }

    test_keyspace_set! {
        Keyspace => 0: "keyspace",
    }

    struct MockTransaction {
        read_sequence_number: SequenceNumber,
        commit_sequence_number: SequenceNumber,
    }

    impl MockTransaction {
        fn new(read_sequence_number: SequenceNumber, commit_sequence_number: SequenceNumber) -> MockTransaction {
            MockTransaction { read_sequence_number, commit_sequence_number }
        }
    }

    fn create_timeline() -> Timeline {
        Timeline::new(SequenceNumber::MIN.next())
    }

    fn tx_open(timeline: &Timeline, read_sequence_number: SequenceNumber) {
        timeline.record_reader(read_sequence_number);
    }

    fn tx_close(timeline: &Timeline, read_sequence_number: SequenceNumber) {
        timeline.remove_reader(read_sequence_number);
    }

    fn tx_start_commit(timeline: &Timeline, tx: &MockTransaction) {
        let window = timeline.get_or_create_window(tx.commit_sequence_number);
        window.insert_pending(tx.commit_sequence_number, _record(tx.read_sequence_number));
    }

    fn tx_finalise_commit_status(timeline: &Timeline, tx: &MockTransaction, validation_result: bool) {
        let window = timeline.try_get_window(tx.commit_sequence_number).unwrap();
        if let CommitStatus::Pending(commit_record) = window.get_status(tx.commit_sequence_number) {
            if validation_result {
                window.set_validated(tx.commit_sequence_number);
                window.set_applied(tx.commit_sequence_number);
            } else {
                window.set_aborted(tx.commit_sequence_number);
            }
            let sequence_number = commit_record.open_sequence_number;
            drop(window);
            timeline.remove_reader(sequence_number);
            timeline.may_increment_watermark(tx.commit_sequence_number);
        } else {
            unreachable!()
        }
    }

    fn tx_complete_commit(timeline: &Timeline, tx: &MockTransaction) {
        tx_finalise_commit_status(timeline, tx, true);
    }

    fn tx_abort_commit(timeline: &Timeline, tx: &MockTransaction) {
        tx_finalise_commit_status(timeline, tx, false);
    }

    fn _seq(from: u64) -> SequenceNumber {
        SequenceNumber::from(from)
    }

    fn _record(read_sequence_number: SequenceNumber) -> CommitRecord {
        CommitRecord::new(OperationsBuffer::new(), read_sequence_number, CommitType::Data)
    }

    #[test]
    fn watermark_is_updated() {
        let timeline = &create_timeline();
        let tx1 = &MockTransaction::new(_seq(0), _seq(1));
        tx_open(timeline, tx1.read_sequence_number);
        tx_start_commit(timeline, tx1);
        tx_finalise_commit_status(timeline, tx1, true);
        assert_eq!(tx1.commit_sequence_number, timeline.watermark());

        let tx2 = &MockTransaction::new(_seq(0), _seq(2));

        tx_open(timeline, tx2.read_sequence_number);
        tx_start_commit(timeline, tx2);
        tx_finalise_commit_status(timeline, tx2, false);
        assert_eq!(tx2.commit_sequence_number, timeline.watermark());

        let tx3 = &MockTransaction::new(_seq(0), _seq(3));
        let tx4 = &MockTransaction::new(_seq(0), _seq(4));
        tx_open(timeline, tx3.read_sequence_number);
        tx_open(timeline, tx4.read_sequence_number);
        tx_start_commit(timeline, tx3);
        tx_start_commit(timeline, tx4);
        tx_finalise_commit_status(timeline, tx4, true);
        assert_eq!(tx2.commit_sequence_number, timeline.watermark()); // tx3 is not yet committed, watermark does not move.
        tx_finalise_commit_status(timeline, tx3, true);
        assert_eq!(tx4.commit_sequence_number, timeline.watermark()); // Watermark goes up all the way to 4.
    }

    #[test]
    fn unused_windows_are_cleaned_up() {
        let timeline = &create_timeline();

        let tx_count = TIMELINE_WINDOW_SIZE + 2;
        for i in 1..tx_count {
            //
            let tx = &MockTransaction::new(_seq(0), _seq(i as u64));
            tx_open(timeline, tx.read_sequence_number);
            tx_start_commit(timeline, tx);
        }

        let stop_at = tx_count - 2;
        for i in 1..stop_at {
            let tx = &MockTransaction::new(_seq(0), _seq(i as u64));
            tx_finalise_commit_status(timeline, tx, true);
        }
        assert!(timeline.try_get_window(_seq(1)).is_some());
        for i in stop_at..tx_count {
            let tx = &MockTransaction::new(_seq(0), _seq(i as u64));
            tx_finalise_commit_status(timeline, tx, true);
        }
        assert!(timeline.try_get_window(_seq(1)).is_none());
    }

    #[test]
    fn watermark_keeps_window_pinned() {
        let timeline = create_timeline();
        let tx1 = &MockTransaction::new(_seq(0), _seq(1));
        tx_open(&timeline, tx1.read_sequence_number);
        tx_start_commit(&timeline, tx1);
        tx_finalise_commit_status(&timeline, tx1, true);

        let got_window = timeline.try_get_window(tx1.commit_sequence_number);
        assert!(got_window.is_some());

        let mut i = tx1.commit_sequence_number + 1;
        while timeline.try_get_window(i).is_some() {
            let tx = &MockTransaction::new(_seq(0), i);
            tx_open(&timeline, tx.read_sequence_number);
            tx_start_commit(&timeline, tx);
            tx_finalise_commit_status(&timeline, tx, true);
            i += 1;
        }

        match timeline.try_get_window(timeline.watermark()) {
            Some(window) => assert_eq!(0, window.get_readers()),
            None => panic!(),
        };
    }

    #[test]
    fn test_highly_concurrent_correctness() {
        let timeline_and_counter = Arc::new((create_timeline(), AtomicU64::new(1)));
        const NUM_THREADS: usize = 32;
        const TRANSACTIONS_PER_THREAD: u64 = 1000;

        let join_handles: [JoinHandle<()>; NUM_THREADS] = array::from_fn(|_| {
            let timeline_and_counter = timeline_and_counter.clone();
            thread::spawn(move || {
                for _ in 0..TRANSACTIONS_PER_THREAD {
                    let (timeline, commit_sequence_number_counter) = &*timeline_and_counter;
                    let index = commit_sequence_number_counter.fetch_add(1, Ordering::SeqCst);
                    let tx = &MockTransaction::new(timeline.watermark(), _seq(index));
                    tx_open(timeline, tx.read_sequence_number);
                    tx_start_commit(timeline, tx);
                    tx_finalise_commit_status(timeline, tx, true);
                }
            })
        });

        for join_handle in join_handles {
            join_handle.join().unwrap()
        }

        let expected_watermark = _seq(NUM_THREADS as u64 * TRANSACTIONS_PER_THREAD);
        let (timeline, _) = &*timeline_and_counter;
        assert_eq!(expected_watermark, timeline.watermark());
        let some_index_in_penultimate_window = expected_watermark - TIMELINE_WINDOW_SIZE - 1;
        assert!(timeline.try_get_window(some_index_in_penultimate_window).is_none());
    }
}
