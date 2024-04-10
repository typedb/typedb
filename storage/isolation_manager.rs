/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// TODO: Check atomic Ordering constraints. We're using SeqCst where we don't have to
// TODO: Benchmark with many small commits to see if the read-write locks affect latency.

use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    error::Error,
    fmt,
    io::Read,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, Ordering},
        Arc, OnceLock, RwLock,
    },
};

use durability::{
    DurabilityRecord, DurabilityRecordType, DurabilityService, SequenceNumber, SequencedDurabilityRecord,
    UnsequencedDurabilityRecord,
};
use itertools::Itertools;
use logger::result::ResultExt;
use resource::constants::storage::TIMELINE_WINDOW_SIZE;
use serde::{Deserialize, Serialize};

use crate::snapshot::{buffer::KeyspaceBuffers, write::Write};

#[derive(Debug)]
pub(crate) struct IsolationManager {
    timeline: Timeline,
}

impl fmt::Display for IsolationManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Timeline[windows={}, watermark={}]", self.timeline.window_count(), self.watermark())
    }
}

impl IsolationManager {
    pub(crate) fn new(next_relative_index: i64) -> IsolationManager {
        IsolationManager { timeline: Timeline::new(next_relative_index) }
    }

    pub(crate) fn opened_for_read(&self, open_sequence_number: SequenceNumber) {
        debug_assert!(open_sequence_number <= self.watermark());
        self.timeline.record_reader(open_sequence_number);
    }

    pub(crate) fn closed_for_read(&self, open_sequence_number: SequenceNumber) {
        self.timeline.remove_reader(open_sequence_number);
    }

    pub(crate) fn applied(&self, relative_index: i64) {
        let (window, slot_index) = self.timeline.try_get_window(relative_index).unwrap();
        window.set_applied(slot_index);
        self.timeline.may_increment_watermark(relative_index);
    }

    pub(crate) fn load_applied(
        &self,
        relative_index: i64,
        sequence_number: SequenceNumber,
        commit_record: CommitRecord,
    ) {
        let (window, slot_index) = self.timeline.get_or_create_window(relative_index);
        window.insert_pending(slot_index, sequence_number, commit_record);
        window.set_applied(slot_index);
        self.timeline.may_increment_watermark(relative_index);
    }

    pub(crate) fn load_aborted(&self, relative_index: i64, sequence_number: SequenceNumber) {
        let (window, slot_index) = self.timeline.get_or_create_window(relative_index);
        window.insert_pending(slot_index, sequence_number, CommitRecord::new(KeyspaceBuffers::new(), sequence_number));
        window.set_aborted(slot_index);
        self.timeline.may_increment_watermark(relative_index);
    }

    pub(crate) fn try_commit<D>(
        &self,
        relative_index: i64,
        commit_sequence_number: SequenceNumber,
        commit_record: CommitRecord,
        durability_service: &D,
    ) -> Result<(), IsolationError>
    where
        D: DurabilityService,
    {
        let (window, slot_index) = self.timeline.get_or_create_window(relative_index);
        window.insert_pending(slot_index, commit_sequence_number.clone(), commit_record);
        if let CommitStatus::Pending(_, commit_record) = window.get_status(slot_index) {
            let validation_result = self.validate_all_concurrent(relative_index, &commit_record, durability_service);
            if validation_result.is_ok() {
                window.set_validated(slot_index);
                // We can't increment watermark here till the status is "applied"
            } else {
                window.set_aborted(slot_index);
                self.timeline.may_increment_watermark(relative_index);
            }
            self.timeline.remove_reader(commit_record.open_sequence_number);
            validation_result
        } else {
            unreachable!()
        }
    }

    fn validate_all_concurrent<D>(
        &self,
        commit_relative_index: i64,
        commit_record: &CommitRecord,
        durability_service: &D,
    ) -> Result<(), IsolationError>
    where
        D: DurabilityService,
    {
        // TODO: decide if we should block until all predecessors finish, allow out of order (non-Calvin model/traditional model)
        //       We could also validate against all predecessors even if they are validating and fail eagerly.
        // TODO: Should we validate from the timeline before going to disk?
        // Pre-collect all the ARCs so we can validate against them.
        let (windows, first_window_relative_index): (Vec<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>>, i64) =
            self.timeline.collect_concurrent_windows(commit_record.open_sequence_number, commit_relative_index);
        let first_sequence_number_in_windows = *windows.get(0).unwrap().starting_sequence_number().unwrap();
        if commit_record.open_sequence_number().next() <= first_sequence_number_in_windows {
            self.validate_concurrent_from_disk(commit_record, first_sequence_number_in_windows, durability_service)?;
        }

        self.validate_concurrent_from_windows(
            commit_record,
            commit_relative_index,
            &windows,
            first_window_relative_index,
        )
    }

    fn validate_concurrent_from_disk<D>(
        &self,
        commit_record: &CommitRecord,
        stop_sequence_number: SequenceNumber,
        durability_service: &D,
    ) -> Result<(), IsolationError>
    where
        D: DurabilityService,
    {
        for commit_status_result in Self::iterate_commit_status_from_disk(
            durability_service,
            commit_record.open_sequence_number.next(),
            stop_sequence_number,
        )
        .unwrap()
        {
            if let Ok(commit_status) = commit_status_result {
                let commit_dependency = match commit_status {
                    CommitStatus::Applied(_, predecessor_record) => {
                        commit_record.compute_dependency(&predecessor_record)
                    }
                    CommitStatus::Aborted(_) => CommitDependency::Independent,
                    CommitStatus::Pending(_, _) => {
                        unreachable!("Evicted records cannot be pending")
                    }
                    CommitStatus::Empty | CommitStatus::Validated(_, _) => unreachable!(),
                };
                handle_dependency(commit_dependency)?;
            } else {
                todo!()
            }
        }
        Ok(())
    }

    fn validate_concurrent_from_windows(
        &self,
        commit_record: &CommitRecord,
        commit_relative_index: i64,
        windows: &Vec<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>>,
        first_window_relative_index: i64,
    ) -> Result<(), IsolationError> {
        let mut window_index = 0;
        let mut slot_index = (0..TIMELINE_WINDOW_SIZE)
            .find(|si| match windows[0].get_sequence_number(*si) {
                Some(seq) => seq > commit_record.open_sequence_number,
                None => false,
            })
            .unwrap();
        let start_validation_index =
            first_window_relative_index + (window_index * TIMELINE_WINDOW_SIZE + slot_index) as i64;
        for _at in start_validation_index..commit_relative_index {
            debug_assert!(window_index < windows.len());
            resolve_concurrent(commit_record, slot_index, &windows[window_index])?;
            slot_index += 1;
            if slot_index >= TIMELINE_WINDOW_SIZE {
                window_index += 1;
                slot_index = 0;
            }
        }
        debug_assert_eq!(
            first_window_relative_index + (window_index * TIMELINE_WINDOW_SIZE + slot_index) as i64,
            commit_relative_index
        );
        Ok(())
    }

    pub(crate) fn iterate_commit_status_from_disk<'a, D>(
        durability_service: &'a D,
        start_sequence_number: SequenceNumber,
        stop_sequence_number: SequenceNumber,
    ) -> durability::Result<impl Iterator<Item = durability::Result<CommitStatus<'a>>>>
    where
        D: DurabilityService,
    {
        let mut is_committed: HashMap<SequenceNumber, bool> = HashMap::new();
        for record in
            durability_service.iter_unsequenced_type_from::<StatusRecord>(start_sequence_number.clone()).unwrap()
        {
            if let Ok(predecessor_record) = record {
                // We can't stop early because status records may be out-of-order
                is_committed
                    .insert(predecessor_record.commit_record_sequence_number(), predecessor_record.was_committed());
            } else {
                todo!()
            }
        }

        Ok(durability_service.iter_sequenced_type_from::<CommitRecord>(start_sequence_number.clone())?.map_while(
            move |result: durability::Result<(SequenceNumber, CommitRecord)>| match result {
                Ok((commit_sequence_number, commit_record)) => {
                    if commit_sequence_number >= stop_sequence_number {
                        None
                    } else {
                        Some(Ok(match is_committed.get(&commit_sequence_number) {
                            None => CommitStatus::Pending(commit_sequence_number, Cow::Owned(commit_record)),
                            Some(true) => CommitStatus::Applied(commit_sequence_number, Cow::Owned(commit_record)),
                            Some(false) => CommitStatus::Aborted(commit_sequence_number),
                        }))
                    }
                }
                Err(err) => Some(Err(err)),
            },
        ))
    }

    pub(crate) fn watermark(&self) -> SequenceNumber {
        let (window, slot_index) = self.timeline.try_get_window(self.timeline.watermark()).unwrap();
        match window.get_status(slot_index) {
            CommitStatus::Applied(seq, _) | CommitStatus::Aborted(seq) => seq.clone(),
            CommitStatus::Validated(_, _) | CommitStatus::Pending(_, _) | CommitStatus::Empty =>
                unreachable!("The isolation manager must always have the watermark commit record in the window, and it must be either committed or aborted."),
        }
    }

    pub(crate) fn apply_to_commit_record<F, T>(&self, relative_index: i64, function: F) -> T
    where
        F: FnOnce(&CommitRecord) -> T,
    {
        let (window, slot_index) = self.timeline.try_get_window(relative_index).unwrap();
        let record = match window.get_status(slot_index) {
            CommitStatus::Validated(_, commit_record) | CommitStatus::Applied(_, commit_record) => commit_record,
            _ => panic!("apply_to_commit_record called on uncommitted record"), // TODO: Do we want to be able to apply on pending?
        };
        function(&record)
    }
}

fn resolve_concurrent(
    commit_record: &CommitRecord,
    predecessor_slot_index: usize,
    predecessor_window: &TimelineWindow<TIMELINE_WINDOW_SIZE>,
) -> Result<(), IsolationError> {
    let commit_dependency = match predecessor_window.get_status(predecessor_slot_index) {
        CommitStatus::Empty => unreachable!("A concurrent status should never be empty at commit time"),
        CommitStatus::Pending(_, predecessor_record) => match commit_record.compute_dependency(&predecessor_record) {
            CommitDependency::Independent => CommitDependency::Independent,
            result => {
                if predecessor_window.await_pending_status_commits(predecessor_slot_index) {
                    result
                } else {
                    CommitDependency::Independent
                }
            }
        },
        CommitStatus::Validated(_, predecessor_record) | CommitStatus::Applied(_, predecessor_record) => {
            commit_record.compute_dependency(&predecessor_record)
        }
        CommitStatus::Aborted(_) => CommitDependency::Independent,
    };
    handle_dependency(commit_dependency)
}

fn handle_dependency(commit_dependency: CommitDependency) -> Result<(), IsolationError> {
    match commit_dependency {
        CommitDependency::Independent => (),
        CommitDependency::DependentPuts { puts } => puts.into_iter().for_each(DependentPut::apply),
        CommitDependency::Conflict(conflict) => return Err(IsolationError::Conflict(conflict)),
    }
    Ok(())
}

#[derive(Debug)]
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

#[derive(Debug)]
pub enum IsolationConflict {
    DeleteRequired,
    RequiredDelete,
}

#[derive(Debug)]
pub enum IsolationError {
    Conflict(IsolationConflict),
}

impl fmt::Display for IsolationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict(IsolationConflict::DeleteRequired) => {
                write!(f, "Isolation violation: Delete-Require conflict. A preceding concurrent commit has deleted a key required by this transaction. Please retry.")
            }
            Self::Conflict(IsolationConflict::RequiredDelete) => {
                write!(f, "Isolation violation: Require-Delete conflict. This commit has deleted a key required by a preceding concurrent transaction. Please retry.")
            }
        }
    }
}

impl Error for IsolationError {}

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
    next_relative_index_and_windows: RwLock<(i64, VecDeque<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>>)>,
    watermark: AtomicI64,
}

impl Timeline {
    fn new(next_relative_index: i64) -> Timeline {
        let windows = VecDeque::new();
        Timeline {
            next_relative_index_and_windows: RwLock::new((next_relative_index, windows)),
            watermark: AtomicI64::new(next_relative_index - 1),
        }
    }

    fn may_free_windows(&self) {
        let watermark = self.watermark();
        let can_free_some: bool = {
            let (next_relative_index, windows) = &*self.next_relative_index_and_windows.read().unwrap_or_log();
            let start_of_second_window = *next_relative_index - ((windows.len() - 1) * TIMELINE_WINDOW_SIZE) as i64;
            match windows.front() {
                None => false,
                Some(front) => front.get_readers() == 0 && watermark >= start_of_second_window,
            }
        };
        if can_free_some {
            let (next_relative_index, windows) = &mut *self.next_relative_index_and_windows.write().unwrap_or_log();
            let mut end_of_first_remaining_window =
                *next_relative_index - ((windows.len() - 1) * TIMELINE_WINDOW_SIZE) as i64;
            while watermark >= end_of_first_remaining_window
                && windows.front().is_some()
                && windows.front().unwrap().get_readers() == 0
            {
                windows.pop_front();
                end_of_first_remaining_window += TIMELINE_WINDOW_SIZE as i64;
            }
        }
    }

    fn may_increment_watermark(&self, relative_index: i64) {
        let mut watermark = self.watermark.load(Ordering::SeqCst);
        if watermark != relative_index - 1 {
            return ();
        }
        let (mut window, mut candidate_slot_index) = self.try_get_window(relative_index).unwrap();
        let end_of_window: i64 = relative_index + (TIMELINE_WINDOW_SIZE - candidate_slot_index) as i64;
        let mut candidate_watermark = relative_index;
        loop {
            let should_update: bool = match window.get_status(candidate_slot_index) {
                CommitStatus::Empty | CommitStatus::Pending(_, _) | CommitStatus::Validated(_, _) => false,
                CommitStatus::Applied(_, _) => true,
                CommitStatus::Aborted(_) => true,
            };
            if should_update
                && self
                    .watermark
                    .compare_exchange(candidate_watermark - 1, candidate_watermark, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                candidate_watermark += 1;
                candidate_slot_index += 1;
                if candidate_slot_index >= TIMELINE_WINDOW_SIZE {
                    if let Some(res) = self.try_get_window(candidate_watermark) {
                        (window, candidate_slot_index) = res;
                        debug_assert_eq!(0, candidate_slot_index);
                    } else {
                        break;
                    }
                }
            } else {
                break;
            }
        }
        if watermark >= end_of_window {
            self.may_free_windows();
        }
    }

    fn watermark(&self) -> i64 {
        self.watermark.load(Ordering::SeqCst)
    }

    fn record_reader(&self, sequence_number: SequenceNumber) {
        let (_, windows) = &*self.next_relative_index_and_windows.read().unwrap_or_log();
        if let Some((window, _)) = self.find_window(windows, sequence_number) {
            window.increment_readers();
        };
    }

    fn remove_reader(&self, reader_sequence_number: SequenceNumber) {
        let found_window_opt = {
            let (_, windows) = &*self.next_relative_index_and_windows.read().unwrap_or_log();
            self.find_window(windows, reader_sequence_number)
        };
        if let Some((window, _)) = found_window_opt {
            if window.decrement_readers() == 0 {
                self.may_free_windows();
            }
        }
    }

    fn find_window(
        &self,
        windows: &VecDeque<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>>,
        sequence_number: SequenceNumber,
    ) -> Option<(Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>, usize)> {
        windows.iter().enumerate().rev().find_map(|(window_index, window)| {
            let start_seq = window.starting_sequence_number();
            if start_seq.is_some() && *start_seq.unwrap() <= sequence_number {
                Some((window.clone(), window_index))
            } else {
                None
            }
        })
    }

    fn collect_concurrent_windows(
        &self,
        open_sequence_number: SequenceNumber,
        commit_relative_index: i64,
    ) -> (Vec<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>>, i64) {
        let (next_relative_index, windows) = &*self.next_relative_index_and_windows.read().unwrap_or_log();
        let first_concurrent_window_index = self
            .find_window(windows, open_sequence_number.next())
            .map(|(window, window_index)| window_index)
            .unwrap_or(0);

        let relative_index_of_window_0 = *next_relative_index - (windows.len() * TIMELINE_WINDOW_SIZE) as i64;
        let last_concurrent_window_index =
            (commit_relative_index - relative_index_of_window_0) as usize / TIMELINE_WINDOW_SIZE;
        let mut concurrent_windows: Vec<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>> = Vec::new();
        (first_concurrent_window_index..(last_concurrent_window_index + 1)).for_each(|window_index| {
            concurrent_windows.push(windows.get(window_index).unwrap().clone());
        });
        let start_index_of_first_concurrent_window =
            relative_index_of_window_0 + (first_concurrent_window_index * TIMELINE_WINDOW_SIZE) as i64;
        (concurrent_windows, start_index_of_first_concurrent_window)
    }

    fn try_get_window(&self, relative_index: i64) -> Option<(Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>, usize)> {
        let (next_relative_index, windows) = &*self.next_relative_index_and_windows.read().unwrap_or_log();
        let relative_index_of_window_0: i64 = next_relative_index - (windows.len() * TIMELINE_WINDOW_SIZE) as i64;

        if relative_index >= relative_index_of_window_0 && relative_index < *next_relative_index {
            let window_index = (relative_index - relative_index_of_window_0) as usize / TIMELINE_WINDOW_SIZE;
            let window = windows.get(window_index)?;
            let slot_index: usize = (relative_index - relative_index_of_window_0) as usize % TIMELINE_WINDOW_SIZE;
            Some((window.clone(), slot_index))
        } else {
            None
        }
    }

    fn get_or_create_window(&self, relative_index: i64) -> (Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>, usize) {
        let next_relative_index: i64 = { self.next_relative_index_and_windows.read().unwrap_or_log().0 };
        if relative_index >= next_relative_index {
            self.create_windows_to(relative_index);
        }
        self.try_get_window(relative_index).unwrap()
    }

    fn create_windows_to(&self, relative_index: i64) {
        let (next_relative_index, windows) = &mut *self.next_relative_index_and_windows.write().unwrap_or_log();
        while relative_index >= *next_relative_index {
            let shared_new_window = Arc::new(TimelineWindow::new());
            *next_relative_index += TIMELINE_WINDOW_SIZE as i64;
            windows.push_back(shared_new_window.clone());
        }
    }

    fn window_count(&self) -> usize {
        let (_, windows) = &*self.next_relative_index_and_windows.read().unwrap_or_log();
        windows.len()
    }
}

#[derive(Debug)]
struct TimelineWindow<const SIZE: usize> {
    slot_status: [AtomicU8; SIZE],
    commit_records: [OnceLock<(SequenceNumber, CommitRecord)>; SIZE],
    readers: AtomicU64,
}

impl<const SIZE: usize> TimelineWindow<SIZE> {
    fn new() -> TimelineWindow<SIZE> {
        const EMPTY: OnceLock<(SequenceNumber, CommitRecord)> = OnceLock::new();
        let commit_records = [EMPTY; SIZE];
        let slot_status: [AtomicU8; SIZE] = core::array::from_fn(|_| AtomicU8::new(0));
        debug_assert_eq!(slot_status[0].load(Ordering::SeqCst), SlotMarker::Empty.as_u8());

        TimelineWindow { slot_status, commit_records, readers: AtomicU64::new(0) }
    }

    /// Sequence number start of window (inclusive)
    fn starting_sequence_number(&self) -> Option<&SequenceNumber> {
        Some(&self.commit_records[0].get()?.0)
    }

    fn insert_pending(&self, index: usize, sequence_number: SequenceNumber, commit_record: CommitRecord) {
        self.commit_records[index].set((sequence_number, commit_record)).unwrap_or_log();
        self.slot_status[index].store(SlotMarker::Pending.as_u8(), Ordering::SeqCst);
    }

    fn set_validated(&self, index: usize) {
        self.slot_status[index].store(SlotMarker::Validated.as_u8(), Ordering::SeqCst);
    }

    fn set_aborted(&self, index: usize) {
        self.slot_status[index].store(SlotMarker::Aborted.as_u8(), Ordering::SeqCst);
    }

    fn set_applied(&self, index: usize) {
        self.slot_status[index].store(SlotMarker::Applied.as_u8(), Ordering::SeqCst);
    }

    fn get_sequence_number(&self, index: usize) -> Option<SequenceNumber> {
        Some(self.commit_records[index].get()?.0)
    }

    fn get_status(&self, index: usize) -> CommitStatus<'_> {
        let status = SlotMarker::from(self.slot_status[index].load(Ordering::SeqCst));
        if let SlotMarker::Empty = status {
            CommitStatus::Empty
        } else {
            let (seq, record) = self.commit_records[index].get().unwrap();
            match status {
                SlotMarker::Empty => unreachable!(),
                SlotMarker::Pending => CommitStatus::Pending(seq.clone(), Cow::Borrowed(record)),
                SlotMarker::Validated => CommitStatus::Validated(seq.clone(), Cow::Borrowed(record)),
                SlotMarker::Applied => CommitStatus::Applied(seq.clone(), Cow::Borrowed(record)),
                SlotMarker::Aborted => CommitStatus::Aborted(seq.clone()),
            }
        }
    }

    fn await_pending_status_commits(&self, index: usize) -> bool {
        debug_assert!(!matches!(self.get_status(index), CommitStatus::Empty));
        loop {
            match self.get_status(index) {
                CommitStatus::Empty => unreachable!("Illegal state - commit status cannot move from pending to empty"),
                CommitStatus::Pending(_, _) => {
                    // TODO: we can improve the spin lock with async/await
                    // Note we only expect to have long waits in long chains of overlapping transactions that would conflict
                    // could also do a little sleep in the spin lock, for example if the validating is still far away
                    std::hint::spin_loop();
                }
                CommitStatus::Validated(_, _) | CommitStatus::Applied(_, _) => return true,
                CommitStatus::Aborted(_) => return false,
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
    Pending(SequenceNumber, Cow<'a, CommitRecord>),
    Validated(SequenceNumber, Cow<'a, CommitRecord>),
    Applied(SequenceNumber, Cow<'a, CommitRecord>),
    Aborted(SequenceNumber),
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

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct CommitRecord {
    // TODO: this could read-through to the WAL if we have to save memory?
    buffers: KeyspaceBuffers,
    open_sequence_number: SequenceNumber,
}

impl Clone for CommitRecord {
    fn clone(&self) -> Self {
        unimplemented!("Do not call into_owned on a commit that's owned by the timeline.")
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct StatusRecord {
    commit_record_sequence_number: SequenceNumber,
    was_committed: bool,
}

impl CommitRecord {
    pub(crate) fn new(buffers: KeyspaceBuffers, open_sequence_number: SequenceNumber) -> CommitRecord {
        CommitRecord { buffers, open_sequence_number }
    }

    pub(crate) fn buffers(&self) -> &KeyspaceBuffers {
        &self.buffers
    }

    pub(crate) fn open_sequence_number(&self) -> SequenceNumber {
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
        // TODO: this can be optimised by some kind of bit-wise NAND of two bloom filter-like data
        // structures first, since we assume few clashes this should mostly succeed
        // TODO: can be optimised with an intersection of two sorted iterators instead of iterate + gets

        let mut puts_to_update = Vec::new();

        for (buffer, predecessor_buffer) in self.buffers().iter().zip(predecessor.buffers()) {
            let map = buffer.map().read().unwrap();
            if map.is_empty() {
                continue;
            }

            let predecessor_map = predecessor_buffer.map().read().unwrap();
            if predecessor_map.is_empty() {
                continue;
            }

            for (key, write) in map.iter() {
                if let Some(predecessor_write) = predecessor_map.get(key.bytes()) {
                    match (predecessor_write, write) {
                        (Write::Delete, Write::RequireExists { .. }) => {
                            return CommitDependency::Conflict(IsolationConflict::DeleteRequired);
                        }
                        (Write::RequireExists { .. }, Write::Delete) => {
                            // we escalate required-delete to failure, since requires implies dependencies that may be broken
                            // TODO: maybe RequireExists should be RequireDependency to capture this?
                            return CommitDependency::Conflict(IsolationConflict::RequiredDelete);
                        }
                        (Write::Insert { .. } | Write::Put { .. }, Write::Put { reinsert, .. }) => {
                            puts_to_update.push(DependentPut::Inserted { reinsert: reinsert.clone() });
                        }
                        (Write::Delete, Write::Put { reinsert, .. }) => {
                            puts_to_update.push(DependentPut::Deleted { reinsert: reinsert.clone() });
                        }
                        _ => (),
                    }
                }
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

    fn deserialise_from(record_type: DurabilityRecordType, reader: impl Read)
    where
        Self: Sized,
    {
        assert_eq!(Self::RECORD_TYPE, record_type);
        // TODO: handle error with a better message
        bincode::deserialize_from(reader).unwrap_or_log()
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, RwLock};
    use std::sync::mpsc::Receiver;
    use std::thread;
    use std::time::Duration;
    use crate::{KeyspaceSet, KeyspaceId};

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
    use durability::SequenceNumber;

    use crate::{
        isolation_manager::{CommitRecord, CommitStatus, Timeline, TIMELINE_WINDOW_SIZE},
        snapshot::buffer::KeyspaceBuffers,
    };

    struct MockTransaction {
        read_sequence_number: SequenceNumber,
        relative_index: i64,
        commit_sequence_number: SequenceNumber,
    }

    impl MockTransaction {
        fn new(
            read_sequence_number: SequenceNumber,
            relative_index: i64,
            commit_sequence_number: SequenceNumber,
        ) -> MockTransaction {
            MockTransaction { read_sequence_number, relative_index, commit_sequence_number }
        }
    }

    fn create_timeline() -> Timeline {
        let timeline = Timeline::new(0);
        let (window, slot_index) = timeline.get_or_create_window(0);
        window.insert_pending(
            slot_index,
            SequenceNumber::MIN,
            CommitRecord::new(KeyspaceBuffers::new(), SequenceNumber::MIN),
        );
        window.set_aborted(slot_index);
        timeline.may_increment_watermark(0);
        timeline
    }

    fn tx_open(timeline: &Timeline, read_sequence_number: SequenceNumber) {
        timeline.record_reader(read_sequence_number);
    }

    fn tx_close(timeline: &Timeline, read_sequence_number: SequenceNumber) {
        timeline.remove_reader(read_sequence_number);
    }

    fn tx_start_commit(timeline: &Timeline, tx: &MockTransaction) {
        let (window, slot_index) = timeline.get_or_create_window(tx.relative_index);
        window.insert_pending(slot_index, tx.commit_sequence_number.clone(), _record(tx.read_sequence_number));
    }

    fn tx_finalise_commit_status(timeline: &Timeline, tx: &MockTransaction, validation_result: bool) {
        let (window, slot_index) = timeline.try_get_window(tx.relative_index).unwrap();
        if let CommitStatus::Pending(_, commit_record) = window.get_status(slot_index) {
            if validation_result {
                window.set_validated(slot_index);
                window.set_applied(slot_index);
            } else {
                window.set_aborted(slot_index);
            }
            timeline.remove_reader(commit_record.open_sequence_number);
            timeline.may_increment_watermark(tx.relative_index);
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

    fn _seq(from: u128) -> SequenceNumber {
        SequenceNumber::from(from)
    }

    fn _record(read_sequence_number: SequenceNumber) -> CommitRecord {
        CommitRecord::new(KeyspaceBuffers::new(), read_sequence_number)
    }

    #[test]
    fn watermark_is_updated() {
        let timeline = &create_timeline();
        let tx1 = &MockTransaction::new(_seq(0), 1, _seq(1));
        tx_open(timeline, tx1.read_sequence_number);
        tx_start_commit(timeline, tx1);
        tx_finalise_commit_status(timeline, tx1, true);
        assert_eq!(tx1.relative_index, timeline.watermark());

        let tx2 = &MockTransaction::new(_seq(0), 2, _seq(2));

        tx_open(timeline, tx2.read_sequence_number);
        tx_start_commit(timeline, tx2);
        tx_finalise_commit_status(timeline, tx2, false);
        assert_eq!(tx2.relative_index, timeline.watermark());

        let tx3 = &MockTransaction::new(_seq(0), 3, _seq(3));
        let tx4 = &MockTransaction::new(_seq(0), 4, _seq(4));
        tx_open(timeline, tx3.read_sequence_number);
        tx_open(timeline, tx4.read_sequence_number);
        tx_start_commit(timeline, tx3);
        tx_start_commit(timeline, tx4);
        tx_finalise_commit_status(timeline, tx4, true);
        assert_eq!(tx2.relative_index, timeline.watermark()); // tx3 is not yet committed, watermark does not move.
        tx_finalise_commit_status(timeline, tx3, true);
        assert_eq!(tx4.relative_index, timeline.watermark()); // Watermark goes up all the way to 4.
    }

    #[test]
    fn unused_windows_are_cleaned_up() {
        let timeline = &create_timeline();

        let tx_count = TIMELINE_WINDOW_SIZE + 2;
        for i in 1..tx_count {
            //
            let tx = &MockTransaction::new(_seq(0), i as i64, _seq(i as u128));
            tx_open(timeline, tx.read_sequence_number);
            tx_start_commit(timeline, tx);
        }

        let stop_at = tx_count - 2;
        for i in 1..stop_at {
            let tx = &MockTransaction::new(_seq(0), i as i64, _seq(i as u128));
            tx_finalise_commit_status(timeline, tx, true);
        }
        assert!(timeline.try_get_window(1).is_some());
        for i in stop_at..tx_count {
            let tx = &MockTransaction::new(_seq(0), i as i64, _seq(i as u128));
            tx_finalise_commit_status(timeline, tx, true);
        }
        assert!(timeline.try_get_window(1).is_none());
    }

    #[test]
    fn watermark_keeps_window_pinned() {
        let timeline = &create_timeline();
        let tx1 = &MockTransaction::new(_seq(0), 1, _seq(1));
        tx_open(timeline, tx1.read_sequence_number);
        tx_start_commit(timeline, tx1);
        tx_finalise_commit_status(timeline, tx1, true);

        let got_window = timeline.try_get_window(tx1.relative_index);
        assert!(got_window.is_some());

        let mut i = tx1.relative_index + 1;
        while timeline.try_get_window(i).is_some() {
            let tx = &MockTransaction::new(_seq(0), i, _seq(i as u128));
            tx_open(timeline, tx.read_sequence_number);
            tx_start_commit(timeline, tx);
            tx_finalise_commit_status(timeline, tx, true);
            i += 1;
        }

        match timeline.try_get_window(timeline.watermark()) {
            Some((window, slot_index)) => {
                debug_assert_eq!(TIMELINE_WINDOW_SIZE - 1, slot_index); // If this fails, the test is wrong.
                assert_eq!(0, window.get_readers());
            }
            None => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_highly_concurrent_correctness() {
        let main_timeline_and_counter = Arc::new((create_timeline(), AtomicU64::new(1)));
        let nthreads = 32;
        let main_ntransactions_per_thread = 1000;
        let mut receivers: Vec<Receiver<()>> = Vec::new();
        for _ti in 0..nthreads {
            let ntransactions_per_thread = main_ntransactions_per_thread;
            let timeline_and_counter = main_timeline_and_counter.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            receivers.push(rx);
            thread::spawn(move || {
                for i in 0..ntransactions_per_thread {
                    let (timeline,commit_sequence_number_counter) = &*timeline_and_counter;
                    let index = commit_sequence_number_counter.fetch_add(1, Ordering::SeqCst) as i64;
                    let tx = &MockTransaction::new(_seq(timeline.watermark() as u128), index, _seq(index as u128));
                    tx_open(timeline, tx.read_sequence_number);
                    tx_start_commit(timeline, tx);
                    tx_finalise_commit_status(timeline, tx, true);
                }
                tx.send(()).unwrap();
            });
        }

        receivers.iter().for_each(|rx| { rx.recv().unwrap() });
        let expected_commits = nthreads * main_ntransactions_per_thread;
        let (timeline,commit_sequence_number_counter) = &*main_timeline_and_counter;
        assert_eq!(expected_commits, timeline.watermark());
        let some_index_in_penultimate_window = expected_commits - TIMELINE_WINDOW_SIZE as i64 - 1;
        assert!(timeline.try_get_window(some_index_in_penultimate_window).is_none());
    }
}
