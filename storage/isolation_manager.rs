/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Read;
use std::iter::empty;
use std::sync::{Arc, OnceLock, RwLock};
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use durability::{DurabilityRecord, DurabilityRecordType, SequenceNumber};
use logger::result::ResultExt;
use primitive::U80;

use crate::keyspace::keyspace::KeyspaceId;
use crate::snapshot::buffer::KeyspaceBuffers;
use crate::snapshot::write::Write;

#[derive(Debug)]
pub(crate) struct IsolationManager {
    timeline: Timeline,
}

impl Display for IsolationManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Timeline[windows={}, next_window_sequence_number={}, watermark={}]", self.timeline.window_count(), self.timeline.next_window_sequence_number(), self.watermark())
    }
}

impl IsolationManager {
    pub(crate) fn new(next_sequence_number: SequenceNumber) -> IsolationManager {
        IsolationManager {
            timeline: Timeline::new(next_sequence_number),
        }
    }

    pub(crate) fn opened(&self, sequence_number: &SequenceNumber) {
        self.timeline.record_reader(sequence_number);
    }

    pub(crate) fn closed(&self, commit_sequence_number: &SequenceNumber, open_sequence_number: &SequenceNumber) {
        self.timeline.record_closed(commit_sequence_number, open_sequence_number);
        self.timeline.remove_reader(open_sequence_number);
    }

    pub(crate) fn try_commit(&self, commit_sequence_number: SequenceNumber, commit_record: CommitRecord) -> Result<Arc<CommitRecord>, IsolationError> {
        let open_sequence_number = commit_record.open_sequence_number().clone();

        let shared_record = self.pending(&commit_sequence_number, commit_record);
        let validation_result = self.validate_all_concurrent(&commit_sequence_number, &shared_record);

        if validation_result.is_ok() {
            self.committed(&commit_sequence_number);
            Ok(shared_record)
        } else {
            self.closed(&commit_sequence_number, &open_sequence_number);
            validation_result.map(|ok| shared_record)
        }
    }

    fn validate_all_concurrent(&self, commit_sequence_number: &SequenceNumber, commit_record: &CommitRecord) -> Result<(), IsolationError> {
        debug_assert!(commit_record.open_sequence_number() < commit_sequence_number);
        // TODO: decide if we should block until all predecessors finish, allow out of order (non-Calvin model/traditional model)
        //       We could also validate against all predecessors even if they are validating and fail eagerly.

        let mut number = commit_record.open_sequence_number().number().number() + 1;
        let end_number = commit_sequence_number.number().number();
        while number < end_number {
            let predecessor_sequence_number = SequenceNumber::new(U80::new(number));
            self.validate_concurrent(commit_record, predecessor_sequence_number)?;
            number += 1;
        }
        Ok(())
    }

    fn validate_concurrent(&self, commit_record: &CommitRecord, predecessor_sequence_number: SequenceNumber) -> Result<(), IsolationError> {
        let predecessor_status = self.get_commit_status(&predecessor_sequence_number);
        match predecessor_status {
            CommitStatus::Empty => {
                unreachable!("A concurrent status should never be empty at commit time")
            },
            CommitStatus::Pending(predecessor_record) => {
                let result = self.validate_isolation(&commit_record, &predecessor_record);
                if result.is_err() && self.await_pending_status_commits(&predecessor_sequence_number) {
                    result
                } else {
                    Ok(())
                }
            }
            CommitStatus::Committed(predecessor_record) => {
                return self.validate_isolation(&commit_record, &predecessor_record);
            }
            CommitStatus::Closed => {
                Ok(())
            }
        }
    }

    fn await_pending_status_commits(&self, predecessor_sequence_number: &SequenceNumber) -> bool {
        debug_assert!(!matches!(self.get_commit_status(&predecessor_sequence_number), CommitStatus::Empty));
        loop {
            let status = self.get_commit_status(&predecessor_sequence_number);
            match status {
                CommitStatus::Empty => unreachable!("Illegal state - commit status cannot move from pending"),
                CommitStatus::Pending(_) => {
                    // TODO: we can improve the spin lock with async/await
                    // Note we only expect to have long waits in long chains of overlapping transactions that would conflict
                    // could also do a little sleep in the spin lock, for example if the validating is still far away
                    continue;
                }
                CommitStatus::Committed(_) => return true,
                CommitStatus::Closed => return false,
            }
        }
    }

    fn validate_isolation(&self, record: &CommitRecord, predecessor: &CommitRecord) -> Result<(), IsolationError> {
        // TODO: this can be optimised by some kind of bit-wise NAND of two bloom filter-like data structures first, since we assume few clashes this should mostly succeed
        // TODO: can be optimised with an intersection of two sorted iterators instead of iterate + gets
        for (index, buffer) in record.buffers().iter().enumerate() {
            let map = buffer.map().read().unwrap();
            if map.is_empty() {
                continue;
            }

            let predecessor_map = predecessor.buffers.get(index as KeyspaceId).map().read().unwrap();
            if (predecessor_map.is_empty()) {
                continue;
            }

            for (key, write) in map.iter() {
                let predecessor_write = predecessor_map.get(key.bytes());
                if predecessor_write.is_some() {
                    match write {
                        Write::Insert(_) => {}
                        Write::InsertPreexisting(value, reinsert) => {
                            // Re-insert the value if a predecessor has deleted it. This may create extra versions of a key
                            //  in the case that the predecessor ends up failing. However, this will be rare.
                            if matches!(predecessor_write.unwrap(), Write::Delete) {
                                reinsert.store(true, Ordering::SeqCst);
                            }
                        }
                        Write::RequireExists(_) => {
                            if matches!(predecessor_write.unwrap(), Write::Delete) {
                                return Err(IsolationError {
                                    kind: IsolationErrorKind::DeleteRequiredViolation
                                });
                            }
                        }
                        Write::Delete => {
                            // we escalate delete-required to failure, since requires imply dependencies that may be broken
                            // TODO: maybe RequireExists should be RequireDependency to capture this?
                            if matches!(predecessor_write.unwrap(), Write::RequireExists(_)) {
                                return Err(IsolationError {
                                    kind: IsolationErrorKind::RequiredDeleteViolation
                                });
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn pending(&self, commit_sequence_number: &SequenceNumber, commit_record: CommitRecord) -> Arc<CommitRecord> {
        self.timeline.record_pending(commit_sequence_number, commit_record)
    }

    fn committed(&self, commit_sequence_number: &SequenceNumber) {
        self.timeline.record_committed(commit_sequence_number)
    }

    pub(crate) fn watermark(&self) -> SequenceNumber {
        self.timeline.watermark()
    }

    pub(crate) fn iterate_records_between(&self, from: &SequenceNumber, to: &SequenceNumber) -> impl Iterator<Item=&CommitRecord> {
        empty()
    }

    pub(crate) fn get_commit_status(&self, sequence_number: &SequenceNumber) -> CommitStatus {
        self.timeline.get_status(sequence_number)
    }
}

const TIMELINE_WINDOW_SIZE: usize = 5;

#[derive(Debug)]
struct Timeline {
    next_window_and_windows: RwLock<(SequenceNumber, VecDeque<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>>)>,
}

impl Timeline {
    fn new(starting_sequence_number: SequenceNumber) -> Timeline {
        debug_assert!(starting_sequence_number.number().number() > 0);
        let last_sequence_number_value = starting_sequence_number.number() - U80::new(1);
        let initial_window = Arc::new(TimelineWindow::new(SequenceNumber::new(last_sequence_number_value)));
        let next_window_start = initial_window.end().clone();
        let mut windows = VecDeque::new();
        windows.push_back(initial_window);

        let timeline = Timeline {
            next_window_and_windows: RwLock::new((next_window_start, windows)),
        };

        // initialise the predecessor slot for readers to index against
        let sequence_number = SequenceNumber::new(last_sequence_number_value);
        timeline.record_closed(&sequence_number, &sequence_number);
        timeline
    }

    fn watermark(&self) -> SequenceNumber {
        // TODO: we should not need to get a read lock every time we want to open get the recent watermark to open a snapshot
        let (next_sequence_number, windows) = &*self.next_window_and_windows.read().unwrap_or_log();
        windows.iter().filter_map(|w| {
            if w.is_finished() {
                None
            } else {
                debug_assert!(w.watermark().is_some());
                w.watermark()
            }
        }).next().unwrap_or_else(|| SequenceNumber::new(next_sequence_number.number() - U80::new(1)))
    }

    fn get_status(&self, sequence_number: &SequenceNumber) -> CommitStatus {
        debug_assert!(self.try_get_window(sequence_number).is_some());
        self.get_window(sequence_number).get_status(sequence_number)
    }

    fn record_reader(&self, sequence_number: &SequenceNumber) {
        let window = self.get_or_create_window(sequence_number);
        window.increment_readers();
    }

    fn record_pending(&self, sequence_number: &SequenceNumber, commit_record: CommitRecord) -> Arc<CommitRecord> {
        let window = self.get_or_create_window(sequence_number);
        window.set_pending(sequence_number, commit_record)
    }

    fn record_committed(&self, sequence_number: &SequenceNumber) {
        let window = self.get_or_create_window(sequence_number);
        window.set_committed(sequence_number);
        self.remove_reader(sequence_number);
    }

    fn record_closed(&self, sequence_number: &SequenceNumber, open_sequence_number: &SequenceNumber) {
        let window = self.get_window(sequence_number);
        window.set_closed(sequence_number);
    }

    fn remove_reader(&self, reader_sequence_number: &SequenceNumber) {
        let read_window = self.get_window(reader_sequence_number);
        let readers_remaining = read_window.decrement_readers();
        // TODO if the readers are 0, we can de-allocate this window if it is the last one
        // TODO and then continue as long as the head has 0 readers
    }

    fn last_window(windows: &VecDeque<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>>) -> Arc<crate::isolation_manager::TimelineWindow<TIMELINE_WINDOW_SIZE>> {
        debug_assert!(windows.len() > 0);
        windows[windows.len() - 1].clone()
    }

    fn get_or_create_window(&self, sequence_number: &SequenceNumber) -> Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>> {
        let window = self.try_get_window(sequence_number);
        if window.is_none() {
            self.create_windows_to(sequence_number)
        } else {
            window.unwrap().clone()
        }
    }

    fn get_window(&self, sequence_number: &SequenceNumber) -> Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>> {
        self.try_get_window(sequence_number).unwrap()
    }

    fn try_get_window(&self, sequence_number: &SequenceNumber) -> Option<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>> {
        let (_, windows) = &*self.next_window_and_windows.read().unwrap_or_log();
        Self::find_window(sequence_number, windows)
    }

    fn find_window(sequence_number: &SequenceNumber, windows: &VecDeque<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>>) -> Option<Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>>> {
        windows.iter().rev().find(|window| window.contains(sequence_number))
            .map(|w| w.clone())
    }

    fn create_windows_to(&self, sequence_number: &SequenceNumber) -> Arc<TimelineWindow<TIMELINE_WINDOW_SIZE>> {
        let (next_window, windows) = &mut *self.next_window_and_windows.write().unwrap_or_log();
        // re-check if another thread created required window already

        let window = Self::find_window(sequence_number, windows);
        if window.is_none() {
            let window = loop {
                let new_window = TimelineWindow::new(*next_window);
                *next_window = new_window.end_sequence_number.clone();
                let shared_new_window = Arc::new(new_window);
                windows.push_back(shared_new_window.clone());
                if shared_new_window.contains(sequence_number) {
                    break shared_new_window;
                }
            };
            window
        } else {
            window.unwrap().clone()
        }
    }

    fn window_count(&self) -> usize {
        let (_, windows) = &*self.next_window_and_windows.read().unwrap_or_log();
        windows.len()
    }

    fn next_window_sequence_number(&self) -> SequenceNumber {
        let (next_window_sequence_number, _) = &*self.next_window_and_windows.read().unwrap_or_log();
        next_window_sequence_number.clone()
    }
}

#[derive(Debug)]
struct TimelineWindow<const SIZE: usize> {
    starting_sequence_number: SequenceNumber,
    end_sequence_number: SequenceNumber,
    slots: [AtomicU8; SIZE],
    commit_records: [OnceLock<Arc<CommitRecord>>; SIZE],
    readers: AtomicU64,
    available_slots: AtomicUsize,
}

impl<const SIZE: usize> TimelineWindow<SIZE> {
    fn new(starting_sequence_number: SequenceNumber) -> TimelineWindow<SIZE> {
        const EMPTY: OnceLock<Arc<CommitRecord>> = OnceLock::new();
        let commit_data = [EMPTY; SIZE];
        let slots: [AtomicU8; SIZE] = core::array::from_fn(|_| AtomicU8::new(0));
        debug_assert_eq!(slots[0].load(Ordering::SeqCst), SlotMarker::Empty.as_u8());

        TimelineWindow {
            starting_sequence_number: starting_sequence_number,
            end_sequence_number: SequenceNumber::new(starting_sequence_number.number() + U80::new(SIZE as u128)),
            slots: slots,
            commit_records: commit_data,
            readers: AtomicU64::new(0),
            available_slots: AtomicUsize::new(SIZE),
        }
    }

    /// Sequence number start of window (inclusive)
    fn start(&self) -> &SequenceNumber {
        &self.starting_sequence_number
    }

    /// Sequence number end of window (exclusive)
    fn end(&self) -> &SequenceNumber {
        &self.end_sequence_number
    }

    fn contains(&self, sequence_number: &SequenceNumber) -> bool {
        self.starting_sequence_number <= *sequence_number && *sequence_number < self.end_sequence_number
    }

    fn is_finished(&self) -> bool {
        self.available_slots.load(Ordering::SeqCst) == 0
    }

    fn watermark(&self) -> Option<SequenceNumber> {
        self.slots.iter().enumerate().filter_map(|(index, status)| {
            let marker = SlotMarker::from(status.load(Ordering::SeqCst));
            match marker {
                SlotMarker::Empty | SlotMarker::Pending => Some(SequenceNumber::new(self.start().number() + U80::new(index as u128 - 1))),
                SlotMarker::Committed | SlotMarker::Closed => None,
            }
        }).next()
    }

    fn set_pending(&self, sequence_number: &SequenceNumber, commit_record: CommitRecord) -> Arc<CommitRecord> {
        debug_assert!(self.contains(sequence_number));
        let index = self.index_of(sequence_number);
        let shared_record = Arc::new(commit_record);
        self.commit_records[index].set(shared_record.clone()).unwrap_or_log();
        self.slots[index].store(SlotMarker::Pending.as_u8(), Ordering::SeqCst);
        shared_record
    }

    fn set_committed(&self, sequence_number: &SequenceNumber) {
        debug_assert!(self.contains(sequence_number));
        let index = self.index_of(sequence_number);
        self.slots[index].store(SlotMarker::Committed.as_u8(), Ordering::SeqCst);
        self.available_slots.fetch_sub(1, Ordering::SeqCst);
    }

    fn set_closed(&self, sequence_number: &SequenceNumber) {
        debug_assert!(self.contains(sequence_number));
        let index = self.index_of(sequence_number);
        self.slots[index].store(SlotMarker::Closed.as_u8(), Ordering::SeqCst);
        self.available_slots.fetch_sub(1, Ordering::SeqCst);
    }

    fn get_status(&self, sequence_number: &SequenceNumber) -> CommitStatus {
        debug_assert!(self.contains(sequence_number));
        let index = self.index_of(sequence_number);
        let status = SlotMarker::from(self.slots[index].load(Ordering::SeqCst));
        match status {
            SlotMarker::Empty => CommitStatus::Empty,
            SlotMarker::Pending => CommitStatus::Pending(self.commit_records[index].get().unwrap().clone()),
            SlotMarker::Committed => CommitStatus::Committed(self.commit_records[index].get().unwrap().clone()),
            SlotMarker::Closed => CommitStatus::Closed,
        }
    }

    fn increment_readers(&self) {
        self.readers.fetch_add(1, Ordering::SeqCst);
    }

    fn decrement_readers(&self) -> u64 {
        self.readers.fetch_sub(1, Ordering::SeqCst)
    }

    fn index_of(&self, sequence_number: &SequenceNumber) -> usize {
        debug_assert!(sequence_number.number().number() - self.starting_sequence_number.number().number() < usize::MAX as u128);
        (sequence_number.number().number() - self.starting_sequence_number.number().number()) as usize
    }
}

#[derive(Debug)]
pub(crate) enum CommitStatus {
    Empty,
    Pending(Arc<CommitRecord>),
    Committed(Arc<CommitRecord>),
    Closed,
}

#[derive(Debug)]
enum SlotMarker {
    Empty,
    Pending,
    Committed,
    Closed,
}

impl SlotMarker {
    const fn as_u8(&self) -> u8 {
        match self {
            SlotMarker::Empty => 0,
            SlotMarker::Pending => 1,
            SlotMarker::Committed => 2,
            SlotMarker::Closed => 3,
        }
    }

    const fn from(value: u8) -> Self {
        match value {
            0 => SlotMarker::Empty,
            1 => SlotMarker::Pending,
            2 => SlotMarker::Committed,
            3 => SlotMarker::Closed,
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

impl CommitRecord {
    pub(crate) const DURABILITY_RECORD_TYPE: DurabilityRecordType = 0;
    pub(crate) const DURABILITY_RECORD_NAME: &'static str = "commit_record";

    pub(crate) fn new(buffers: KeyspaceBuffers, open_sequence_number: SequenceNumber) -> CommitRecord {
        CommitRecord {
            buffers: buffers,
            open_sequence_number: open_sequence_number,
        }
    }

    pub(crate) fn buffers(&self) -> &KeyspaceBuffers {
        &self.buffers
    }

    pub(crate) fn open_sequence_number(&self) -> &SequenceNumber {
        &self.open_sequence_number
    }
}

impl DurabilityRecord for CommitRecord {
    fn record_type(&self) -> DurabilityRecordType {
        return Self::DURABILITY_RECORD_TYPE;
    }

    fn serialise_into(&self, writer: &mut impl std::io::Write) -> bincode::Result<()> {
        debug_assert_eq!(
            bincode::serialize(
                &bincode::deserialize::<CommitRecord>(bincode::serialize(&self).as_ref().unwrap()).unwrap()
            ).unwrap(),
            bincode::serialize(self).unwrap());
        bincode::serialize_into(writer, &self.buffers)
    }

    // fn deserialise_from(record_type: DurabilityRecordType, reader: &impl Read) -> Result<Self, dyn Error>
    //     where Self: Sized {
    //     assert_eq!(Self::DURABILITY_RECORD_TYPE, record_type);
    //     bincode::deserialize_from(reader)
    // }
}

#[derive(Debug)]
pub struct IsolationError {
    pub kind: IsolationErrorKind,
}

#[derive(Debug)]
pub enum IsolationErrorKind {
    DeleteRequiredViolation,
    RequiredDeleteViolation,
}

impl Display for IsolationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            IsolationErrorKind::DeleteRequiredViolation => write!(f, "Isolation violation: Delete-Require conflict. A concurrent commit has deleted a key required by this transaction. Please retry"),
            IsolationErrorKind::RequiredDeleteViolation => write!(f, "Isolation violation: Require-Delete conflict. This commit has deleted a key required by a concurrent transaction. Please retry"),
        }
    }
}

impl Error for IsolationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            IsolationErrorKind::DeleteRequiredViolation => None,
            IsolationErrorKind::RequiredDeleteViolation => None,
        }
    }
}
