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

use std::collections::{BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::iter::empty;
use std::sync::RwLock;
use std::sync::atomic::AtomicU64;

use durability::SequenceNumber;
use logger::result::ResultExt;

use crate::key_value::Key;
use crate::snapshot::{ModifyData, WriteData};

pub(crate) struct IsolationManager {
    // TODO improve: RWLock is not optimal
    commits: RwLock<Vec<CommitRecord>>,
    timeline: Timeline,
}

impl IsolationManager {
    pub(crate) fn new(next_sequence_number: SequenceNumber) -> IsolationManager {
        IsolationManager {
            commits: Default::default(),
            timeline: Timeline::new(next_sequence_number),
        }
    }

    pub(crate) fn notify_open(&self, sequence_number: SequenceNumber) {
        // TODO
    }

    pub(crate) fn notify_commit_confirmed(&self, commit_sequence_number: SequenceNumber, commit_record: CommitRecord) {
        let mut commits = self.commits.write().unwrap_or_log();
        commits.push(commit_record);
    }

    pub(crate) fn notify_commit_failed(&self, commit_sequence_number: SequenceNumber) {
        todo!()
    }

    pub(crate) fn last_committed(&self) -> SequenceNumber {
        SequenceNumber::new(0)
    }

    pub(crate) fn iterate_commits_between(&self, from: &SequenceNumber, to: &SequenceNumber) -> impl Iterator<Item=&CommitRecord> {
        empty()
    }

    pub(crate) fn validate_isolation(&self, predecessor: &CommitRecord, successor: &CommitRecord) -> Result<(), IsolationError> {
        // predecessor.writes

        // if (txn.dataStorage.modifyDeleteConflict(mayConflict.dataStorage)) {
        //     throw TypeDBException.of(TRANSACTION_ISOLATION_MODIFY_DELETE_VIOLATION);
        // } else if (txn.dataStorage.deleteModifyConflict(mayConflict.dataStorage)) {
        //     throw TypeDBException.of(TRANSACTION_ISOLATION_DELETE_MODIFY_VIOLATION);
        // } else if (txn.dataStorage.exclusiveCreateConflict(mayConflict.dataStorage)) {
        //     throw TypeDBException.of(TRANSACTION_ISOLATION_EXCLUSIVE_CREATE_VIOLATION);
        // }
        todo!()
    }

    fn delete_modify_violation(&self, predecessor_writes: WriteData, successor_modifications: ModifyData) -> bool {
        // TODO: this can be optimised by some kind of bit-wise NAND of two bloom filter-like data structures first, since we assume few clashes this should mostly succeed
        successor_modifications.iter() .any(|key|
            predecessor_writes.get(key).map_or(false, |write| write.is_delete())
        )
    }

    fn modify_delete_violation(&self, predecessor_modifications: ModifyData, successor_writes: WriteData) -> bool {
        // TODO: this can be optimised by some kind of bit-wise NAND of two bloom filter-like data structures first, since we assume few clashes this should mostly succeed
        successor_writes.iter().any(|(key, write)|
            write.is_delete() && predecessor_modifications.get(key).is_some()
        )
    }
}

const TIMELINE_WINDOW_SIZE: usize = 100;

struct Timeline {
    windows: RwLock<VecDeque<Box<TimelineWindow<TIMELINE_WINDOW_SIZE>>>>,
}

impl Timeline {
    fn new(starting_sequence_number: SequenceNumber) -> Timeline {
        let mut windows = VecDeque::new();
        windows.push_back(Box::new(TimelineWindow::new(starting_sequence_number)));
        Timeline {
            windows: RwLock::new(windows),
        }
    }

    fn set_pending(&self, sequence_number: SequenceNumber, commit_record: CommitRecord) {
        let record = TimelineSlot::Pending(commit_record);
        let window = self.get_or_create_window(sequence_number);
    }

    fn get_or_create_window(&self, sequence_number: SequenceNumber) {
        let windows = self.windows.read().unwrap_or_log();
        let last_window = windows.back();
    }

    fn try_get_window(&self, sequence_number: SequenceNumber) -> Option<Box<TimelineWindow<TIMELINE_WINDOW_SIZE>>> {
        // let windows = self.windows.read().unwrap_or_log();
        // windows.iter().rev().find(|window| window.contains(&sequence_number))
        //     .map(|w| *w)
        todo!()
    }
}

struct TimelineWindow<const SIZE: usize> {
    starting_sequence_number: SequenceNumber,
    end_sequence_number: SequenceNumber,
    window: [TimelineSlot; SIZE],
    readers: AtomicU64,
}

impl<const SIZE: usize> TimelineWindow<SIZE> {
    fn new(starting_sequence_number: SequenceNumber) -> TimelineWindow<SIZE> {
        const EMPTY: TimelineSlot = TimelineSlot::Empty;
        let window = [EMPTY; SIZE];
        TimelineWindow {
            starting_sequence_number: starting_sequence_number,
            end_sequence_number: starting_sequence_number.plus(SIZE as u64),
            window: window,
            readers: AtomicU64::new(0),
        }
    }

    fn start(&self) -> &SequenceNumber {
        &self.starting_sequence_number
    }

    fn end(&self) -> &SequenceNumber {
        &self.end_sequence_number
    }

    fn contains(&self, sequence_number: &SequenceNumber) -> bool {
        self.starting_sequence_number <= *sequence_number && *sequence_number < self.end_sequence_number
    }
}

enum TimelineSlot {
    Empty,
    Pending(CommitRecord),
    Committed(CommitRecord),
    Failed,
}

pub(crate) struct CommitRecord {
    // TODO: this could read-through to the WAL if we have to?
    writes: WriteData,
    modifications: ModifyData,
    open_sequence_number: SequenceNumber,
}

impl CommitRecord {
    pub(crate) fn new(writes: WriteData, modifications: BTreeSet<Key>, open_sequence_number: SequenceNumber) -> CommitRecord {
        CommitRecord {
            writes: writes,
            modifications: modifications,
            open_sequence_number: open_sequence_number
        }
    }

    pub(crate) fn writes(&self) -> &WriteData {
        &self.writes
    }

    pub(crate) fn open_sequence_number(&self) -> &SequenceNumber {
        &self.open_sequence_number
    }
}


#[derive(Debug)]
pub struct IsolationError {
    pub kind: IsolationErrorKind,
}

#[derive(Debug)]
pub enum IsolationErrorKind {
}

impl Display for IsolationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for IsolationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        todo!()
        // match &self.kind {
        // }
    }
}
