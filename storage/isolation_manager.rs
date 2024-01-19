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

use std::collections::BTreeMap;
use std::iter::empty;
use std::rc::Rc;
use std::sync::RwLock;
use logger::result::ResultExt;

use wal::SequenceNumber;

use crate::key::WriteKey;
use crate::snapshot::Snapshot;

pub(crate) struct IsolationManager {
    // TODO improve: RWLock is not optimal
    commits: RwLock<Vec<(SequenceNumber, SequenceNumber, Rc<BTreeMap<WriteKey, Option<Box<[u8]>>>>)>>
}

impl IsolationManager {
    pub(crate) fn new() -> IsolationManager {
        IsolationManager {
            commits: RwLock::new(vec![]),
        }
    }

    pub(crate) fn notify_open(&self, sequence_number: SequenceNumber) {
        todo!()
    }

    pub(crate) fn notify_commit(&self, open_sequence_number: SequenceNumber, commit_sequence_number: SequenceNumber, writes: Rc<BTreeMap<WriteKey, Option<Box<[u8]>>>>) {
        let mut lock = self.commits.write().unwrap_or_log();
        lock.push((open_sequence_number, commit_sequence_number, writes));
    }

    pub(crate) fn notify_closed(&self, sequence_number: SequenceNumber) {
        todo!()
    }

    pub(crate) fn iterate_snapshots(&self, from: SequenceNumber, to: SequenceNumber) -> impl Iterator<Item=Snapshot> {
        empty()
    }
}


