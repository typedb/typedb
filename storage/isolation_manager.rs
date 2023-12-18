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

use std::iter::empty;

use wal::SequenceNumber;

use crate::snapshot::Snapshot;

struct IsolationManager {}

impl IsolationManager {
    fn notify_open(&self, sequence_number: SequenceNumber) {
        todo!()
    }

    fn notify_commit(&self, sequence_number: SequenceNumber, snapshot: &Snapshot) {
        todo!()
    }

    fn notify_closed(&self, sequence_number: SequenceNumber) {
        todo!()
    }

    fn iterate_snapshots(&self, from: SequenceNumber, to: SequenceNumber) -> impl Iterator<Item=Snapshot> {
        empty()
    }
}


