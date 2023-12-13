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
 *
 */

use wal::{Record, SequenceNumber};

pub(crate) trait DurabilityService {
    fn sequenced_write(&self, record: &dyn Record) -> SequenceNumber;

    fn last_sequence_number(&self) -> SequenceNumber;

    fn iterate_records_from(&self, sequence_number: SequenceNumber) -> Box<dyn Iterator<Item=(SequenceNumber, &dyn Record)>>;
}
