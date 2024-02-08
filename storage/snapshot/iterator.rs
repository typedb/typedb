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

use crate::key_value::{StorageKey, StorageKeyArray};
use crate::MVCCPrefixIterator;
use crate::snapshot::buffer::BUFFER_INLINE_KEY;
use crate::snapshot::snapshot::SnapshotError;
use crate::snapshot::write::Write;

struct SnapshotPrefixIterator<'a> {
    mvcc_iterator: MVCCPrefixIterator<'a>,
    buffered_iterator: dyn Iterator<Item=(StorageKeyArray<BUFFER_INLINE_KEY>, Write)>
}

// impl<'a> SnapshotPrefixIterator<'a> {
//
//     fn next<'this>(&'this mut self) -> Option<Result<StorageKey<'this, BUFFER_INLINE_KEY>, StorageVal<'this, BUFFER_INLINE_KEY>>, SnapshotError> {
//
//     }
// }