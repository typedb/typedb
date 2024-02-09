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

use std::iter::Peekable;
use crate::key_value::{StorageKey, StorageKeyArray};
use crate::MVCCPrefixIterator;
use crate::snapshot::buffer::BUFFER_INLINE_KEY;
use crate::snapshot::snapshot::SnapshotError;
use crate::snapshot::write::Write;

struct SnapshotPrefixIterator<'a> {
    mvcc_iterator: MVCCPrefixIterator<'a>,
    buffered_iterator: dyn Iterator<Item=(StorageKeyArray<BUFFER_INLINE_KEY>, Write)>
}
//
// impl<'a> SnapshotPrefixIterator<'a> {
//
//     fn new(&self, mvcc_iterator: MVCCPrefixIterator<'a>, buffered_iterator: impl Iterator<Item=(StorageKeyArray<BUFFER_INLINE_KEY>, Write)>) -> SnapshotPrefixIterator<'a> {
//         SnapshotPrefixIterator {
//             mvcc_iterator: mvcc_iterator,
//             buffered_iterator: buffered_iterator.peekable(),
//         }
//     }
//
//     fn peek(&mut self) {
//         let next_mvcc = self.mvcc_iterator.peek();
//         let next_buffered = self.buffered_iterator.peek()
//
//     }
// }
//

impl<'a> SnapshotPrefixIterator<'a> {

    fn new() -> SnapshotPrefixIterator<'a> {
        todo!()
    }

    fn peek(&mut self) {
        ///
        /// if state == READY -> return peek
        /// if state == DONE -> return empty
        /// if state == ERROR -> return error
        /// if state == INIT -> update_state, then return peek()
        /// if state == UPDATING -> unreachable
    }

    fn advance(&mut self) {
        ///
        /// if state == DONE | ERROR -> do nothing
        /// if state == READY -> sub-iterator.advance(), state = UPDATING, update_state()
        /// if state == INIT -> update_state(), then advance()
        /// if state == UPDATING -> unreachable()
    }

    fn update_state(&mut self) {
        /// assert state ==  INIT | UPDATING
        ///
        /// poll item = sub-iterator
        /// if item == None -> state = DONE
        /// if item == Some(error) -> state = ERROR(error)
        /// if item == Some(value) -> state = READY
    }

    fn seek(&mut self) {
        // if state == DONE | ERROR -> do nothing
        // if state == INIT -> seek(), state = UPDATING, update_state()
        // if state == EMPTY -> seek(), state = UPDATING, update_state() TODO: compare to previous to prevent backward seek?
        // if state == READY -> seek(), state = UPDATING, update_state() TODO: compare to peek() to prevent backwrard seek?
    }



}