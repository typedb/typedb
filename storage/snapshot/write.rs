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

use std::sync::atomic::{AtomicBool, Ordering};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use bytes::byte_array::ByteArray;
use crate::snapshot::buffer::BUFFER_INLINE_VALUE;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) enum Write {
    // Insert KeyValue with a new version. Never conflicts.
    Insert(ByteArray<BUFFER_INLINE_VALUE>),
    // Insert KeyValue with new version if a concurrent Txn deletes Key. Boolean indicates requires re-insertion. Never conflicts.
    InsertPreexisting(ByteArray<BUFFER_INLINE_VALUE>, Arc<AtomicBool>),
    // TODO what happens during replay
    // Mark Key as required from storage. Caches existing storage Value. Conflicts with Delete.
    RequireExists(ByteArray<BUFFER_INLINE_VALUE>),
    // Delete with a new version. Conflicts with Require.
    Delete,
}

impl PartialEq<Self> for Write {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Write::Insert(value) => {
                if let Write::Insert(other_value) = other {
                    value == other_value
                } else {
                    false
                }
            }
            Write::InsertPreexisting(value, reinsert) => {
                if let Write::InsertPreexisting(other_value, other_reinsert) = other {
                    other_value == value &&
                        reinsert.load(Ordering::SeqCst) == other_reinsert.load(Ordering::SeqCst)
                } else {
                    false
                }
            }
            Write::RequireExists(value) => {
                if let Write::RequireExists(other_value) = other {
                    value == other_value
                } else {
                    false
                }
            }
            Write::Delete => {
                matches!(other, Write::Delete)
            }
        }
    }
}

impl Eq for Write {}

impl Write {
    pub(crate) fn is_insert(&self) -> bool {
        matches!(self, Write::Insert(_))
    }

    pub(crate) fn is_insert_preexisting(&self) -> bool {
        matches!(self, Write::InsertPreexisting(_, _))
    }

    pub(crate) fn is_delete(&self) -> bool {
        matches!(self, Write::Delete)
    }

    pub(crate) fn get_value(&self) -> &ByteArray<BUFFER_INLINE_VALUE> {
        match self {
            Write::Insert(value) => value,
            Write::InsertPreexisting(value, _) => value,
            Write::RequireExists(value) => value,
            Write::Delete => panic!("Buffered delete does not have a value."),
        }
    }
}
