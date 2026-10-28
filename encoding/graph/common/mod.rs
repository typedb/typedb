/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub(crate) mod schema_id_allocator;
pub(crate) mod value_hasher;

pub(crate) enum FoundOrAbsent<T> {
    Found { item: T, in_storage: bool },
    Absent { item: T },
}

impl<T> FoundOrAbsent<T> {
    pub(crate) fn into_inner(self) -> T {
        match self {
            FoundOrAbsent::Found { item, .. } | FoundOrAbsent::Absent { item } => item,
        }
    }

    pub(crate) fn map<U, F>(self, mapper: F) -> FoundOrAbsent<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            FoundOrAbsent::Found { item, in_storage } => FoundOrAbsent::Found { item: mapper(item), in_storage },
            FoundOrAbsent::Absent { item } => FoundOrAbsent::Absent { item: mapper(item) },
        }
    }
}
