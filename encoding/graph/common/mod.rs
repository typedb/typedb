/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub(crate) mod schema_id_allocator;
pub(crate) mod value_hasher;

pub(crate) enum ExistingOrNew<T> {
    Existing { item: T, in_storage: bool },
    New { item: T },
}

impl<T> ExistingOrNew<T> {
    pub(crate) fn into_inner(self) -> T {
        match self {
            ExistingOrNew::Existing { item, .. } | ExistingOrNew::New { item } => item,
        }
    }

    pub(crate) fn map<U, F>(self, mapper: F) -> ExistingOrNew<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            ExistingOrNew::Existing { item, in_storage } => ExistingOrNew::Existing { item: mapper(item), in_storage },
            ExistingOrNew::New { item } => ExistingOrNew::New { item: mapper(item) },
        }
    }
}
