/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// FIXME: exported macros must provide their own use-declarations or use fully qualified paths
//        As it stands, this macro requires imports at point of use.

use std::marker::PhantomData;

use encoding::graph::thing::ThingVertex;
use lending_iterator::{higher_order::Hkt, LendingIterator};

use crate::{
    error::{ConceptReadError, ConceptReadError::SnapshotIterate},
    thing::ThingAPI,
};

pub struct InstanceIterator<T: Hkt> {
    snapshot_iterator: Option<storage::snapshot::iterator::SnapshotRangeIterator>,
    _ph: PhantomData<T>,
}

impl<T: Hkt> InstanceIterator<T> {
    pub(crate) fn new(snapshot_iterator: storage::snapshot::iterator::SnapshotRangeIterator) -> Self {
        Self { snapshot_iterator: Some(snapshot_iterator), _ph: PhantomData }
    }

    pub(crate) fn empty() -> Self {
        Self { snapshot_iterator: None, _ph: PhantomData }
    }
}

impl<T> LendingIterator for InstanceIterator<T>
where
    T: Hkt,
    for<'a> <T as Hkt>::HktSelf<'a>: ThingAPI<'a>,
{
    type Item<'a> = Result<T::HktSelf<'a>, Box<ConceptReadError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.snapshot_iterator.as_mut()?.next().map(|result| {
            result
                .map(|(storage_key, _value_bytes)| {
                    T::HktSelf::new(<T::HktSelf<'_> as ThingAPI<'_>>::Vertex::new(storage_key.into_bytes()))
                })
                .map_err(|error| Box::new(SnapshotIterate { source: error }))
        })
    }
}

#[macro_export]
macro_rules! concept_iterator {
    ($name:ident, $concept_type:ident, $map_fn: expr) => {
        pub struct $name {
            snapshot_iterator: Option<storage::snapshot::iterator::SnapshotRangeIterator>,
        }

        #[allow(unused)]
        impl $name {
            pub(crate) fn new(snapshot_iterator: storage::snapshot::iterator::SnapshotRangeIterator) -> Self {
                $name { snapshot_iterator: Some(snapshot_iterator) }
            }

            pub(crate) fn new_empty() -> Self {
                $name { snapshot_iterator: None }
            }

            pub fn seek(&mut self) {
                todo!()
            }
        }

        impl ::lending_iterator::LendingIterator for $name {
            type Item<'a> = Result<$concept_type, Box<$crate::error::ConceptReadError>>;
            fn next(&mut self) -> Option<Self::Item<'_>> {
                use $crate::error::ConceptReadError::SnapshotIterate;
                self.snapshot_iterator.as_mut()?.next().map(|result| {
                    result
                        .map(|(storage_key, _value_bytes)| $map_fn(storage_key))
                        .map_err(|error| Box::new(SnapshotIterate { source: error }))
                })
            }
        }
    };
}

#[macro_export]
macro_rules! edge_iterator {
    ($name:ident; $lt:lifetime -> $mapped_type:ty; $map_fn: expr) => {
        pub struct $name {
            snapshot_iterator: Option<storage::snapshot::iterator::SnapshotRangeIterator>,
        }

        #[allow(unused)]
        impl $name {
            pub(crate) fn new(snapshot_iterator: storage::snapshot::iterator::SnapshotRangeIterator) -> Self {
                $name { snapshot_iterator: Some(snapshot_iterator) }
            }

            pub(crate) fn new_empty() -> Self {
                $name { snapshot_iterator: None }
            }

            pub fn peek<$lt>(&$lt mut self) -> Option<Result<$mapped_type, Box<$crate::error::ConceptReadError>>> {
                use $crate::error::ConceptReadError::SnapshotIterate;
                self.snapshot_iterator.as_mut()?.peek().map(|result| {
                    result
                        .map(|(storage_key, value_bytes)| {
                            $map_fn(
                                ::storage::key_value::StorageKey::Reference(storage_key),
                                ::bytes::Bytes::Reference(value_bytes),
                            )
                        })
                        .map_err(|error| Box::new(SnapshotIterate { source: error }))
                })
            }

            pub fn seek(&mut self) {
                todo!()
            }
        }

        impl ::lending_iterator::LendingIterator for $name {
            type Item<$lt> = Result<$mapped_type, Box<$crate::error::ConceptReadError>>;
            fn next(&mut self) -> Option<Self::Item<'_>> {
                use $crate::error::ConceptReadError::SnapshotIterate;
                self.snapshot_iterator.as_mut()?.next().map(|result| {
                    result
                        .map(|(storage_key, value_bytes)| $map_fn(storage_key, value_bytes))
                        .map_err(|error| Box::new(SnapshotIterate { source: error }))
                })
            }

        }
    };
}
