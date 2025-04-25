/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// FIXME: exported macros must provide their own use-declarations or use fully qualified paths
//        As it stands, this macro requires imports at point of use.

use std::{cmp::Ordering, marker::PhantomData};

use encoding::{graph::thing::ThingVertex, Keyable};
use lending_iterator::{LendingIterator, Seekable};

use crate::{
    error::{ConceptReadError, ConceptReadError::SnapshotIterate},
    thing::ThingAPI,
};

pub struct InstanceIterator<T> {
    snapshot_iterator: Option<storage::snapshot::iterator::SnapshotRangeIterator>,
    _ph: PhantomData<T>,
}

impl<T> InstanceIterator<T> {
    pub(crate) fn new(snapshot_iterator: storage::snapshot::iterator::SnapshotRangeIterator) -> Self {
        Self { snapshot_iterator: Some(snapshot_iterator), _ph: PhantomData }
    }

    pub(crate) fn empty() -> Self {
        Self { snapshot_iterator: None, _ph: PhantomData }
    }
}

impl<T: ThingAPI> InstanceIterator<T> {
    pub fn seek(&mut self, target: &T) -> Result<(), Box<ConceptReadError>> {
        match self.snapshot_iterator.as_mut() {
            None => Ok(()),
            Some(iterator) => {
                let target_key = target.vertex().into_storage_key();
                iterator.seek(target_key.as_reference());
                Ok(())
            }
        }
    }
}

impl<T> Iterator for InstanceIterator<T>
where
    T: ThingAPI + 'static,
{
    type Item = Result<T, Box<ConceptReadError>>;

    fn next(&mut self) -> Option<Self::Item> {
        let iter = match &mut self.snapshot_iterator {
            Some(iter) => iter,
            None => return None,
        };
        let item = match iter.next() {
            Some(Ok((storage_key, _))) => Ok(T::new(T::Vertex::decode(storage_key.bytes()))),
            Some(Err(err)) => Err(Box::new(SnapshotIterate { source: err })),
            None => return None,
        };
        Some(item)
    }
}

impl<T> LendingIterator for InstanceIterator<T>
where
    T: ThingAPI + 'static,
{
    type Item<'a> = Result<T, Box<ConceptReadError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        Iterator::next(self)
    }
}

impl<T> Seekable<T> for InstanceIterator<T>
where
    T: ThingAPI + 'static,
{
    fn seek(&mut self, key: &T) {
        if let Some(iterator) = self.snapshot_iterator.as_mut() {
            let target = key.vertex().into_storage_key();
            iterator.seek(target.as_reference());
        }
    }

    fn compare_key(&self, instance: &Self::Item<'_>, other_instance: &T) -> Ordering {
        if let Ok(instance) = instance {
            instance.vertex().into_storage_key().cmp(&other_instance.vertex().into_storage_key())
        } else {
            // arbitary?
            Ordering::Equal
        }
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

            #[cfg(unused_unimplemented_function)]
            pub fn seek(&mut self) {
                ensure_unimplemented_unused!()
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
    ($name:ident; $mapped_type:ty; $map_fn: expr, $unmap_fn: expr) => {
        pub struct $name {
            snapshot_iterator: Option<storage::snapshot::iterator::SnapshotRangeIterator>,
        }

        #[allow(unused)]
        impl $name {
            pub(crate) fn new(snapshot_iterator: storage::snapshot::iterator::SnapshotRangeIterator) -> Self {
                $name { snapshot_iterator: Some(snapshot_iterator) }
            }

            pub fn new_empty() -> Self {
                $name { snapshot_iterator: None }
            }

            pub fn peek(&mut self) -> Option<Result<$mapped_type, Box<$crate::error::ConceptReadError>>> {
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

            pub fn seek(&mut self, target: &$mapped_type) {
                let unmapped_target = $unmap_fn(target);
                if let Some(iterator) = self.snapshot_iterator.as_mut() {
                    iterator.seek(unmapped_target.as_reference());
                }
            }
        }

        impl Iterator for $name {
            type Item = Result<$mapped_type, Box<$crate::error::ConceptReadError>>;
            fn next(&mut self) -> Option<Self::Item> {
                use ::lending_iterator::LendingIterator;
                use $crate::error::ConceptReadError::SnapshotIterate;
                self.snapshot_iterator.as_mut()?.next().map(|result| {
                    result
                        .map(|(storage_key, value_bytes)| $map_fn(storage_key, value_bytes))
                        .map_err(|error| Box::new(SnapshotIterate { source: error }))
                })
            }
        }

        impl ::lending_iterator::LendingIterator for $name {
            type Item<'a> = Result<$mapped_type, Box<$crate::error::ConceptReadError>>;
            fn next(&mut self) -> Option<Self::Item<'_>> {
                Iterator::next(self)
            }
        }

        impl ::lending_iterator::Seekable<Result<$mapped_type, Box<$crate::error::ConceptReadError>>> for $name {
            fn seek(&mut self, key: &Result<$mapped_type, Box<$crate::error::ConceptReadError>>) {
                if let Ok(key) = key {
                    self.seek(key)
                }
            }

            fn compare_key(
                &self,
                item: &Self::Item<'_>,
                other_item: &Result<$mapped_type, Box<$crate::error::ConceptReadError>>,
            ) -> std::cmp::Ordering {
                if let Ok(item) = item {
                    if let Ok(other_item) = other_item {
                        let unmapped_item = $unmap_fn(item);
                        let other_unmapped_item = $unmap_fn(other_item);
                        unmapped_item.cmp(&other_unmapped_item)
                    } else {
                        // arbitary?
                        std::cmp::Ordering::Equal
                    }
                } else {
                    // arbitary?
                    std::cmp::Ordering::Equal
                }
            }
        }
    };
}
