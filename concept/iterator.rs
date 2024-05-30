/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// FIXME: exported macros must provide their own use-declarations or use fully qualified paths
//        As it stands, this macro requires imports at point of use.
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

            pub fn peek(&mut self) -> Option<Result<$concept_type<'_>, $crate::error::ConceptReadError>> {
                use $crate::error::ConceptReadError::SnapshotIterate;
                self.snapshot_iterator.as_mut()?.peek().map(|result| {
                    result
                        .map(|(storage_key, _value_bytes)| {
                            $map_fn(::storage::key_value::StorageKey::Reference(storage_key))
                        })
                        .map_err(|error| SnapshotIterate { source: error })
                })
            }

            pub fn seek(&mut self) {
                todo!()
            }

            pub fn collect_cloned(mut self) -> Vec<$concept_type<'static>> {
                use ::lending_iterator::LendingIterator;
                self.map_static(|item| item.unwrap().into_owned()).collect()
            }
        }

        impl ::lending_iterator::LendingIterator for $name {
            type Item<'a> = Result<$concept_type<'a>, $crate::error::ConceptReadError>;
            fn next(&mut self) -> Option<Self::Item<'_>> {
                use $crate::error::ConceptReadError::SnapshotIterate;
                self.snapshot_iterator.as_mut()?.next().map(|result| {
                    result
                        .map(|(storage_key, _value_bytes)| $map_fn(storage_key))
                        .map_err(|error| SnapshotIterate { source: error })
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

            pub fn peek<$lt>(&$lt mut self) -> Option<Result<$mapped_type, $crate::error::ConceptReadError>> {
                use $crate::error::ConceptReadError::SnapshotIterate;
                self.snapshot_iterator.as_mut()?.peek().map(|result| {
                    result
                        .map(|(storage_key, value_bytes)| {
                            $map_fn(
                                ::storage::key_value::StorageKey::Reference(storage_key),
                                ::bytes::Bytes::Reference(value_bytes),
                            )
                        })
                        .map_err(|error| SnapshotIterate { source: error })
                })
            }

            pub fn seek(&mut self) {
                todo!()
            }
        }

        impl ::lending_iterator::LendingIterator for $name {
            type Item<$lt> = Result<$mapped_type, $crate::error::ConceptReadError>;
            fn next(&mut self) -> Option<Self::Item<'_>> {
                use $crate::error::ConceptReadError::SnapshotIterate;
                self.snapshot_iterator.as_mut()?.next().map(|result| {
                    result
                        .map(|(storage_key, value_bytes)| $map_fn(storage_key, value_bytes))
                        .map_err(|error| SnapshotIterate { source: error })
                })
            }

        }
    };
}
