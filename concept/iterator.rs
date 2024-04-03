/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::{error::Error, fmt, sync::Arc};

use storage::snapshot::iterator::SnapshotIteratorError;

#[derive(Debug)]
pub enum ConceptIteratorError {
    SnapshotIterator { source: Arc<SnapshotIteratorError> },
}

impl fmt::Display for ConceptIteratorError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for ConceptIteratorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotIterator { source } => Some(source),
        }
    }
}

// FIXME: exported macros must provide their own use-declarations or use fully qualified paths
//        As it stands, this macro requires imports at point of use.
#[macro_export]
macro_rules! concept_iterator {
    ($name:ident, $concept_type:ident, $map_fn: expr) => {
        pub struct $name<'a, const S: usize> {
            snapshot_iterator: Option<storage::snapshot::iterator::SnapshotRangeIterator<'a, S>>,
        }

        impl<'a, const S: usize> $name<'a, S> {
            pub(crate) fn new(snapshot_iterator: storage::snapshot::iterator::SnapshotRangeIterator<'a, S>) -> Self {
                $name { snapshot_iterator: Some(snapshot_iterator) }
            }

            pub(crate) fn new_empty() -> Self {
                $name { snapshot_iterator: None }
            }

            pub fn peek(&mut self) -> Option<Result<$concept_type<'_>, $crate::iterator::ConceptIteratorError>> {
                use $crate::iterator::ConceptIteratorError::SnapshotIterator;
                self.iter_peek().map(|result| {
                    result
                        .map(|(storage_key, _value_bytes)| $map_fn(storage_key))
                        .map_err(|error| SnapshotIterator { source: error })
                })
            }

            // a lending iterator trait is infeasible with the current borrow checker
            #[allow(clippy::should_implement_trait)]
            pub fn next(&mut self) -> Option<Result<$concept_type<'_>, $crate::iterator::ConceptIteratorError>> {
                use $crate::iterator::ConceptIteratorError::SnapshotIterator;
                self.iter_next().map(|result| {
                    result
                        .map(|(storage_key, _value_bytes)| $map_fn(storage_key))
                        .map_err(|error| SnapshotIterator { source: error })
                })
            }

            pub fn seek(&mut self) {
                todo!()
            }

            fn iter_peek(
                &mut self,
            ) -> Option<
                Result<
                    (StorageKeyReference<'_>, bytes::byte_reference::ByteReference<'_>),
                    std::sync::Arc<storage::snapshot::iterator::SnapshotIteratorError>,
                >,
            > {
                if let Some(iter) = self.snapshot_iterator.as_mut() {
                    iter.peek()
                } else {
                    None
                }
            }

            fn iter_next(
                &mut self,
            ) -> Option<
                Result<
                    (StorageKeyReference<'_>, bytes::byte_reference::ByteReference<'_>),
                    std::sync::Arc<storage::snapshot::iterator::SnapshotIteratorError>,
                >,
            > {
                if let Some(iter) = self.snapshot_iterator.as_mut() {
                    iter.next()
                } else {
                    None
                }
            }

            pub fn collect_cloned(mut self) -> Vec<$concept_type<'static>> {
                let mut vec = Vec::new();
                loop {
                    let item = self.next();
                    if item.is_none() {
                        break;
                    }
                    let key = item.unwrap().unwrap().into_owned();
                    vec.push(key);
                }
                vec
            }
        }
    };
}
