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

#[macro_export]
macro_rules! concept_iterator {
    ($name:ident, $concept_type:ident, $map_fn: expr) => {
        pub struct $name<'a, const S: usize> {
            snapshot_iterator: SnapshotRangeIterator<'a, S>,
        }

        impl<'a, const S: usize> $name<'a, S> {
            pub(crate) fn new(snapshot_iterator: SnapshotRangeIterator<'a, S>) -> Self {
                $name { snapshot_iterator }
            }

            pub fn peek(&mut self) -> Option<Result<$concept_type<'_>, ConceptError>> {
                self.snapshot_iterator.peek().map(|result| {
                    result.map(|(storage_key, _value_bytes)| $map_fn(storage_key)).map_err(|snapshot_error| {
                        ConceptError { kind: ConceptErrorKind::SnapshotError { source: snapshot_error } }
                    })
                })
            }

            // a lending iterator trait is infeasible with the current borrow checker
            #[allow(clippy::should_implement_trait)]
            pub fn next(&mut self) -> Option<Result<$concept_type<'_>, ConceptError>> {
                self.snapshot_iterator.next().map(|result| {
                    result.map(|(storage_key, _value_bytes)| $map_fn(storage_key)).map_err(|snapshot_error| {
                        ConceptError { kind: ConceptErrorKind::SnapshotError { source: snapshot_error } }
                    })
                })
            }

            pub fn seek(&mut self) {
                todo!()
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
