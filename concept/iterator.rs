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
            snapshot_iterator: SnapshotPrefixIterator<'a, S>,
        }

        impl<'a, const S: usize> $name<'a, S> {
            pub(crate) fn new(snapshot_iterator: SnapshotPrefixIterator<'a, S>) -> Self {
                $name { snapshot_iterator: snapshot_iterator, }
            }

            pub fn peek<'this>(&'this mut self) -> Option<Result<$concept_type<'this>, ConceptError>> {
                self.snapshot_iterator.peek().map(|result|
                    result.map(|(storage_key, value_bytes)| {
                        $map_fn(storage_key)
                    }).map_err(|snapshot_error| {
                        ConceptError { kind: ConceptErrorKind::SnapshotError { source: snapshot_error } }
                    })
                )
            }

            pub fn next<'this>(&'this mut self) -> Option<Result<$concept_type<'this>, ConceptError>> {
                self.snapshot_iterator.next().map(|result|
                    result.map(|(storage_key, value_bytes)| {
                        $map_fn(storage_key)
                    }).map_err(|snapshot_error| {
                        ConceptError { kind: ConceptErrorKind::SnapshotError { source: snapshot_error } }
                    })
                )
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


//
// struct Iter<'a> {
//     source: Source<'a>,
//     index: usize,
// }
//
// impl<'a> Iter<'a> {
//
//     fn new(source: Source<'a>) -> Self {
//         Iter { source, index: 0 }
//     }
//
//     fn next<'this>(&'this mut self) -> Option<Cow<'this, u8>> {
//         self.index += 1;
//         if self.index < self.source.data.len() {
//             Some(Cow::Borrowed(&self.source.data[self.index - 1]))
//         } else {
//             None
//         }
//     }
//
//     fn collect(mut self) -> Vec<u8> {
//         let mut vec = vec![];
//         loop {
//             let next = self.next();
//             if next.is_none() {
//                 break;
//             }
//             let byte = next.unwrap().into_owned().clone();
//             vec.push(byte)
//         }
//         vec
//     }
// }

//
// pub struct EntityIterator<'a, const S: usize> {
//     snapshot_iterator: SnapshotPrefixIterator<'a, S>,
// }
//
// impl<'a, const S: usize> EntityIterator<'a, S> {
//     pub(crate) fn new(snapshot_iterator: SnapshotPrefixIterator<'a, S>) -> Self {
//         EntityIterator { snapshot_iterator: snapshot_iterator }
//     }
//
//     pub fn next<'this>(&'this mut self) -> Option<Result<Entity<'this>, ConceptError>> {
//         self.snapshot_iterator.next().map(|result|
//             result.map(|(storage_key, value_bytes)| {
//                 create_entity(storage_key)
//             }).map_err(|snapshot_error| {
//                 ConceptError { kind: ConceptErrorKind::SnapshotError { source: snapshot_error } }
//             })
//         )
//     }
//
//     pub(crate) fn collect_cloned(mut self) -> Vec<Entity<'static>> {
//         let mut vec = Vec::new();
//         loop {
//             let item = self.next();
//             if item.is_none() {
//                 break;
//             }
//             let key = item.unwrap().unwrap().to_owned();
//             vec.push(key);
//         }
//         vec
//     }
// }


// pub struct ConceptIterator<'a, F, T, const S: usize>
//     where
//         T: Concept,
//         F: for<'b> Fn(StorageKeyReference<'b>) -> T {
//     snapshot_iterator: SnapshotPrefixIterator<'a, S>,
//     key_mapper: F,
// }
//
// impl<'a, F, T, const S: usize> ConceptIterator<'a, F, T, S>
//     where
//         T: Concept,
//         F: for<'b> Fn(StorageKeyReference<'b>) -> T {
//     pub(crate) fn new(
//         snapshot_iterator: SnapshotPrefixIterator<'a, S>,
//         key_mapper: F
//     ) -> Self {
//         ConceptIterator {
//             snapshot_iterator: snapshot_iterator,
//             key_mapper: key_mapper,
//         }
//     }
//
//     pub fn peek<'this>(&'this mut self) -> Option<Result<T, ConceptError>> where T: 'this {
//         self.snapshot_iterator.peek().map(|result|
//             result.map(|(storage_key, value_bytes)| {
//                 (self.key_mapper)(storage_key)
//             }).map_err(|snapshot_error| {
//                 ConceptError { kind: ConceptErrorKind::SnapshotError { source: snapshot_error } }
//             })
//         )
//     }
//
//     pub fn next<'this>(&'this mut self) -> Option<Result<T, ConceptError>> where T: 'this {
//         self.snapshot_iterator.next().map(|result|
//             result.map(|(storage_key, value_bytes)| {
//                 (self.key_mapper)(storage_key)
//             }).map_err(|snapshot_error| {
//                 ConceptError { kind: ConceptErrorKind::SnapshotError { source: snapshot_error } }
//             })
//         )
//     }
//
//     pub fn seek(&mut self) {
//         todo!()
//     }
//
//     pub(crate) fn collect_cloned(mut self) -> Vec<T> where T: 'static {
//         let mut vec = Vec::new();
//         loop {
//             let item = self.next();
//             if item.is_none() {
//                 break;
//             }
//             let key = item.unwrap().unwrap();
//             vec.push(key.clone());
//         }
//         vec
//     }
// }
