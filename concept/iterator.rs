/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// FIXME: exported macros must provide their own use-declarations or use fully qualified paths
//        As it stands, this macro requires imports at point of use.

use std::{cmp::Ordering, iter::Peekable, marker::PhantomData};

use encoding::{Keyable, graph::thing::ThingVertex};
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

// walks instances with their edges, both sorted by instance, so instances without edges are visited too
pub(crate) struct InstanceEdgeWalk<Instance, Instances, Edges: Iterator, EdgeInstance> {
    instances: Instances,
    edges: Peekable<Edges>,
    edge_instance: EdgeInstance,
    current: Option<Instance>,
}

impl<Instance, Edge, Instances, Edges, EdgeInstance> InstanceEdgeWalk<Instance, Instances, Edges, EdgeInstance>
where
    Instance: Ord + Clone,
    Instances: Iterator<Item = Result<Instance, Box<ConceptReadError>>>,
    Edges: Iterator<Item = Result<Edge, Box<ConceptReadError>>>,
    EdgeInstance: Fn(&Edge) -> Instance,
{
    pub(crate) fn new(instances: Instances, edges: Edges, edge_instance: EdgeInstance) -> Self {
        Self { instances, edges: edges.peekable(), edge_instance, current: None }
    }

    // skips the remaining edges of the current instance, then moves to the next one
    pub(crate) fn next_instance(&mut self) -> Option<Result<Instance, Box<ConceptReadError>>> {
        while let Some(edge) = self.next_edge() {
            if let Err(error) = edge {
                return Some(Err(error));
            }
        }
        self.current = None;
        let instance = self.instances.next()?;
        if let Ok(instance) = &instance {
            self.current = Some(instance.clone());
        }
        Some(instance)
    }

    // the next edge of the current instance, skipping edges of instances not returned
    pub(crate) fn next_edge(&mut self) -> Option<Result<Edge, Box<ConceptReadError>>> {
        let current = self.current.as_ref()?;
        loop {
            let edge_instance = match self.edges.peek()? {
                Ok(edge) => (self.edge_instance)(edge),
                Err(_) => return self.edges.next(),
            };
            match edge_instance.cmp(current) {
                Ordering::Less => {
                    self.edges.next();
                }
                Ordering::Equal => return self.edges.next(),
                Ordering::Greater => return None,
            }
        }
    }
}

#[macro_export]
macro_rules! concept_iterator {
    ($name:ident, $concept_type:ident, $map_fn: expr) => {
        pub struct $name {
            snapshot_iterator: Option<storage::snapshot::iterator::SnapshotRangeIterator>,
        }

        #[allow(unexpected_cfgs, reason = "unimplemented function is left as a marker for future implementation")]
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
                use lending_iterator::LendingIterator;
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

#[cfg(test)]
mod tests {
    use super::*;

    type Item<T> = Result<T, Box<ConceptReadError>>;

    fn walk(instances: Vec<Item<u64>>, edges: Vec<Item<(u64, char)>>) -> Item<Vec<(u64, Vec<char>)>> {
        let mut walk = InstanceEdgeWalk::new(instances.into_iter(), edges.into_iter(), |&(instance, _)| instance);
        let mut visited = Vec::new();
        while let Some(instance) = walk.next_instance().transpose()? {
            let mut edges = Vec::new();
            while let Some((_, edge)) = walk.next_edge().transpose()? {
                edges.push(edge);
            }
            visited.push((instance, edges));
        }
        Ok(visited)
    }

    fn error<T>() -> Item<T> {
        Err(Box::new(ConceptReadError::OrderingValueMissing {}))
    }

    #[test]
    fn instances_are_visited_with_their_edges_including_those_without_edges() {
        let visited = walk(vec![Ok(1), Ok(2), Ok(3), Ok(4)], vec![Ok((1, 'a')), Ok((1, 'b')), Ok((3, 'c'))]).unwrap();
        assert_eq!(visited, vec![(1, vec!['a', 'b']), (2, vec![]), (3, vec!['c']), (4, vec![])]);
    }

    #[test]
    fn edges_of_instances_not_returned_are_skipped() {
        let edges = vec![Ok((0, 'a')), Ok((1, 'b')), Ok((2, 'c')), Ok((3, 'd')), Ok((5, 'e'))];
        let visited = walk(vec![Ok(1), Ok(3), Ok(4)], edges).unwrap();
        assert_eq!(visited, vec![(1, vec!['b']), (3, vec!['d']), (4, vec![])]);
    }

    #[test]
    fn unread_edges_are_skipped_when_moving_to_the_next_instance() {
        let edges = vec![Ok((1, 'a')), Ok((1, 'b')), Ok((2, 'c'))];
        let mut walk =
            InstanceEdgeWalk::new(vec![Ok(1), Ok(2)].into_iter(), edges.into_iter(), |&(i, _): &(u64, char)| i);
        assert_eq!(walk.next_instance().unwrap().unwrap(), 1);
        assert_eq!(walk.next_edge().unwrap().unwrap(), (1, 'a'));
        assert_eq!(walk.next_instance().unwrap().unwrap(), 2);
        assert_eq!(walk.next_edge().unwrap().unwrap(), (2, 'c'));
        assert!(walk.next_edge().is_none());
        assert!(walk.next_instance().is_none());
    }

    #[test]
    fn errors_are_returned() {
        assert!(walk(vec![Ok(1)], vec![error()]).is_err());
        assert!(walk(vec![Ok(1), error()], vec![]).is_err());
    }

    #[test]
    fn an_error_in_unread_edges_is_returned_when_moving_to_the_next_instance() {
        let edges = vec![Ok((1, 'a')), error()];
        let mut walk =
            InstanceEdgeWalk::new(vec![Ok(1), Ok(2)].into_iter(), edges.into_iter(), |&(i, _): &(u64, char)| i);
        assert_eq!(walk.next_instance().unwrap().unwrap(), 1);
        assert!(walk.next_instance().unwrap().is_err());
    }
}
