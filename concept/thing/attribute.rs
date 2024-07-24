/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::HashSet,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::Arc,
};

use bytes::{byte_array::ByteArray, Bytes};
use encoding::{
    graph::{
        thing::{edge::ThingEdgeHasReverse, vertex_attribute::AttributeVertex, vertex_object::ObjectVertex},
        type_::vertex::PrefixedTypeVertexEncoding,
        Typed,
    },
    value::{
        decode_value_u64,
        value::Value,
        value_struct::{StructIndexEntry, StructIndexEntryKey},
    },
    AsBytes, Keyable,
};
use iterator::State;
use lending_iterator::LendingIterator;
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::{
    key_value::{StorageKey, StorageKeyReference},
    snapshot::{iterator::SnapshotRangeIterator, ReadableSnapshot, WritableSnapshot},
};

use crate::{
    edge_iterator,
    error::{ConceptReadError, ConceptWriteError},
    thing::{object::Object, thing_manager::ThingManager, ThingAPI},
    type_::{attribute_type::AttributeType, ObjectTypeAPI, TypeAPI},
    ByteReference, ConceptAPI, ConceptStatus,
};

#[derive(Debug, Clone)]
pub struct Attribute<'a> {
    vertex: AttributeVertex<'a>,
    value: Option<Arc<Value<'a>>>, // TODO: if we end up doing traversals over Vertex instead of Concept, we could embed the Value cache into the AttributeVertex
}

impl<'a> Attribute<'a> {
    pub(crate) fn new(vertex: AttributeVertex<'a>) -> Self {
        Attribute { vertex, value: None }
    }

    pub fn type_(&self) -> AttributeType<'static> {
        AttributeType::build_from_type_id(self.vertex.type_id_())
    }

    pub fn iid(&self) -> ByteReference<'_> {
        self.vertex.bytes()
    }

    pub fn get_value(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Value<'_>, ConceptReadError> {
        if self.value.is_none() {
            let value = thing_manager.get_attribute_value(snapshot, self)?;
            self.value = Some(Arc::new(value));
        }
        Ok(self.value.as_ref().unwrap().as_reference())
    }

    pub fn has_owners(&self, snapshot: &impl ReadableSnapshot, thing_manager: &ThingManager) -> bool {
        match self.get_status(snapshot, thing_manager) {
            ConceptStatus::Put | ConceptStatus::Persisted => {
                thing_manager.has_owners(snapshot, self.as_reference(), false)
            }
            ConceptStatus::Inserted | ConceptStatus::Deleted => {
                unreachable!("Attributes are expected to always have a PUT status.")
            }
        }
    }

    pub fn get_owners<'m>(
        &self,
        snapshot: &'m impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
    ) -> AttributeOwnerIterator {
        thing_manager.get_owners(snapshot, self.as_reference())
    }

    pub fn get_owners_by_type<'m, 'o>(
        &self,
        snapshot: &'m impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
        owner_type: impl ObjectTypeAPI<'o>,
    ) -> AttributeOwnerIterator {
        thing_manager.get_owners_by_type(snapshot, self.as_reference(), owner_type)
    }

    pub fn as_reference(&self) -> Attribute<'_> {
        Attribute { vertex: self.vertex.as_reference(), value: self.value.clone() }
    }

    pub(crate) fn vertex<'this: 'a>(&'this self) -> AttributeVertex<'this> {
        self.vertex.as_reference()
    }

    pub fn next_possible(&self) -> Attribute<'static> {
        let mut bytes = ByteArray::from(self.vertex.bytes());
        bytes.increment().unwrap();
        Attribute::new(AttributeVertex::new(Bytes::Array(bytes)))
    }

    pub(crate) fn into_vertex(self) -> AttributeVertex<'a> {
        self.vertex
    }

    pub fn into_owned(self) -> Attribute<'static> {
        Attribute::new(self.vertex.into_owned())
    }
}

impl<'a> ConceptAPI<'a> for Attribute<'a> {}

impl<'a> ThingAPI<'a> for Attribute<'a> {
    type VertexType<'b> = AttributeVertex<'b>;

    fn set_modified(&self, snapshot: &mut impl WritableSnapshot, thing_manager: &ThingManager) {
        debug_assert_eq!(thing_manager.get_status(snapshot, self.vertex().as_storage_key()), ConceptStatus::Put);
        // Attributes are always PUT, so we don't have to record a lock on modification
    }

    fn get_status(&self, snapshot: &impl ReadableSnapshot, thing_manager: &ThingManager) -> ConceptStatus {
        thing_manager.get_status(snapshot, self.vertex().as_storage_key())
    }

    fn errors(
        &self,
        _snapshot: &impl WritableSnapshot,
        _thing_manager: &ThingManager,
    ) -> Result<Vec<ConceptWriteError>, ConceptReadError> {
        Ok(Vec::new())
    }

    fn delete(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptWriteError> {
        let owners = self
            .get_owners(snapshot, thing_manager)
            .map_static(|res| res.map(|(key, _)| key.into_owned()))
            .try_collect::<Vec<_>, _>()
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        for object in owners {
            thing_manager.unset_has(snapshot, &object, self.as_reference());
        }
        thing_manager.delete_attribute(snapshot, self)?;

        Ok(())
    }
}

impl<'a> PartialEq<Self> for Attribute<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.vertex().eq(&other.vertex())
    }
}

impl<'a> Eq for Attribute<'a> {}

impl<'a> PartialOrd<Self> for Attribute<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for Attribute<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.vertex.cmp(&other.vertex())
    }
}

/// Attribute iterators handle hiding dependent attributes that were not deleted yet
trait KeyAttributeExtractor {
    fn storage_key_to_attribute_vertex<'bytes>(
        storage_key: StorageKey<'bytes, BUFFER_KEY_INLINE>,
    ) -> AttributeVertex<'bytes>;
}

pub struct StandardAttributeExtractor;

impl KeyAttributeExtractor for StandardAttributeExtractor {
    fn storage_key_to_attribute_vertex<'bytes>(
        storage_key: StorageKey<'bytes, BUFFER_KEY_INLINE>,
    ) -> AttributeVertex<'bytes> {
        AttributeVertex::new(storage_key.into_bytes())
    }
}

pub struct StructIndexAttributeExtractor;

impl KeyAttributeExtractor for StructIndexAttributeExtractor {
    fn storage_key_to_attribute_vertex<'bytes>(
        storage_key: StorageKey<'bytes, BUFFER_KEY_INLINE>,
    ) -> AttributeVertex<'bytes> {
        StructIndexEntry::new(StructIndexEntryKey::new(Bytes::reference(storage_key.bytes())), None).attribute_vertex()
    }
}

pub struct AttributeIteratorImpl<AttributeExtractor: KeyAttributeExtractor> {
    independent_attribute_types: Arc<HashSet<AttributeType<'static>>>,
    attributes_iterator: Option<SnapshotRangeIterator>,
    has_reverse_iterator: Option<SnapshotRangeIterator>,
    state: State<ConceptReadError>,
    key_interpreter: PhantomData<AttributeExtractor>,
}

pub type AttributeIterator = AttributeIteratorImpl<StandardAttributeExtractor>;
pub type StructIndexToAttributeIterator = AttributeIteratorImpl<StructIndexAttributeExtractor>;

impl<KeyExtractor: KeyAttributeExtractor> AttributeIteratorImpl<KeyExtractor> {
    pub(crate) fn new(
        attributes_iterator: SnapshotRangeIterator,
        has_reverse_iterator: SnapshotRangeIterator,
        independent_attribute_types: Arc<HashSet<AttributeType<'static>>>,
    ) -> Self {
        Self {
            independent_attribute_types,
            attributes_iterator: Some(attributes_iterator),
            has_reverse_iterator: Some(has_reverse_iterator),
            state: State::Init,
            key_interpreter: PhantomData,
        }
    }

    pub(crate) fn new_empty() -> Self {
        Self {
            independent_attribute_types: Arc::new(HashSet::new()),
            attributes_iterator: None,
            has_reverse_iterator: None,
            state: State::Done,
            key_interpreter: PhantomData,
        }
    }

    pub fn peek(&mut self) -> Option<Result<Attribute<'_>, ConceptReadError>> {
        self.iter_peek().map(|result| {
            result.map(|(storage_key, _value_bytes)| {
                Attribute::new(KeyExtractor::storage_key_to_attribute_vertex(StorageKey::Reference(storage_key)))
            })
        })
    }

    pub fn seek(&mut self) {
        todo!()
    }

    fn iter_peek(&mut self) -> Option<Result<(StorageKeyReference<'_>, ByteReference<'_>), ConceptReadError>> {
        match &self.state {
            State::Init | State::ItemUsed => {
                self.find_next_state();
                self.iter_peek()
            }
            State::ItemReady => self
                .attributes_iterator
                .as_mut()
                .unwrap()
                .peek()
                .transpose()
                .map_err(|err| ConceptReadError::SnapshotIterate { source: err.clone() })
                .transpose(),
            State::Error(error) => Some(Err(error.clone())),
            State::Done => None,
        }
    }

    fn iter_next(
        &mut self,
    ) -> Option<Result<(StorageKey<'_, BUFFER_KEY_INLINE>, Bytes<'_, BUFFER_VALUE_INLINE>), ConceptReadError>> {
        match &self.state {
            State::Init | State::ItemUsed => {
                self.find_next_state();
                self.iter_next()
            }
            State::ItemReady => {
                let next = self
                    .attributes_iterator
                    .as_mut()
                    .unwrap()
                    .next()
                    .transpose()
                    .map_err(|err| ConceptReadError::SnapshotIterate { source: err.clone() })
                    .transpose();
                let _ = self.has_reverse_iterator.as_mut().unwrap().next();
                self.state = State::ItemUsed;
                next
            }
            State::Error(error) => Some(Err(error.clone())),
            State::Done => None,
        }
    }

    fn find_next_state(&mut self) {
        assert!(matches!(&self.state, State::Init | State::ItemUsed));
        while matches!(&self.state, State::Init | State::ItemUsed) {
            let mut advance_attribute = false;
            match self.attributes_iterator.as_mut().unwrap().peek() {
                None => self.state = State::Done,
                Some(Ok((key, _))) => {
                    let attribute_vertex = KeyExtractor::storage_key_to_attribute_vertex(StorageKey::Reference(key));
                    let independent = self
                        .independent_attribute_types
                        .contains(&Attribute::new(attribute_vertex.as_reference()).type_());
                    if independent {
                        self.state = State::ItemReady;
                    } else {
                        match Self::has_owner(self.has_reverse_iterator.as_mut().unwrap(), attribute_vertex) {
                            Ok(has_owner) => {
                                if has_owner {
                                    self.state = State::ItemReady
                                } else {
                                    advance_attribute = true
                                }
                            }
                            Err(err) => self.state = State::Error(err),
                        }
                    }
                }
                Some(Err(err)) => self.state = State::Error(ConceptReadError::SnapshotIterate { source: err.clone() }),
            }
            if advance_attribute {
                self.attributes_iterator.as_mut().unwrap().next();
            }
        }
    }

    fn has_owner(
        has_reverse_iterator: &mut SnapshotRangeIterator,
        attribute_vertex: AttributeVertex<'_>,
    ) -> Result<bool, ConceptReadError> {
        let has_reverse_prefix = ThingEdgeHasReverse::prefix_from_attribute(attribute_vertex.as_reference());
        has_reverse_iterator.seek(has_reverse_prefix.as_reference());
        match has_reverse_iterator.peek() {
            None => Ok(false),
            Some(Err(err)) => Err(ConceptReadError::SnapshotIterate { source: err.clone() }),
            Some(Ok((bytes, _))) => {
                let edge = ThingEdgeHasReverse::new(Bytes::Reference(bytes.byte_ref()));
                let edge_from = edge.from();
                match edge_from.cmp(&attribute_vertex) {
                    Ordering::Less => {
                        panic!("Unexpected attribute edge encountered for a previous attribute, which should not be possible.");
                    }
                    Ordering::Equal => Ok(true),
                    Ordering::Greater => Ok(false),
                }
            }
        }
    }
}

impl<KeyExtractor: KeyAttributeExtractor + 'static> LendingIterator for AttributeIteratorImpl<KeyExtractor> {
    type Item<'a> = Result<Attribute<'a>, ConceptReadError>;
    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iter_next().map(|result| {
            result.map(|(storage_key, _value_bytes)| {
                Attribute::new(KeyExtractor::storage_key_to_attribute_vertex(storage_key))
            })
        })
    }
}

fn storage_key_to_owner<'a>(
    storage_key: StorageKey<'a, BUFFER_KEY_INLINE>,
    value: Bytes<'a, BUFFER_VALUE_INLINE>,
) -> (Object<'a>, u64) {
    let edge = ThingEdgeHasReverse::new(storage_key.into_bytes());
    (Object::new(edge.into_to()), decode_value_u64(value.as_reference()))
}

edge_iterator!(
    AttributeOwnerIterator;
    'a -> (Object<'a>, u64);
    storage_key_to_owner
);

impl<'a> Display for Attribute<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[Attribute-{}:{}:{}]",
            self.vertex().value_type_category(),
            self.type_().vertex().type_id_(),
            self.vertex.attribute_id()
        )
    }
}

impl<'a> Hash for Attribute<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.vertex, state)
    }
}
