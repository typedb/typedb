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
    sync::{Arc, OnceLock},
};

use bytes::{byte_array::ByteArray, Bytes};
use encoding::{
    graph::{
        thing::{edge::ThingEdgeHasReverse, vertex_attribute::AttributeVertex, ThingVertex},
        type_::vertex::{PrefixedTypeVertexEncoding, TypeVertexEncoding},
        Typed,
    },
    layout::prefix::Prefix,
    value::{decode_value_u64, value::Value, value_type::ValueType},
    AsBytes, Keyable,
};
use iterator::State;
use lending_iterator::{higher_order::Hkt, LendingIterator, Peekable};
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::{
    key_value::StorageKey,
    snapshot::{buffer::BufferRangeIterator, iterator::SnapshotRangeIterator, ReadableSnapshot, WritableSnapshot},
};

use crate::{
    edge_iterator,
    error::{ConceptReadError, ConceptWriteError},
    thing::{object::Object, thing_manager::ThingManager, HKInstance, ThingAPI},
    type_::{attribute_type::AttributeType, ObjectTypeAPI},
    ByteReference, ConceptAPI, ConceptStatus,
};

#[derive(Debug, Clone)]
pub struct Attribute<'a> {
    vertex: AttributeVertex<'a>,
    value: OnceLock<Arc<Value<'static>>>,
}

impl<'a> Attribute<'a> {
    pub fn type_(&self) -> AttributeType<'static> {
        AttributeType::build_from_type_id(self.vertex.type_id_())
    }

    pub fn iid(&self) -> ByteReference<'_> {
        self.vertex.bytes()
    }

    pub fn get_value(
        &'a self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Value<'a>, ConceptReadError> {
        if self.value.get().is_none() {
            let value = thing_manager.get_attribute_value(snapshot, self)?;
            let _ = self.value.set(Arc::new(value));
        }
        Ok(self.value.get().unwrap().as_reference())
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

    pub fn get_owners(&self, snapshot: &impl ReadableSnapshot, thing_manager: &ThingManager) -> AttributeOwnerIterator {
        thing_manager.get_owners(snapshot, self.as_reference())
    }

    pub fn get_owners_by_type<'o>(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner_type: impl ObjectTypeAPI<'o>,
    ) -> AttributeOwnerIterator {
        thing_manager.get_owners_by_type(snapshot, self.as_reference(), owner_type)
    }

    pub fn next_possible(&self) -> Attribute<'static> {
        let mut bytes = ByteArray::from(self.vertex.bytes());
        bytes.increment().unwrap();
        Attribute::new(AttributeVertex::new(Bytes::Array(bytes)))
    }

    pub fn as_reference(&self) -> Attribute<'_> {
        Attribute { vertex: self.vertex.as_reference(), value: self.value.clone() }
    }

    pub fn into_owned(self) -> Attribute<'static> {
        Attribute::new(self.vertex.into_owned())
    }
}

impl<'a> ConceptAPI<'a> for Attribute<'a> {}

impl<'a> ThingAPI<'a> for Attribute<'a> {
    type Vertex<'b> = AttributeVertex<'b>;
    type TypeAPI<'b> = AttributeType<'b>;
    type Owned = Attribute<'static>;
    const PREFIX_RANGE: (Prefix, Prefix) = (Prefix::VertexAttribute, Prefix::VertexAttribute);

    fn new(vertex: Self::Vertex<'a>) -> Self {
        Attribute { vertex, value: OnceLock::new() }
    }

    fn vertex(&self) -> Self::Vertex<'_> {
        self.vertex.as_reference()
    }

    fn into_vertex(self) -> Self::Vertex<'a> {
        self.vertex
    }

    fn into_owned(self) -> Self::Owned {
        Attribute::new(self.vertex.into_owned())
    }

    fn iid(&self) -> ByteReference<'_> {
        self.vertex.bytes()
    }

    fn set_required(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptReadError> {
        match self.type_().get_value_type_without_source(snapshot, thing_manager.type_manager())? {
            Some(value_type) => match value_type {
                | ValueType::Boolean
                | ValueType::Long
                | ValueType::Double
                | ValueType::Decimal
                | ValueType::Date
                | ValueType::DateTime
                | ValueType::DateTimeTZ
                | ValueType::Duration => snapshot.put(self.vertex().as_storage_key().into_owned_array()),
                // ValueTypes with expensive writes
                | ValueType::String | ValueType::Struct(_) => {
                    thing_manager.lock_existing_attribute(snapshot, self.as_reference())
                }
            },
            None => panic!("Attribute instances must have a value type"),
        }
        Ok(())
    }

    fn get_status(&self, snapshot: &impl ReadableSnapshot, thing_manager: &ThingManager) -> ConceptStatus {
        thing_manager.get_status(snapshot, self.vertex().as_storage_key())
    }

    fn delete(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptWriteError> {
        let owners = self
            .get_owners(snapshot, thing_manager)
            .map_static(|res| res.map(|(key, _)| key.into_owned()))
            .try_collect::<Vec<_>, _>()?;
        for object in owners {
            thing_manager.unset_has(snapshot, &object, self.as_reference());
        }
        thing_manager.delete_attribute(snapshot, self)?;

        Ok(())
    }

    fn prefix_for_type(_type: Self::TypeAPI<'_>) -> Prefix {
        Prefix::VertexAttribute
    }
}

impl HKInstance for Attribute<'static> {}

impl Hkt for Attribute<'static> {
    type HktSelf<'a> = Attribute<'a>;
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

pub struct AttributeIterator<AllAttributesIterator>
where
    AllAttributesIterator: for<'a> LendingIterator<Item<'a> = Result<Attribute<'a>, ConceptReadError>>,
{
    independent_attribute_types: Arc<HashSet<AttributeType<'static>>>,
    attributes_iterator: Option<Peekable<AllAttributesIterator>>,
    has_reverse_iterator_buffer: Option<BufferRangeIterator>,
    has_reverse_iterator_storage: Option<SnapshotRangeIterator>,
    state: State<ConceptReadError>,
}

impl<AllAttributesIterator> AttributeIterator<AllAttributesIterator>
where
    AllAttributesIterator: for<'a> LendingIterator<Item<'a> = Result<Attribute<'a>, ConceptReadError>>,
{
    pub(crate) fn new(
        attributes_iterator: AllAttributesIterator,
        has_reverse_iterator_buffer: BufferRangeIterator,
        has_reverse_iterator_storage: SnapshotRangeIterator,
        independent_attribute_types: Arc<HashSet<AttributeType<'static>>>,
    ) -> Self {
        Self {
            independent_attribute_types,
            attributes_iterator: Some(Peekable::new(attributes_iterator)),
            has_reverse_iterator_buffer: Some(has_reverse_iterator_buffer),
            has_reverse_iterator_storage: Some(has_reverse_iterator_storage),
            state: State::Init,
        }
    }

    pub(crate) fn new_empty() -> Self {
        Self {
            independent_attribute_types: Arc::new(HashSet::new()),
            attributes_iterator: None,
            has_reverse_iterator_buffer: None,
            has_reverse_iterator_storage: None,
            state: State::Done,
        }
    }

    pub fn seek(&mut self) {
        todo!()
    }

    fn iter_next(&mut self) -> Option<Result<Attribute<'_>, ConceptReadError>> {
        match &self.state {
            State::Init | State::ItemUsed => {
                self.find_next_state();
                self.iter_next()
            }
            State::ItemReady => {
                let next = self.attributes_iterator.as_mut().unwrap().next();
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
                Some(Ok(attribute)) => {
                    let attribute_vertex = attribute.vertex();
                    let independent = self.independent_attribute_types.contains(&attribute.type_());
                    if independent {
                        self.state = State::ItemReady;
                    } else {
                        match Self::has_any_writes(
                            self.has_reverse_iterator_buffer.as_mut().unwrap(),
                            attribute_vertex.clone(),
                        ) {
                            Ok(has_writes) => {
                                if has_writes {
                                    self.state = State::ItemReady
                                } else {
                                    match Self::has_owner(
                                        self.has_reverse_iterator_storage.as_mut().unwrap(),
                                        attribute_vertex,
                                    ) {
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
                            Err(err) => self.state = State::Error(err),
                        }
                    }
                }
                Some(Err(err)) => self.state = State::Error(err.clone()),
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

    fn has_any_writes(
        has_reverse_iterator: &mut BufferRangeIterator,
        attribute_vertex: AttributeVertex<'_>,
    ) -> Result<bool, ConceptReadError> {
        let has_reverse_prefix = ThingEdgeHasReverse::prefix_from_attribute(attribute_vertex.as_reference());
        has_reverse_iterator.seek(has_reverse_prefix.bytes());
        Ok(has_reverse_iterator.peek().is_some())
    }
}

impl<Iterator> LendingIterator for AttributeIterator<Iterator>
where
    Iterator: for<'a> LendingIterator<Item<'a> = Result<Attribute<'a>, ConceptReadError>>,
{
    type Item<'a> = Result<Attribute<'a>, ConceptReadError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iter_next()
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
