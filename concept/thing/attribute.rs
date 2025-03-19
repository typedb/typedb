/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::HashSet,
    fmt,
    hash::{Hash, Hasher},
    sync::{Arc, OnceLock},
};

use bytes::Bytes;
use encoding::{
    graph::{
        thing::{
            edge::ThingEdgeHasReverse,
            vertex_attribute::{AttributeID, AttributeVertex},
            vertex_object::ObjectVertex,
        },
        type_::vertex::{PrefixedTypeVertexEncoding, TypeID, TypeVertexEncoding},
        Typed,
    },
    layout::prefix::Prefix,
    value::{value::Value, value_type::ValueType},
    AsBytes, Keyable,
};
use iterator::State;
use itertools::Itertools;
use lending_iterator::{higher_order::Hkt, LendingIterator, Peekable, Seekable};
use resource::{constants::snapshot::BUFFER_KEY_INLINE, profile::StorageCounters};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::{
        has::Has,
        object::{HasReverseIterator, Object},
        thing_manager::ThingManager,
        HKInstance, ThingAPI,
    },
    type_::{attribute_type::AttributeType, ObjectTypeAPI},
    ConceptAPI, ConceptStatus,
};

#[derive(Debug, Clone)]
pub struct Attribute {
    vertex: AttributeVertex,
    value: OnceLock<Arc<Value<'static>>>,
}

pub static MIN_STATIC: Attribute = Attribute::MIN;
impl Attribute {
    const fn new_const(vertex: AttributeVertex) -> Self {
        Self { vertex, value: OnceLock::new() }
    }

    pub fn type_(&self) -> AttributeType {
        AttributeType::build_from_type_id(self.vertex.type_id_())
    }

    pub fn iid(&self) -> Bytes<'_, BUFFER_KEY_INLINE> {
        self.vertex.to_bytes()
    }

    pub fn get_value(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        storage_counters: StorageCounters,
    ) -> Result<Value<'_>, Box<ConceptReadError>> {
        if self.value.get().is_none() {
            let value = thing_manager.get_attribute_value(snapshot, self, storage_counters)?;
            let _ = self.value.set(Arc::new(value));
        }
        Ok(self.value.get().unwrap().as_reference())
    }

    pub fn has_owners(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        storage_counters: StorageCounters,
    ) -> bool {
        match self.get_status(snapshot, thing_manager, storage_counters) {
            ConceptStatus::Put | ConceptStatus::Persisted => thing_manager.has_owners(snapshot, self, false),
            ConceptStatus::Inserted | ConceptStatus::Deleted => {
                unreachable!("Attributes are expected to always have a PUT status.")
            }
        }
    }

    pub fn get_owners(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        storage_counters: StorageCounters,
    ) -> impl Iterator<Item = Result<(Object, u64), Box<ConceptReadError>>> {
        thing_manager.get_owners(snapshot, self, storage_counters)
    }

    pub fn get_owners_by_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner_type: impl ObjectTypeAPI,
        storage_counters: StorageCounters,
    ) -> impl Iterator<Item = Result<(Object, u64), Box<ConceptReadError>>> {
        thing_manager.get_owners_by_type(snapshot, self, owner_type, storage_counters)
    }

    pub fn next_possible(&self) -> Attribute {
        let mut bytes = self.vertex.to_bytes().into_array();
        bytes.increment().unwrap();
        Attribute::new(AttributeVertex::decode(&bytes))
    }
}

impl ConceptAPI for Attribute {}

impl ThingAPI for Attribute {
    type Vertex = AttributeVertex;
    type TypeAPI = AttributeType;
    const MIN: Attribute = Attribute::new_const(Self::Vertex::new(TypeID::MIN, AttributeID::MIN));
    const PREFIX_RANGE_INCLUSIVE: (Prefix, Prefix) = (Prefix::VertexAttribute, Prefix::VertexAttribute);

    fn new(vertex: Self::Vertex) -> Self {
        Self::new_const(vertex)
    }

    fn vertex(&self) -> Self::Vertex {
        self.vertex
    }

    fn iid(&self) -> Bytes<'_, BUFFER_KEY_INLINE> {
        self.vertex.to_bytes()
    }

    fn set_required(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        _storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        match self.type_().get_value_type_without_source(snapshot, thing_manager.type_manager())? {
            Some(value_type) => match value_type {
                | ValueType::Boolean
                | ValueType::Integer
                | ValueType::Double
                | ValueType::Decimal
                | ValueType::Date
                | ValueType::DateTime
                | ValueType::DateTimeTZ
                | ValueType::Duration => snapshot.put(self.vertex().into_storage_key().into_owned_array()),
                // ValueTypes with expensive writes
                | ValueType::String | ValueType::Struct(_) => thing_manager.lock_existing_attribute(snapshot, self),
            },
            None => panic!("Attribute instances must have a value type"),
        }
        Ok(())
    }

    fn get_status(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        storage_counters: StorageCounters,
    ) -> ConceptStatus {
        thing_manager.get_status(snapshot, self.vertex().into_storage_key(), storage_counters)
    }

    fn delete(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        for object in self.get_owners(snapshot, thing_manager, StorageCounters::DISABLED).map_ok(|(key, _)| key) {
            thing_manager.unset_has(snapshot, object?, &self, storage_counters.clone())?;
        }
        thing_manager.delete_attribute(snapshot, self, storage_counters)?;

        Ok(())
    }

    fn prefix_for_type(_type: Self::TypeAPI) -> Prefix {
        Prefix::VertexAttribute
    }
}

impl HKInstance for Attribute {}

impl Hkt for Attribute {
    type HktSelf<'a> = Attribute;
}

impl PartialEq<Self> for Attribute {
    fn eq(&self, other: &Self) -> bool {
        self.vertex().eq(&other.vertex())
    }
}

impl Eq for Attribute {}

impl PartialOrd<Self> for Attribute {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Attribute {
    fn cmp(&self, other: &Self) -> Ordering {
        self.vertex.cmp(&other.vertex())
    }
}

pub struct AttributeIterator<AllAttributesIterator>
where
    AllAttributesIterator:
        Seekable<Attribute> + for<'a> LendingIterator<Item<'a> = Result<Attribute, Box<ConceptReadError>>>,
{
    independent_attribute_types: Arc<HashSet<AttributeType>>,
    attributes_iterator: Option<Peekable<AllAttributesIterator>>,
    has_reverse_iterator: Option<HasReverseIterator>,
    state: State<Box<ConceptReadError>>,
}

impl<AllAttributesIterator> AttributeIterator<AllAttributesIterator>
where
    AllAttributesIterator:
        Seekable<Attribute> + for<'a> LendingIterator<Item<'a> = Result<Attribute, Box<ConceptReadError>>>,
{
    pub(crate) fn new(
        attributes_iterator: AllAttributesIterator,
        has_reverse_iterator: HasReverseIterator,
        independent_attribute_types: Arc<HashSet<AttributeType>>,
    ) -> Self {
        Self {
            independent_attribute_types,
            attributes_iterator: Some(Peekable::new(attributes_iterator)),
            has_reverse_iterator: Some(has_reverse_iterator),
            state: State::Init,
        }
    }

    pub(crate) fn new_empty() -> Self {
        Self {
            independent_attribute_types: Arc::new(HashSet::new()),
            attributes_iterator: None,
            has_reverse_iterator: None,
            state: State::Done,
        }
    }

    pub fn seek(&mut self, target: &Attribute) {
        todo!()
    }

    pub fn peek(&mut self) -> Option<Result<Attribute, Box<ConceptReadError>>> {
        match &self.state {
            State::Init | State::ItemUsed => {
                self.find_next_state();
                self.peek()
            }
            State::ItemReady => self.attributes_iterator.as_mut().unwrap().peek().cloned(),
            State::Error(error) => Some(Err(error.clone())),
            State::Done => None,
        }
    }

    fn iter_next(&mut self) -> Option<Result<Attribute, Box<ConceptReadError>>> {
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
                    let independent = self.independent_attribute_types.contains(&attribute.type_());
                    if independent {
                        self.state = State::ItemReady;
                    } else {
                        match Self::has_owner(self.has_reverse_iterator.as_mut().unwrap(), attribute.vertex()) {
                            Ok(true) => self.state = State::ItemReady,
                            Ok(false) => advance_attribute = true,
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
        has_reverse_iterator: &mut HasReverseIterator,
        attribute_vertex: AttributeVertex,
    ) -> Result<bool, Box<ConceptReadError>> {
        let target_has = Has::EdgeReverse(ThingEdgeHasReverse::new(attribute_vertex, ObjectVertex::MIN));
        has_reverse_iterator.seek(&(target_has, 0));
        match has_reverse_iterator.peek() {
            None => Ok(false),
            Some(Err(err)) => Err(err),
            Some(Ok((found_has, _))) => {
                match found_has.attribute().vertex.cmp(&attribute_vertex) {
                    Ordering::Less => {
                        unreachable!("Unexpected attribute edge encountered for a previous attribute, which should not be possible.");
                    }
                    Ordering::Equal => Ok(true),
                    Ordering::Greater => Ok(false),
                }
            }
        }
    }
}

impl<I> Iterator for AttributeIterator<I>
where
    I: Seekable<Attribute> + for<'a> LendingIterator<Item<'a> = Result<Attribute, Box<ConceptReadError>>>,
{
    type Item = Result<Attribute, Box<ConceptReadError>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter_next()
    }
}

impl fmt::Display for Attribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[Attribute-{}:{}:{}]",
            self.vertex().value_type_category(),
            self.type_().vertex().type_id_(),
            self.vertex.attribute_id()
        )
    }
}

impl Hash for Attribute {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.vertex, state)
    }
}
