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
    iter,
    sync::{Arc, OnceLock},
};
use std::io::Read;

use bytes::Bytes;
use encoding::{
    graph::{
        thing::{edge::ThingEdgeHasReverse, vertex_attribute::AttributeVertex},
        type_::vertex::{PrefixedTypeVertexEncoding, TypeVertexEncoding},
        Typed,
    },
    layout::prefix::Prefix,
    value::{decode_value_u64, value::Value, value_type::ValueType},
    AsBytes, Keyable,
};
use iterator::State;
use itertools::Itertools;
use encoding::graph::thing::edge::ThingEdgeHas;
use lending_iterator::{higher_order::Hkt, LendingIterator};
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::{
    key_value::StorageKey,
    snapshot::{buffer::BufferRangeIterator, iterator::SnapshotRangeIterator, ReadableSnapshot, WritableSnapshot},
};
use storage::key_range::{KeyRange, RangeEnd, RangeStart};
use storage::key_value::StorageKeyArray;
use storage::snapshot::write::{Write, WriteCategory};

use crate::{
    edge_iterator,
    error::{ConceptReadError, ConceptWriteError},
    thing::{object::Object, thing_manager::ThingManager, HKInstance, ThingAPI},
    type_::{attribute_type::AttributeType, ObjectTypeAPI, TypeAPI},
    ConceptAPI, ConceptStatus,
};

#[derive(Debug, Clone)]
pub struct Attribute {
    vertex: AttributeVertex,
    value: OnceLock<Arc<Value<'static>>>,
}

impl Attribute {
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
    ) -> Result<Value<'_>, Box<ConceptReadError>> {
        if self.value.get().is_none() {
            let value = thing_manager.get_attribute_value(snapshot, self)?;
            let _ = self.value.set(Arc::new(value));
        }
        Ok(self.value.get().unwrap().as_reference())
    }

    pub fn has_owners(&self, snapshot: &impl ReadableSnapshot, thing_manager: &ThingManager) -> bool {
        match self.get_status(snapshot, thing_manager) {
            ConceptStatus::Put | ConceptStatus::Persisted => thing_manager.has_owners(snapshot, self, false),
            ConceptStatus::Inserted | ConceptStatus::Deleted => {
                unreachable!("Attributes are expected to always have a PUT status.")
            }
        }
    }

    pub fn get_owners(&self, snapshot: &impl ReadableSnapshot, thing_manager: &ThingManager) -> AttributeOwnerIterator {
        thing_manager.get_owners(snapshot, self)
    }

    pub fn get_owners_by_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner_type: impl ObjectTypeAPI,
    ) -> AttributeOwnerIterator {
        thing_manager.get_owners_by_type(snapshot, self, owner_type)
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
    const PREFIX_RANGE_INCLUSIVE: (Prefix, Prefix) = (Prefix::VertexAttribute, Prefix::VertexAttribute);

    fn new(vertex: Self::Vertex) -> Self {
        Attribute { vertex, value: OnceLock::new() }
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

    fn get_status(&self, snapshot: &impl ReadableSnapshot, thing_manager: &ThingManager) -> ConceptStatus {
        thing_manager.get_status(snapshot, self.vertex().into_storage_key())
    }

    fn delete(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<(), Box<ConceptWriteError>> {
        for object in self.get_owners(snapshot, thing_manager).map_ok(|(key, _)| key) {
            // TODO: This will not work for ordered owns: it unsets 1 under the hood. Write tests with lists when implemented.
            thing_manager.unset_has(snapshot, object?, &self)?;
        }
        thing_manager.delete_attribute(snapshot, self)?;

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
    AllAttributesIterator: Iterator<Item = Result<Attribute, Box<ConceptReadError>>>,
{
    independent_attribute_types: Arc<HashSet<AttributeType>>,
    attributes_iterator: Option<iter::Peekable<AllAttributesIterator>>,
    has_reverse_iterator_buffer: Option<BufferRangeIterator>,
    has_reverse_iterator_storage: Option<SnapshotRangeIterator>,
    state: State<Box<ConceptReadError>>,
}

impl<AllAttributesIterator> AttributeIterator<AllAttributesIterator>
where
    AllAttributesIterator: Iterator<Item = Result<Attribute, Box<ConceptReadError>>>,
{
    pub(crate) fn new(
        attributes_iterator: AllAttributesIterator,
        has_reverse_iterator_buffer: BufferRangeIterator,
        has_reverse_iterator_storage: SnapshotRangeIterator,
        independent_attribute_types: Arc<HashSet<AttributeType>>,
    ) -> Self {
        Self {
            independent_attribute_types,
            attributes_iterator: Some(attributes_iterator.peekable()),
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

    #[cfg(unused_unimplemented_function)]
    pub fn seek(&mut self) {
        ensure_unimplemented_unused!()
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
                    println!("Checking attribute: {}", attribute);
                    let attribute_vertex = attribute.vertex();
                    let independent = self.independent_attribute_types.contains(&attribute.type_());
                    if independent {
                        println!("INDEPENDENT: READY");
                        self.state = State::ItemReady;
                    } else {
                        println!("NOT INDEPENDENT");
                        match Self::write_category(self.has_reverse_iterator_buffer.as_mut().unwrap(), attribute_vertex)
                        {
                            Ok(Some(write_category)) => {
                                if write_category.is_new() {
                                    println!("HAS WRITES: NEW");
                                    self.state = State::ItemReady
                                } else if write_category.is_delete() {
                                    println!("HAS WRITES: DELETE");
                                    advance_attribute = true
                                } else {
                                    unreachable!("Not new and not delete write category");
                                }
                            }
                            Ok(None) => {
                                println!("DOES NOT HAVE WRITES");
                                match Self::has_owner(
                                    self.has_reverse_iterator_storage.as_mut().unwrap(),
                                    attribute_vertex,
                                ) {
                                    Ok(has_owner) => {
                                        if has_owner {
                                            println!("HAS OWNER!");
                                            self.state = State::ItemReady
                                        } else {
                                            println!("DOES NOT HAVE OWNER");
                                            advance_attribute = true
                                        }
                                    }
                                    Err(err) => self.state = State::Error(err),
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
        attribute_vertex: AttributeVertex,
    ) -> Result<bool, Box<ConceptReadError>> {
        let has_reverse_prefix = ThingEdgeHasReverse::prefix_from_attribute(attribute_vertex);
        has_reverse_iterator.seek(has_reverse_prefix.as_reference());
        match has_reverse_iterator.peek().transpose().map_err(|source| Box::new(ConceptReadError::SnapshotIterate { source }))? {
            None => Ok(false),
            Some((bytes, _)) => {
                let edge = ThingEdgeHasReverse::decode(Bytes::Reference(bytes.bytes()));
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

    fn write_category(
        has_reverse_iterator: &mut BufferRangeIterator,
        attribute_vertex: AttributeVertex,
    ) -> Result<Option<WriteCategory>, Box<ConceptReadError>> {
        let has_reverse_prefix = ThingEdgeHasReverse::prefix_from_attribute(attribute_vertex);
        has_reverse_iterator.seek(has_reverse_prefix.bytes());
        if let Some((key, write)) = has_reverse_iterator.peek() {
            let edge = ThingEdgeHasReverse::decode(Bytes::Reference(key.byte_array()));
            if edge.from() == attribute_vertex {
                return Ok(Some(write.category()));
            }
        }
        Ok(None)
    }
}

impl<I> Iterator for AttributeIterator<I>
where
    I: Iterator<Item = Result<Attribute, Box<ConceptReadError>>>,
{
    type Item = Result<Attribute, Box<ConceptReadError>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter_next()
    }
}

fn storage_key_to_owner<'a>(
    storage_key: StorageKey<'a, BUFFER_KEY_INLINE>,
    value: Bytes<'a, BUFFER_VALUE_INLINE>,
) -> (Object, u64) {
    let edge = ThingEdgeHasReverse::decode(storage_key.into_bytes());
    (Object::new(edge.to()), decode_value_u64(&value))
}

edge_iterator!(
    AttributeOwnerIterator;
    (Object, u64);
    storage_key_to_owner
);

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
