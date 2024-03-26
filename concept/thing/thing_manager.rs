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

use std::rc::Rc;

use bytes::Bytes;
use encoding::{
    graph::{
        thing::{
            vertex_attribute::AttributeVertex,
            vertex_generator::{StringAttributeID, ThingVertexGenerator},
            vertex_object::ObjectVertex,
        },
        Typed,
    },
    Keyable,
    layout::prefix::{PrefixID, PrefixType},
    value::{long::Long, string::StringBytes, value_type::ValueType},
};
use primitive::prefix_range::PrefixRange;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::snapshot::snapshot::Snapshot;

use crate::{
    error::{ConceptError, ConceptErrorKind},
    thing::{
        attribute::{Attribute, AttributeIterator},
        AttributeAPI,
        entity::{Entity, EntityIterator},
        value::Value,
    },
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, type_manager::TypeManager, TypeAPI,
    },
};

pub struct ThingManager<'txn, 'storage: 'txn, D> {
    snapshot: Rc<Snapshot<'storage, D>>,
    vertex_generator: &'txn ThingVertexGenerator,
    type_manager: Rc<TypeManager<'txn, 'storage, D>>,
}

impl<'txn, 'storage: 'txn, D> ThingManager<'txn, 'storage, D> {
    pub fn new(
        snapshot: Rc<Snapshot<'storage, D>>,
        vertex_generator: &'txn ThingVertexGenerator,
        type_manager: Rc<TypeManager<'txn, 'storage, D>>,
    ) -> Self {
        ThingManager { snapshot, vertex_generator, type_manager }
    }

    pub fn create_entity(&self, entity_type: &EntityType<'_>) -> Entity<'_> {
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            Entity::new(self.vertex_generator.create_entity(Typed::type_id(entity_type.vertex()), write_snapshot))
        } else {
            panic!("Illegal state: create entity requires write snapshot")
        }
    }

    pub fn create_attribute(
        &self,
        attribute_type: &AttributeType<'_>,
        value: Value,
    ) -> Result<Attribute<'_>, ConceptError> {
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let value_type = attribute_type.get_value_type(self.type_manager.as_ref());
            if Some(value.value_type()) == value_type {
                let vertex = match value {
                    Value::Boolean(_bool) => {
                        todo!()
                    }
                    Value::Long(long) => {
                        let encoded_long = Long::build(long);
                        self.vertex_generator.create_attribute_long(
                            Typed::type_id(attribute_type.vertex()),
                            encoded_long,
                            write_snapshot,
                        )
                    }
                    Value::Double(_double) => {
                        todo!()
                    }
                    Value::String(string) => {
                        let encoded_string: StringBytes<'_, BUFFER_KEY_INLINE> = StringBytes::build_ref(&string);
                        self.vertex_generator.create_attribute_string(
                            Typed::type_id(attribute_type.vertex()),
                            encoded_string,
                            write_snapshot,
                        )
                    }
                };
                Ok(Attribute::new(vertex))
            } else {
                Err(ConceptError {
                    kind: ConceptErrorKind::AttributeValueTypeMismatch {
                        attribute_type_value_type: value_type,
                        provided_value_type: value.value_type(),
                    },
                })
            }
        } else {
            panic!("Illegal state: create entity requires write snapshot")
        }
    }

    pub fn get_entities(&self) -> EntityIterator<'_, 1, D> {
        let prefix = ObjectVertex::build_prefix_prefix(PrefixType::VertexEntity.prefix_id());
        let snapshot_iterator = self.snapshot.iterate_range(PrefixRange::new_within(prefix));
        EntityIterator::new(snapshot_iterator)
    }

    pub fn get_attributes(&self) -> AttributeIterator<'_, 1, D> {
        let start = AttributeVertex::build_prefix_prefix(PrefixID::VERTEX_ATTRIBUTE_MIN);
        let end = AttributeVertex::build_prefix_prefix(PrefixID::VERTEX_ATTRIBUTE_MAX);
        let snapshot_iterator = self.snapshot.iterate_range(PrefixRange::new_inclusive(start, end));
        AttributeIterator::new(snapshot_iterator)
    }

    pub fn get_attributes_in(&self, attribute_type: AttributeType<'_>) -> AttributeIterator<'_, 3, D> {
        attribute_type
            .get_value_type(self.type_manager.as_ref())
            .map(|value_type| {
                let prefix = AttributeVertex::build_prefix_type(value_type, Typed::type_id(attribute_type.vertex()));
                let snapshot_iterator = self.snapshot.iterate_range(PrefixRange::new_within(prefix));
                AttributeIterator::new(snapshot_iterator)
            })
            .unwrap_or_else(|| AttributeIterator::new_empty())
    }

    pub(crate) fn get_attribute_value(&self, attribute: &Attribute<'_>) -> Value {
        match attribute.value_type() {
            ValueType::Boolean => {
                todo!()
            }
            ValueType::Long => {
                let attribute_id = attribute.vertex().attribute_id().unwrap_bytes_8();
                Value::Long(Long::new(attribute_id.bytes()).as_i64())
            }
            ValueType::Double => {
                todo!()
            }
            ValueType::String => {
                let attribute_id = StringAttributeID::new(attribute.vertex().attribute_id().unwrap_bytes_16());
                if attribute_id.is_inline() {
                    Value::String(String::from(attribute_id.get_inline_string_bytes().as_str()).into_boxed_str())
                } else {
                    self.snapshot
                        .get_mapped(attribute.vertex().as_storage_key().as_reference(), |bytes| {
                            Value::String(
                                String::from(StringBytes::new(Bytes::<1>::Reference(bytes)).as_str())
                                    .into_boxed_str(),
                            )
                        })
                        .unwrap()
                }
            }
        }
    }
}
