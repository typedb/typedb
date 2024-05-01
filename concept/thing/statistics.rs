/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use std::hash::Hash;
use std::ops::{Add, AddAssign};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeStruct;

use durability::SequenceNumber;
use encoding::graph::type_::vertex::{build_vertex_attribute_type, build_vertex_entity_type, build_vertex_relation_type, build_vertex_role_type, TypeID, TypeIDUInt};
use encoding::graph::Typed;

use crate::type_::attribute_type::AttributeType;
use crate::type_::entity_type::EntityType;
use crate::type_::object_type::ObjectType;
use crate::type_::relation_type::RelationType;
use crate::type_::role_type::RoleType;
use crate::type_::TypeAPI;

#[derive(Debug)]
pub struct Statistics {
    open_sequence_number: SequenceNumber,

    total_thing_count: u64,
    total_entity_count: u64,
    total_relation_count: u64,
    total_attribute_count: u64,
    total_role_count: u64,
    total_has_count: u64,

    entity_counts: HashMap<EntityType<'static>, u64>,
    relation_counts: HashMap<RelationType<'static>, u64>,
    attribute_counts: HashMap<AttributeType<'static>, u64>,
    role_counts: HashMap<RoleType<'static>, u64>,

    has_attribute_counts: HashMap<ObjectType<'static>, HashMap<AttributeType<'static>, u64>>,
    attribute_owner_counts: HashMap<AttributeType<'static>, HashMap<ObjectType<'static>, u64>>,
    role_player_counts: HashMap<ObjectType<'static>, HashMap<RoleType<'static>, u64>>,
    relation_role_counts: HashMap<RelationType<'static>, HashMap<RoleType<'static>, u64>>,
    player_index_counts: HashMap<ObjectType<'static>, HashMap<ObjectType<'static>, u64>>,

    // future: attribute value distributions, attribute value ownership distributions, etc.
}

impl Statistics {

    pub fn new(sequence_number: SequenceNumber) -> Self {
        Statistics {
            open_sequence_number: sequence_number,
            total_thing_count: 0,
            total_entity_count: 0,
            total_relation_count: 0,
            total_attribute_count: 0,
            total_role_count: 0,
            total_has_count: 0,
            entity_counts: HashMap::new(),
            relation_counts: HashMap::new(),
            attribute_counts: HashMap::new(),
            role_counts: HashMap::new(),
            has_attribute_counts: HashMap::new(),
            attribute_owner_counts: HashMap::new(),
            role_player_counts: HashMap::new(),
            relation_role_counts: HashMap::new(),
            player_index_counts: HashMap::new(),
        }
    }

    fn update_entities(&mut self, entity_type: EntityType<'static>, delta: u64) {
        self.entity_counts.entry(entity_type).or_insert(0).add_assign(delta);
        self.total_entity_count += delta;
        self.total_thing_count += delta;
    }

    fn update_relations(&mut self, relation_type: RelationType<'static>, delta: u64) {
        self.relation_counts.entry(relation_type).or_insert(0).add_assign(delta);
        self.total_relation_count+= delta;
        self.total_thing_count += delta;
    }

    fn update_attributes(&mut self, attribute_type: AttributeType<'static>, delta: u64) {
        self.attribute_counts.entry(attribute_type).or_insert(0).add_assign(delta);
        self.total_attribute_count+= delta;
        self.total_thing_count += delta;
    }

    fn update_roles(&mut self, role_type: RoleType<'static>, delta: u64) {
        self.role_counts.entry(role_type).or_insert(0).add_assign(delta);
        self.total_role_count+= delta;
    }

    fn update_has(&mut self, owner_type: ObjectType<'static>, attribute_type: AttributeType<'static>, delta: u64) {
        self.has_attribute_counts.entry(owner_type.clone()).or_insert_with(|| HashMap::new())
            .entry(attribute_type.clone()).or_insert(0).add_assign(delta);
        self.attribute_owner_counts.entry(attribute_type).or_insert_with(|| HashMap::new())
            .entry(owner_type).or_insert(0).add_assign(delta);
        self.total_has_count += delta;
    }

    fn update_role_player(
        &mut self,
        player_type: ObjectType<'static>,
        role_type: RoleType<'static>,
        relation_type: RelationType<'static>,
        delta: u64
    ) {
        self.role_player_counts.entry(player_type).or_insert_with(|| HashMap::new())
            .entry(role_type.clone()).or_insert(0).add_assign(delta);
        self.relation_role_counts.entry(relation_type).or_insert_with(|| HashMap::new())
            .entry(role_type).or_insert(0).add_assign(delta);
    }

    fn update_indexed_player(&mut self, player_1_type: ObjectType<'static>, player_2_type: ObjectType<'static>, delta: u64) {
        self.player_index_counts.entry(player_1_type.clone()).or_insert_with(|| HashMap::new())
            .entry(player_2_type.clone()).or_insert(0).add_assign(delta);
        if player_1_type != player_2_type {
            self.player_index_counts.entry(player_2_type).or_insert_with(|| HashMap::new())
                .entry(player_1_type).or_insert(0).add_assign(delta);
        }
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash)]
enum SerialisableType {
    Entity(TypeIDUInt),
    Relation(TypeIDUInt),
    Attribute(TypeIDUInt),
    Role(TypeIDUInt),
}

impl SerialisableType {
    pub(crate) fn id(&self) -> TypeIDUInt {
        match self {
            SerialisableType::Entity(id) => *id,
            SerialisableType::Relation(id) => *id,
            SerialisableType::Attribute(id) => *id,
            SerialisableType::Role(id) => *id,
        }
    }

    pub(crate) fn into_entity_type(self) -> EntityType<'static> {
        match self {
            Self::Entity(id) => EntityType::new(build_vertex_entity_type(TypeID::build(id))),
            _ => panic!("Incompatible conversion."),
        }
    }

    pub(crate) fn into_relation_type(self) -> RelationType<'static> {
        match self {
            Self::Relation(id) => RelationType::new(build_vertex_relation_type(TypeID::build(id))),
            _ => panic!("Incompatible conversion."),
        }
    }

    pub(crate) fn into_object_type(self) -> ObjectType<'static> {
        match self {
            Self::Entity(id) => ObjectType::Entity(EntityType::new(build_vertex_entity_type(TypeID::build(id)))),
            Self::Relation(id) => ObjectType::Relation(RelationType::new(build_vertex_relation_type(TypeID::build(id)))),
            _ => panic!("Incompatible conversion."),
        }
    }

    pub(crate) fn into_attribute_type(self) -> AttributeType<'static> {
        match self {
            Self::Attribute(id) => AttributeType::new(build_vertex_attribute_type(TypeID::build(id))),
            _ => panic!("Incompatible conversion."),
        }
    }

    pub(crate) fn into_role_type(self) -> RoleType<'static> {
        match self {
            Self::Role(id) => RoleType::new(build_vertex_role_type(TypeID::build(id))),
            _ => panic!("Incompatible conversion."),
        }
    }
}

impl From<ObjectType<'static>> for SerialisableType {
    fn from(object: ObjectType<'static>) -> Self {
        match object {
            ObjectType::Entity(entity) => Self::from(entity),
            ObjectType::Relation(relation) => Self::from(relation),
        }
    }
}

impl From<EntityType<'static>> for SerialisableType {
    fn from(entity: EntityType<'static>) -> Self {
        Self::Entity(entity.vertex().type_id_().as_u16())
    }
}

impl From<RelationType<'static>> for SerialisableType {
    fn from(relation: RelationType<'static>) -> Self {
        Self::Relation(relation.vertex().type_id_().as_u16())
    }
}

impl From<AttributeType<'static>> for SerialisableType {
    fn from(attribute: AttributeType<'static>) -> Self {
        Self::Attribute(attribute.vertex().type_id_().as_u16())
    }
}

impl From<RoleType<'static>> for SerialisableType {
    fn from(role_type: RoleType<'static>) -> Self {
        Self::Role(role_type.vertex().type_id_().as_u16())
    }
}

mod serialise {
    use std::collections::HashMap;
    use std::fmt;

    use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
    use serde::de::{MapAccess, SeqAccess, Visitor};
    use serde::ser::SerializeStruct;

    use crate::thing::statistics::{SerialisableType, Statistics};
    use crate::type_::attribute_type::AttributeType;
    use crate::type_::entity_type::EntityType;
    use crate::type_::object_type::ObjectType;
    use crate::type_::relation_type::RelationType;
    use crate::type_::role_type::RoleType;

    enum Field {
        OpenSequenceNumber,
        TotalThingCount,
        TotalEntityCount,
        TotalRelationCount,
        TotalAttributeCount,
        TotalRoleCount,
        TotalHasCount,
        EntityCounts,
        RelationCounts,
        AttributeCounts,
        RoleCounts,
        HasAttributeCounts,
        AttributeOwnerCounts,
        RolePlayerCounts,
        RelationRoleCounts,
        PlayerIndexCounts,
    }

    impl Field {
        const NAMES: [&'static str; 16] = [
            Self::OpenSequenceNumber.name(),
            Self::TotalThingCount.name(),
            Self::TotalEntityCount.name(),
            Self::TotalRelationCount.name(),
            Self::TotalAttributeCount.name(),
            Self::TotalRoleCount.name(),
            Self::TotalHasCount.name(),
            Self::EntityCounts.name(),
            Self::RelationCounts.name(),
            Self::AttributeCounts.name(),
            Self::RoleCounts.name(),
            Self::HasAttributeCounts.name(),
            Self::AttributeOwnerCounts.name(),
            Self::RolePlayerCounts.name(),
            Self::RelationRoleCounts.name(),
            Self::PlayerIndexCounts.name(),
        ];

        const fn name(&self) -> &str {
            match self {
                Field::OpenSequenceNumber => "OpenSequenceNumber",
                Field::TotalThingCount => "TotalThingCount",
                Field::TotalEntityCount => "TotalEntityCount",
                Field::TotalRelationCount => "TotalRelationCount",
                Field::TotalAttributeCount => "TotalAttributeCount",
                Field::TotalRoleCount => "TotalRoleCount",
                Field::TotalHasCount => "TotalHasCount",
                Field::EntityCounts => "EntityCounts",
                Field::RelationCounts => "RelationCounts",
                Field::AttributeCounts => "AttributeCounts",
                Field::RoleCounts => "RoleCounts",
                Field::HasAttributeCounts => "HasAttributeCounts",
                Field::AttributeOwnerCounts => "AttributeOwnerCounts",
                Field::RolePlayerCounts => "RolePlayerCounts",
                Field::RelationRoleCounts => "RelationRoleCounts",
                Field::PlayerIndexCounts => "PlayerIndexCounts",
            }
        }

        fn from(string: &str) -> Option<Self> {
            match string {
                "OpenSequenceNumber" => Some(Field::OpenSequenceNumber),
                "TotalThingCount" => Some(Field::TotalThingCount),
                "TotalEntityCount" => Some(Field::TotalEntityCount),
                "TotalRelationCount" => Some(Field::TotalRelationCount),
                "TotalAttributeCount" => Some(Field::TotalAttributeCount),
                "TotalRoleCount" => Some(Field::TotalRoleCount),
                "TotalHasCount" => Some(Field::TotalHasCount),
                "EntityCounts" => Some(Field::EntityCounts),
                "RelationCounts" => Some(Field::RelationCounts),
                "AttributeCounts" => Some(Field::AttributeCounts),
                "RoleCounts" => Some(Field::RoleCounts),
                "HasAttributeCounts" => Some(Field::HasAttributeCounts),
                "AttributeOwnerCounts" => Some(Field::AttributeOwnerCounts),
                "RolePlayerCounts" => Some(Field::RolePlayerCounts),
                "RelationRoleCounts" => Some(Field::RelationRoleCounts),
                "PlayerIndexCounts" => Some(Field::PlayerIndexCounts),
                _ => None,
            }
        }
    }

    impl Serialize for Statistics {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
        {
            let mut state = serializer.serialize_struct("Statistics", Field::NAMES.len())?;

            state.serialize_field(Field::OpenSequenceNumber.name(), &self.open_sequence_number)?;

            state.serialize_field(Field::TotalThingCount.name(), &self.total_thing_count)?;
            state.serialize_field(Field::TotalEntityCount.name(), &self.total_entity_count)?;
            state.serialize_field(Field::TotalRelationCount.name(), &self.total_relation_count)?;
            state.serialize_field(Field::TotalAttributeCount.name(), &self.total_attribute_count)?;
            state.serialize_field(Field::TotalRoleCount.name(), &self.total_role_count)?;
            state.serialize_field(Field::TotalHasCount.name(), &self.total_has_count)?;

            state.serialize_field(Field::EntityCounts.name(), &to_serialisable_map(&self.entity_counts))?;
            state.serialize_field(Field::RelationCounts.name(), &to_serialisable_map(&self.relation_counts))?;
            state.serialize_field(Field::AttributeCounts.name(), &to_serialisable_map(&self.attribute_counts))?;
            state.serialize_field(Field::RoleCounts.name(), &to_serialisable_map(&self.role_counts))?;

            let has_attribute_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> = to_serialisable_map_map(
                &self.has_attribute_counts
            );
            state.serialize_field(Field::HasAttributeCounts.name(), &has_attribute_counts)?;

            let attribute_owner_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> = to_serialisable_map_map(
                &self.attribute_owner_counts
            );
            state.serialize_field(Field::AttributeOwnerCounts.name(), &attribute_owner_counts)?;

            let role_player_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> = to_serialisable_map_map(
                &self.role_player_counts
            );
            state.serialize_field(Field::RolePlayerCounts.name(), &role_player_counts)?;

            let relation_role_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> = to_serialisable_map_map(
                &self.relation_role_counts
            );
            state.serialize_field(Field::RelationRoleCounts.name(), &relation_role_counts)?;

            let player_index_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> = to_serialisable_map_map(
                &self.player_index_counts
            );
            state.serialize_field(Field::PlayerIndexCounts.name(), &player_index_counts)?;

            state.end()
        }
    }

    fn to_serialisable_map_map<Type1: Into<SerialisableType> + Clone, Type2: Into<SerialisableType> + Clone>(
        map: &HashMap<Type1, HashMap<Type2, u64>>
    ) -> HashMap<SerialisableType, HashMap<SerialisableType, u64>> {
        map.iter()
            .map(|(type_, value)| (type_.clone().into(), to_serialisable_map(value)))
            .collect()
    }

    fn to_serialisable_map<Type_: Into<SerialisableType> + Clone>(map: &HashMap<Type_, u64>) -> HashMap<SerialisableType, u64> {
        map.iter()
            .map(|(type_, value)| (type_.clone().into(), *value))
            .collect()
    }

    fn into_entity_map(map: HashMap<SerialisableType, u64>) -> HashMap<EntityType<'static>, u64> {
        map.into_iter()
            .map(|(type_, value)| (type_.into_entity_type(), value))
            .collect()
    }

    fn into_relation_map(map: HashMap<SerialisableType, u64>) -> HashMap<RelationType<'static>, u64> {
        map.into_iter()
            .map(|(type_, value)| (type_.into_relation_type(), value))
            .collect()
    }

    fn into_attribute_map(map: HashMap<SerialisableType, u64>) -> HashMap<AttributeType<'static>, u64> {
        map.into_iter()
            .map(|(type_, value)| (type_.into_attribute_type(), value))
            .collect()
    }

    fn into_role_map(map: HashMap<SerialisableType, u64>) -> HashMap<RoleType<'static>, u64> {
        map.into_iter()
            .map(|(type_, value)| (type_.into_role_type(), value))
            .collect()
    }

    fn into_object_map(map: HashMap<SerialisableType, u64>) -> HashMap<ObjectType<'static>, u64> {
        map.into_iter()
            .map(|(type_, value)| (type_.into_object_type(), value))
            .collect()
    }

    impl<'de> Deserialize<'de> for Statistics {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
        {
            impl<'de> Deserialize<'de> for Field {
                fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
                    where
                        D: Deserializer<'de>,
                {
                    struct FieldVisitor;

                    impl<'de> Visitor<'de> for FieldVisitor {
                        type Value = Field;

                        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                            formatter.write_str("Unrecognised field")
                        }

                        fn visit_str<E>(self, value: &str) -> Result<Field, E>
                            where E: de::Error,
                        {
                            Field::from(value).ok_or_else(|| {
                                de::Error::unknown_field(value, &Field::NAMES)
                            })
                        }
                    }

                    deserializer.deserialize_identifier(FieldVisitor)
                }
            }

            struct StatisticsVisitor;

            impl<'de> Visitor<'de> for StatisticsVisitor {
                type Value = Statistics;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("struct StatisticsVisitor")
                }

                fn visit_seq<V>(self, mut seq: V) -> Result<Statistics, V::Error>
                    where
                        V: SeqAccess<'de>,
                {
                    let open_sequence_number = seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                    let total_thing_count = seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                    let total_entity_count = seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                    let total_relation_count = seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(3, &self))?;
                    let total_attribute_count = seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(4, &self))?;
                    let total_role_count = seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(5, &self))?;
                    let total_has_count = seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(6, &self))?;
                    let encoded_entity_counts = seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(7, &self))?;
                    let entity_counts = into_entity_map(encoded_entity_counts);
                    let encoded_relation_counts = seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(8, &self))?;
                    let relation_counts = into_relation_map(encoded_relation_counts);
                    let encoded_attribute_counts = seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(9, &self))?;
                    let attribute_counts = into_attribute_map(encoded_attribute_counts);
                    let encoded_role_counts = seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(10, &self))?;
                    let role_counts = into_role_map(encoded_role_counts);
                    let encoded_has_attribute_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?
                            .ok_or_else(|| de::Error::invalid_length(11, &self))?;
                    let has_attribute_counts = encoded_has_attribute_counts.into_iter()
                        .map(|(type_1, map)| (type_1.into_object_type(), into_attribute_map(map)))
                        .collect();
                    let encoded_attribute_owner_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?
                            .ok_or_else(|| de::Error::invalid_length(12, &self))?;
                    let attribute_owner_counts = encoded_attribute_owner_counts.into_iter()
                        .map(|(type_1, map)| (type_1.into_attribute_type(), into_object_map(map)))
                        .collect();
                    let encoded_role_player_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?
                            .ok_or_else(|| de::Error::invalid_length(13, &self))?;
                    let role_player_counts = encoded_role_player_counts.into_iter()
                        .map(|(type_1, map)| (type_1.into_object_type(), into_role_map(map)))
                        .collect();
                    let encoded_relation_role_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?
                            .ok_or_else(|| de::Error::invalid_length(14, &self))?;
                    let relation_role_counts = encoded_relation_role_counts.into_iter()
                        .map(|(type_1, map)| (type_1.into_relation_type(), into_role_map(map)))
                        .collect();
                    let encoded_player_index_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?
                            .ok_or_else(|| de::Error::invalid_length(15, &self))?;
                    let player_index_counts = encoded_player_index_counts.into_iter()
                        .map(|(type_1, map)| (type_1.into_object_type(), into_object_map(map)))
                        .collect();
                    Ok(Statistics {
                        open_sequence_number,
                        total_thing_count,
                        total_entity_count,
                        total_relation_count,
                        total_attribute_count,
                        total_role_count,
                        total_has_count,
                        entity_counts,
                        relation_counts,
                        attribute_counts,
                        role_counts,
                        has_attribute_counts,
                        attribute_owner_counts,
                        role_player_counts,
                        relation_role_counts,
                        player_index_counts,
                    })
                }

                fn visit_map<V>(self, mut map: V) -> Result<Statistics, V::Error>
                    where V: MapAccess<'de>,
                {
                    let mut open_sequence_number = None;
                    let mut total_thing_count = None;
                    let mut total_entity_count = None;
                    let mut total_relation_count = None;
                    let mut total_attribute_count = None;
                    let mut total_role_count = None;
                    let mut total_has_count = None;
                    let mut entity_counts = None;
                    let mut relation_counts = None;
                    let mut attribute_counts = None;
                    let mut role_counts = None;
                    let mut has_attribute_counts = None;
                    let mut attribute_owner_counts = None;
                    let mut role_player_counts = None;
                    let mut relation_role_counts = None;
                    let mut player_index_counts = None;
                    while let Some(key) = map.next_key()? {
                        match key {
                            Field::OpenSequenceNumber => {
                                if open_sequence_number.is_some() {
                                    return Err(de::Error::duplicate_field(Field::OpenSequenceNumber.name()));
                                }
                                open_sequence_number = Some(map.next_value()?);
                            }
                            Field::TotalThingCount => {
                                if total_thing_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalThingCount.name()));
                                }
                                total_thing_count = Some(map.next_value()?);
                            }
                            Field::TotalEntityCount => {
                                if total_entity_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalEntityCount.name()));
                                }
                                total_entity_count = Some(map.next_value()?);
                            }
                            Field::TotalRelationCount => {
                                if total_relation_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalRelationCount.name()));
                                }
                                total_relation_count = Some(map.next_value()?);
                            }
                            Field::TotalAttributeCount => {
                                if total_attribute_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalAttributeCount.name()));
                                }
                                total_attribute_count = Some(map.next_value()?);
                            }
                            Field::TotalRoleCount => {
                                if total_role_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalRoleCount.name()));
                                }
                                total_role_count = Some(map.next_value()?);
                            }
                            Field::TotalHasCount => {
                                if total_has_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalRoleCount.name()));
                                }
                                total_has_count = Some(map.next_value()?);
                            }
                            Field::EntityCounts => {
                                if entity_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalRoleCount.name()));
                                }
                                entity_counts = Some(into_entity_map(map.next_value()?));
                            }
                            Field::RelationCounts => {
                                if relation_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::RelationCounts.name()));
                                }
                                relation_counts = Some(into_relation_map(map.next_value()?));
                            }
                            Field::AttributeCounts => {
                                if attribute_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::AttributeCounts.name()));
                                }
                                attribute_counts = Some(into_attribute_map(map.next_value()?));
                            }
                            Field::RoleCounts => {
                                if role_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::RoleCounts.name()));
                                }
                                role_counts = Some(into_role_map(map.next_value()?));
                            }
                            Field::HasAttributeCounts => {
                                if has_attribute_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::HasAttributeCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> = map.next_value()?;
                                has_attribute_counts = Some(encoded.into_iter()
                                    .map(|(type_1, map)| (type_1.into_object_type(), into_attribute_map(map)))
                                    .collect());
                            }
                            Field::AttributeOwnerCounts => {
                                if attribute_owner_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::AttributeOwnerCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> = map.next_value()?;
                                attribute_owner_counts = Some(encoded.into_iter()
                                    .map(|(type_1, map)| (type_1.into_attribute_type(), into_object_map(map)))
                                    .collect());
                            }
                            Field::RolePlayerCounts => {
                                if role_player_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::RolePlayerCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> = map.next_value()?;
                                role_player_counts = Some(encoded.into_iter()
                                    .map(|(type_1, map)| (type_1.into_object_type(), into_role_map(map)))
                                    .collect());
                            }
                            Field::RelationRoleCounts => {
                                if relation_role_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::RelationRoleCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> = map.next_value()?;
                                relation_role_counts = Some(encoded.into_iter()
                                    .map(|(type_1, map)| (type_1.into_relation_type(), into_role_map(map)))
                                    .collect());
                            }
                            Field::PlayerIndexCounts => {
                                if player_index_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::PlayerIndexCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> = map.next_value()?;
                                player_index_counts = Some(encoded.into_iter()
                                    .map(|(type_1, map)| (type_1.into_object_type(), into_object_map(map)))
                                    .collect());
                            }
                        }
                    }

                    Ok(Statistics {
                        open_sequence_number: open_sequence_number
                            .ok_or_else(|| de::Error::missing_field(Field::OpenSequenceNumber.name()))?,
                        total_thing_count: total_thing_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalThingCount.name()))?,
                        total_entity_count: total_entity_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalEntityCount.name()))?,
                        total_relation_count: total_relation_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalRelationCount.name()))?,
                        total_attribute_count: total_attribute_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalAttributeCount.name()))?,
                        total_role_count: total_role_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalRoleCount.name()))?,
                        total_has_count: total_has_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalHasCount.name()))?,
                        entity_counts: entity_counts
                            .ok_or_else(|| de::Error::missing_field(Field::EntityCounts.name()))?,
                        relation_counts: relation_counts
                            .ok_or_else(|| de::Error::missing_field(Field::RelationCounts.name()))?,
                        attribute_counts: attribute_counts
                            .ok_or_else(|| de::Error::missing_field(Field::AttributeCounts.name()))?,
                        role_counts: role_counts
                            .ok_or_else(|| de::Error::missing_field(Field::RoleCounts.name()))?,
                        has_attribute_counts: has_attribute_counts
                            .ok_or_else(|| de::Error::missing_field(Field::HasAttributeCounts.name()))?,
                        attribute_owner_counts: attribute_owner_counts
                            .ok_or_else(|| de::Error::missing_field(Field::AttributeOwnerCounts.name()))?,
                        role_player_counts: role_player_counts
                            .ok_or_else(|| de::Error::missing_field(Field::RolePlayerCounts.name()))?,
                        relation_role_counts: relation_role_counts
                            .ok_or_else(|| de::Error::missing_field(Field::RelationRoleCounts.name()))?,
                        player_index_counts: player_index_counts
                            .ok_or_else(|| de::Error::missing_field(Field::PlayerIndexCounts.name()))?,
                    })
                }
            }

            deserializer.deserialize_struct("Statistics", &Field::NAMES, StatisticsVisitor)
        }
    }
}