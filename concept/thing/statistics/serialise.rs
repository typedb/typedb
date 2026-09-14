/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt};

use encoding::graph::{
    Typed,
    type_::vertex::{PrefixedTypeVertexEncoding, TypeID, TypeIDUInt, TypeVertexEncoding},
};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de,
    de::{MapAccess, SeqAccess, Visitor},
    ser::SerializeStruct,
};

use crate::{
    thing::statistics::{Statistics, StatisticsEncodingVersion},
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType, relation_type::RelationType,
        role_type::RoleType,
    },
};

enum Field {
    StatisticsVersion,
    OpenSequenceNumber,
    LastDurableWriteTotalCount,
    TotalCount,
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
    RelationRolePlayerCounts,
    PlayerRoleRelationCounts,
    LinksIndexCounts,
}

impl Field {
    const NAMES: [&'static str; 21] = [
        Self::StatisticsVersion.name(),
        Self::OpenSequenceNumber.name(),
        Self::LastDurableWriteTotalCount.name(),
        Self::TotalCount.name(),
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
        Self::RelationRolePlayerCounts.name(),
        Self::PlayerRoleRelationCounts.name(),
        Self::LinksIndexCounts.name(),
    ];

    const fn name(&self) -> &str {
        match self {
            Field::StatisticsVersion => "StatisticsVersion",
            Field::OpenSequenceNumber => "OpenSequenceNumber",
            Field::LastDurableWriteTotalCount => "LastDurableWriteTotalCount",
            Field::TotalCount => "TotalCount",
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
            Field::RelationRolePlayerCounts => "RelationRolePlayerCounts",
            Field::PlayerRoleRelationCounts => "RolePlayerRelationCounts",
            Field::LinksIndexCounts => "PlayerIndexCounts",
        }
    }

    fn from(string: &str) -> Option<Self> {
        match string {
            "StatisticsVersion" => Some(Field::StatisticsVersion),
            "OpenSequenceNumber" => Some(Field::OpenSequenceNumber),
            "LastDurableWriteTotalCount" => Some(Field::LastDurableWriteTotalCount),
            "TotalCount" => Some(Field::TotalCount),
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
            "RelationRolePlayerCounts" => Some(Field::RelationRolePlayerCounts),
            "RolePlayerRelationCounts" => Some(Field::PlayerRoleRelationCounts),
            "PlayerIndexCounts" => Some(Field::LinksIndexCounts),
            _ => None,
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
    pub(crate) fn into_entity_type(self) -> EntityType {
        match self {
            Self::Entity(id) => EntityType::build_from_type_id(TypeID::new(id)),
            _ => panic!("Incompatible conversion."),
        }
    }

    pub(crate) fn into_relation_type(self) -> RelationType {
        match self {
            Self::Relation(id) => RelationType::build_from_type_id(TypeID::new(id)),
            _ => panic!("Incompatible conversion."),
        }
    }

    pub(crate) fn into_object_type(self) -> ObjectType {
        match self {
            Self::Entity(id) => ObjectType::Entity(EntityType::build_from_type_id(TypeID::new(id))),
            Self::Relation(id) => ObjectType::Relation(RelationType::build_from_type_id(TypeID::new(id))),
            _ => panic!("Incompatible conversion."),
        }
    }

    pub(crate) fn into_attribute_type(self) -> AttributeType {
        match self {
            Self::Attribute(id) => AttributeType::build_from_type_id(TypeID::new(id)),
            _ => panic!("Incompatible conversion."),
        }
    }

    pub(crate) fn into_role_type(self) -> RoleType {
        match self {
            Self::Role(id) => RoleType::build_from_type_id(TypeID::new(id)),
            _ => panic!("Incompatible conversion."),
        }
    }
}

impl From<ObjectType> for SerialisableType {
    fn from(object: ObjectType) -> Self {
        match object {
            ObjectType::Entity(entity) => Self::from(entity),
            ObjectType::Relation(relation) => Self::from(relation),
        }
    }
}

impl From<EntityType> for SerialisableType {
    fn from(entity: EntityType) -> Self {
        Self::Entity(entity.vertex().type_id_().as_u16())
    }
}

impl From<RelationType> for SerialisableType {
    fn from(relation: RelationType) -> Self {
        Self::Relation(relation.vertex().type_id_().as_u16())
    }
}

impl From<AttributeType> for SerialisableType {
    fn from(attribute: AttributeType) -> Self {
        Self::Attribute(attribute.vertex().type_id_().as_u16())
    }
}

impl From<RoleType> for SerialisableType {
    fn from(role_type: RoleType) -> Self {
        Self::Role(role_type.vertex().type_id_().as_u16())
    }
}

impl Serialize for StatisticsEncodingVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        u64::serialize(&(*self).into(), serializer)
    }
}

impl<'de> Deserialize<'de> for StatisticsEncodingVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u64 = u64::deserialize(deserializer)?;
        Self::try_from(u64).map_err(|_| de::Error::invalid_value(de::Unexpected::Unsigned(u64), &"0"))
    }
}

impl Serialize for Statistics {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Statistics", Field::NAMES.len())?;
        state.serialize_field(Field::StatisticsVersion.name(), &self.encoding_version)?;

        state.serialize_field(Field::OpenSequenceNumber.name(), &self.sequence_number)?;
        state.serialize_field(Field::LastDurableWriteTotalCount.name(), &self.last_durable_write_total_count)?;

        state.serialize_field(Field::TotalCount.name(), &self.total_count)?;
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

        state
            .serialize_field(Field::HasAttributeCounts.name(), &to_serialisable_map_map(&self.has_attribute_counts))?;

        state.serialize_field(
            Field::AttributeOwnerCounts.name(),
            &to_serialisable_map_map(&self.attribute_owner_counts),
        )?;

        state.serialize_field(Field::RolePlayerCounts.name(), &to_serialisable_map_map(&self.role_player_counts))?;

        state
            .serialize_field(Field::RelationRoleCounts.name(), &to_serialisable_map_map(&self.relation_role_counts))?;

        state.serialize_field(
            Field::RelationRolePlayerCounts.name(),
            &to_serialisable_map_map_map(&self.relation_role_player_counts),
        )?;

        state.serialize_field(
            Field::PlayerRoleRelationCounts.name(),
            &to_serialisable_map_map_map(&self.player_role_relation_counts),
        )?;

        state.serialize_field(Field::LinksIndexCounts.name(), &to_serialisable_map_map(&self.links_index_counts))?;

        state.end()
    }
}

fn to_serialisable_map_map<Type1: Into<SerialisableType> + Clone, Type2: Into<SerialisableType> + Clone>(
    map: &HashMap<Type1, HashMap<Type2, u64>>,
) -> HashMap<SerialisableType, HashMap<SerialisableType, u64>> {
    map.iter().map(|(type_, value)| (type_.clone().into(), to_serialisable_map(value))).collect()
}

fn to_serialisable_map_map_map<
    Type1: Into<SerialisableType> + Clone,
    Type2: Into<SerialisableType> + Clone,
    Type3: Into<SerialisableType> + Clone,
>(
    map: &HashMap<Type1, HashMap<Type2, HashMap<Type3, u64>>>,
) -> HashMap<SerialisableType, HashMap<SerialisableType, HashMap<SerialisableType, u64>>> {
    map.iter().map(|(type_, value)| (type_.clone().into(), to_serialisable_map_map(value))).collect()
}

fn to_serialisable_map<Type_: Into<SerialisableType> + Clone>(
    map: &HashMap<Type_, u64>,
) -> HashMap<SerialisableType, u64> {
    map.iter().map(|(type_, value)| (type_.clone().into(), *value)).collect()
}

fn into_entity_map(map: HashMap<SerialisableType, u64>) -> HashMap<EntityType, u64> {
    map.into_iter().map(|(type_, value)| (type_.into_entity_type(), value)).collect()
}

fn into_relation_map(map: HashMap<SerialisableType, u64>) -> HashMap<RelationType, u64> {
    map.into_iter().map(|(type_, value)| (type_.into_relation_type(), value)).collect()
}

fn into_attribute_map(map: HashMap<SerialisableType, u64>) -> HashMap<AttributeType, u64> {
    map.into_iter().map(|(type_, value)| (type_.into_attribute_type(), value)).collect()
}

fn into_role_map(map: HashMap<SerialisableType, u64>) -> HashMap<RoleType, u64> {
    map.into_iter().map(|(type_, value)| (type_.into_role_type(), value)).collect()
}

fn into_object_map(map: HashMap<SerialisableType, u64>) -> HashMap<ObjectType, u64> {
    map.into_iter().map(|(type_, value)| (type_.into_object_type(), value)).collect()
}

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FieldVisitor;

        impl Visitor<'_> for FieldVisitor {
            type Value = Field;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("Unrecognised field")
            }

            fn visit_str<E>(self, value: &str) -> Result<Field, E>
            where
                E: de::Error,
            {
                Field::from(value).ok_or_else(|| de::Error::unknown_field(value, &Field::NAMES))
            }
        }

        deserializer.deserialize_identifier(FieldVisitor)
    }
}

impl<'de> Deserialize<'de> for Statistics {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
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
                let statistics_version = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let sequence_number = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let last_durable_write_total_count =
                    seq.next_element()?.ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let total_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(3, &self))?;
                let total_thing_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(4, &self))?;
                let total_entity_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(5, &self))?;
                let total_relation_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(6, &self))?;
                let total_attribute_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(7, &self))?;
                let total_role_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(8, &self))?;
                let total_has_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(9, &self))?;
                let encoded_entity_counts = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(10, &self))?;
                let entity_counts = into_entity_map(encoded_entity_counts);
                let encoded_relation_counts =
                    seq.next_element()?.ok_or_else(|| de::Error::invalid_length(11, &self))?;
                let relation_counts = into_relation_map(encoded_relation_counts);
                let encoded_attribute_counts =
                    seq.next_element()?.ok_or_else(|| de::Error::invalid_length(12, &self))?;
                let attribute_counts = into_attribute_map(encoded_attribute_counts);
                let encoded_role_counts = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(13, &self))?;
                let role_counts = into_role_map(encoded_role_counts);
                let encoded_has_attribute_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                    seq.next_element()?.ok_or_else(|| de::Error::invalid_length(14, &self))?;
                let has_attribute_counts = encoded_has_attribute_counts
                    .into_iter()
                    .map(|(type_1, map)| (type_1.into_object_type(), into_attribute_map(map)))
                    .collect();
                let encoded_attribute_owner_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                    seq.next_element()?.ok_or_else(|| de::Error::invalid_length(15, &self))?;
                let attribute_owner_counts = encoded_attribute_owner_counts
                    .into_iter()
                    .map(|(type_1, map)| (type_1.into_attribute_type(), into_object_map(map)))
                    .collect();
                let encoded_role_player_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                    seq.next_element()?.ok_or_else(|| de::Error::invalid_length(16, &self))?;
                let role_player_counts = encoded_role_player_counts
                    .into_iter()
                    .map(|(type_1, map)| (type_1.into_object_type(), into_role_map(map)))
                    .collect();
                let encoded_relation_role_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                    seq.next_element()?.ok_or_else(|| de::Error::invalid_length(17, &self))?;
                let relation_role_counts = encoded_relation_role_counts
                    .into_iter()
                    .map(|(type_1, map)| (type_1.into_relation_type(), into_role_map(map)))
                    .collect();
                let encoded_relation_role_player_counts: HashMap<
                    SerialisableType,
                    HashMap<SerialisableType, HashMap<SerialisableType, u64>>,
                > = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(18, &self))?;
                let relation_role_player_counts = encoded_relation_role_player_counts
                    .into_iter()
                    .map(|(type_1, map)| {
                        (
                            type_1.into_relation_type(),
                            map.into_iter()
                                .map(|(type_1, map)| (type_1.into_role_type(), into_object_map(map)))
                                .collect(),
                        )
                    })
                    .collect();
                let encoded_player_role_relation_counts: HashMap<
                    SerialisableType,
                    HashMap<SerialisableType, HashMap<SerialisableType, u64>>,
                > = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(19, &self))?;
                let player_role_relation_counts = encoded_player_role_relation_counts
                    .into_iter()
                    .map(|(type_1, map)| {
                        (
                            type_1.into_object_type(),
                            map.into_iter()
                                .map(|(type_1, map)| (type_1.into_role_type(), into_relation_map(map)))
                                .collect(),
                        )
                    })
                    .collect();
                let encoded_links_index_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                    seq.next_element()?.ok_or_else(|| de::Error::invalid_length(20, &self))?;
                let links_index_counts = encoded_links_index_counts
                    .into_iter()
                    .map(|(type_1, map)| (type_1.into_object_type(), into_object_map(map)))
                    .collect();
                Ok(Statistics {
                    encoding_version: statistics_version,
                    sequence_number,
                    last_durable_write_sequence_number: sequence_number,
                    last_durable_write_total_count,
                    total_count,
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
                    relation_role_player_counts,
                    player_role_relation_counts,
                    links_index_counts,
                })
            }

            fn visit_map<V>(self, mut map: V) -> Result<Statistics, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut statistics_version = None;
                let mut open_sequence_number = None;
                let mut last_durable_write_total_count = None;
                let mut total_count = None;
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
                let mut relation_role_player_counts = None;
                let mut player_role_relation_counts = None;
                let mut links_indexs_counts = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::StatisticsVersion => {
                            if statistics_version.is_some() {
                                return Err(de::Error::duplicate_field(Field::StatisticsVersion.name()));
                            }
                            statistics_version = Some(map.next_value()?);
                        }
                        Field::OpenSequenceNumber => {
                            if open_sequence_number.is_some() {
                                return Err(de::Error::duplicate_field(Field::OpenSequenceNumber.name()));
                            }
                            open_sequence_number = Some(map.next_value()?);
                        }
                        Field::LastDurableWriteTotalCount => {
                            if total_count.is_some() {
                                return Err(de::Error::duplicate_field(Field::LastDurableWriteTotalCount.name()));
                            }
                            last_durable_write_total_count = Some(map.next_value()?);
                        }
                        Field::TotalCount => {
                            if total_count.is_some() {
                                return Err(de::Error::duplicate_field(Field::TotalCount.name()));
                            }
                            total_count = Some(map.next_value()?);
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
                            let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                map.next_value()?;
                            has_attribute_counts = Some(
                                encoded
                                    .into_iter()
                                    .map(|(type_1, map)| (type_1.into_object_type(), into_attribute_map(map)))
                                    .collect(),
                            );
                        }
                        Field::AttributeOwnerCounts => {
                            if attribute_owner_counts.is_some() {
                                return Err(de::Error::duplicate_field(Field::AttributeOwnerCounts.name()));
                            }
                            let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                map.next_value()?;
                            attribute_owner_counts = Some(
                                encoded
                                    .into_iter()
                                    .map(|(type_1, map)| (type_1.into_attribute_type(), into_object_map(map)))
                                    .collect(),
                            );
                        }
                        Field::RolePlayerCounts => {
                            if role_player_counts.is_some() {
                                return Err(de::Error::duplicate_field(Field::RolePlayerCounts.name()));
                            }
                            let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                map.next_value()?;
                            role_player_counts = Some(
                                encoded
                                    .into_iter()
                                    .map(|(type_1, map)| (type_1.into_object_type(), into_role_map(map)))
                                    .collect(),
                            );
                        }
                        Field::RelationRoleCounts => {
                            if relation_role_counts.is_some() {
                                return Err(de::Error::duplicate_field(Field::RelationRoleCounts.name()));
                            }
                            let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                map.next_value()?;
                            relation_role_counts = Some(
                                encoded
                                    .into_iter()
                                    .map(|(type_1, map)| (type_1.into_relation_type(), into_role_map(map)))
                                    .collect(),
                            );
                        }
                        Field::RelationRolePlayerCounts => {
                            if relation_role_counts.is_some() {
                                return Err(de::Error::duplicate_field(Field::RelationRolePlayerCounts.name()));
                            }
                            let encoded: HashMap<
                                SerialisableType,
                                HashMap<SerialisableType, HashMap<SerialisableType, u64>>,
                            > = map.next_value()?;
                            relation_role_player_counts = Some(
                                encoded
                                    .into_iter()
                                    .map(|(type_1, map)| {
                                        (
                                            type_1.into_relation_type(),
                                            map.into_iter()
                                                .map(|(type_1, map)| (type_1.into_role_type(), into_object_map(map)))
                                                .collect(),
                                        )
                                    })
                                    .collect(),
                            );
                        }
                        Field::PlayerRoleRelationCounts => {
                            if relation_role_counts.is_some() {
                                return Err(de::Error::duplicate_field(Field::PlayerRoleRelationCounts.name()));
                            }
                            let encoded: HashMap<
                                SerialisableType,
                                HashMap<SerialisableType, HashMap<SerialisableType, u64>>,
                            > = map.next_value()?;
                            player_role_relation_counts = Some(
                                encoded
                                    .into_iter()
                                    .map(|(type_1, map)| {
                                        (
                                            type_1.into_object_type(),
                                            map.into_iter()
                                                .map(|(type_1, map)| (type_1.into_role_type(), into_relation_map(map)))
                                                .collect(),
                                        )
                                    })
                                    .collect(),
                            );
                        }
                        Field::LinksIndexCounts => {
                            if links_indexs_counts.is_some() {
                                return Err(de::Error::duplicate_field(Field::LinksIndexCounts.name()));
                            }
                            let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                map.next_value()?;
                            links_indexs_counts = Some(
                                encoded
                                    .into_iter()
                                    .map(|(type_1, map)| (type_1.into_object_type(), into_object_map(map)))
                                    .collect(),
                            );
                        }
                    }
                }

                Ok(Statistics {
                    encoding_version: statistics_version
                        .ok_or_else(|| de::Error::missing_field(Field::StatisticsVersion.name()))?,
                    sequence_number: open_sequence_number
                        .ok_or_else(|| de::Error::missing_field(Field::OpenSequenceNumber.name()))?,
                    last_durable_write_total_count: last_durable_write_total_count
                        .ok_or_else(|| de::Error::missing_field(Field::LastDurableWriteTotalCount.name()))?,
                    last_durable_write_sequence_number: open_sequence_number
                        .ok_or_else(|| de::Error::missing_field(Field::OpenSequenceNumber.name()))?,
                    total_count: total_count.ok_or_else(|| de::Error::missing_field(Field::TotalCount.name()))?,
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
                    entity_counts: entity_counts.ok_or_else(|| de::Error::missing_field(Field::EntityCounts.name()))?,
                    relation_counts: relation_counts
                        .ok_or_else(|| de::Error::missing_field(Field::RelationCounts.name()))?,
                    attribute_counts: attribute_counts
                        .ok_or_else(|| de::Error::missing_field(Field::AttributeCounts.name()))?,
                    role_counts: role_counts.ok_or_else(|| de::Error::missing_field(Field::RoleCounts.name()))?,
                    has_attribute_counts: has_attribute_counts
                        .ok_or_else(|| de::Error::missing_field(Field::HasAttributeCounts.name()))?,
                    attribute_owner_counts: attribute_owner_counts
                        .ok_or_else(|| de::Error::missing_field(Field::AttributeOwnerCounts.name()))?,
                    role_player_counts: role_player_counts
                        .ok_or_else(|| de::Error::missing_field(Field::RolePlayerCounts.name()))?,
                    relation_role_counts: relation_role_counts
                        .ok_or_else(|| de::Error::missing_field(Field::RelationRoleCounts.name()))?,
                    relation_role_player_counts: relation_role_player_counts
                        .ok_or_else(|| de::Error::missing_field(Field::RelationRolePlayerCounts.name()))?,
                    player_role_relation_counts: player_role_relation_counts
                        .ok_or_else(|| de::Error::missing_field(Field::PlayerRoleRelationCounts.name()))?,
                    links_index_counts: links_indexs_counts
                        .ok_or_else(|| de::Error::missing_field(Field::LinksIndexCounts.name()))?,
                })
            }
        }

        deserializer.deserialize_struct("Statistics", &Field::NAMES, StatisticsVisitor)
    }
}
