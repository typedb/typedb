/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt};

use encoding::{DecodableKey, graph::type_::vertex::PrefixedTypeVertexEncoding};
use storage::{
    durability_client::{DurabilityRecord, UnsequencedDurabilityRecord},
    record::CommitRecord,
    sequence_number::SequenceNumber,
};

use crate::{
    thing::{
        ThingAPI,
        attribute::Attribute,
        entity::Entity,
        object::Object,
        relation::Relation,
        statistics::{DoubleHashMap, DoubleHashMapExt, TripleHashMap, TripleHashMapExt},
    },
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType, relation_type::RelationType,
        role_type::RoleType,
    },
};

#[derive(Debug, Default, Clone, Copy)]
pub struct Delta {
    inserts: u64,
    overwrites: u64,
    deletes: u64,
}

impl Delta {
    pub fn net_change(self) -> i64 {
        let inserts = self.inserts;
        let deletes = self.deletes;
        // overwrites shadow an existing key without affecting overall count
        inserts
            .checked_signed_diff(deletes)
            .unwrap_or_else(|| panic!("{inserts} inserts - {deletes} deletes overflows i64!"))
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u64)]
pub(crate) enum CommitDeltasEncodingVersion {
    V0 = 0,
}

impl From<CommitDeltasEncodingVersion> for u64 {
    fn from(value: CommitDeltasEncodingVersion) -> u64 {
        value as u64
    }
}

pub struct UnknownCommitDeltasEncodingVersion(pub u64);

impl fmt::Display for UnknownCommitDeltasEncodingVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unknown commit deltas encoding version: {}", self.0)
    }
}

impl fmt::Debug for UnknownCommitDeltasEncodingVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl TryFrom<u64> for CommitDeltasEncodingVersion {
    type Error = UnknownCommitDeltasEncodingVersion;

    fn try_from(u64: u64) -> Result<Self, Self::Error> {
        match u64 {
            0 => Ok(Self::V0),
            other => Err(UnknownCommitDeltasEncodingVersion(other)),
        }
    }
}

#[derive(Clone)]
pub struct CommitDeltas {
    pub(crate) encoding_version: CommitDeltasEncodingVersion,
    pub commit_sequence_number: SequenceNumber,

    pub entity_deltas: HashMap<EntityType, Delta>,
    pub relation_deltas: HashMap<RelationType, Delta>,
    pub attribute_deltas: HashMap<AttributeType, Delta>,

    pub has_attribute_deltas: DoubleHashMap<ObjectType, AttributeType, Delta>,
    pub relation_role_player_deltas: TripleHashMap<RelationType, RoleType, ObjectType, Delta>,

    // TODO: adding role types is possible, but won't help with filtering before reading storage since roles are not in the prefix
    //       see also in Statistics
    pub links_index_deltas: DoubleHashMap<ObjectType, ObjectType, Delta>,
}

impl CommitDeltas {
    const ENCODING_VERSION: CommitDeltasEncodingVersion = CommitDeltasEncodingVersion::V0;

    pub fn from_commit(commit_record: &CommitRecord, commit_sequence_number: SequenceNumber) -> Self {
        let mut entity_deltas = HashMap::<_, Delta>::new();
        let mut relation_deltas = HashMap::<_, Delta>::new();
        let mut attribute_deltas = HashMap::<_, Delta>::new();
        let mut has_attribute_deltas = DoubleHashMap::<_, _, Delta>::new();
        let mut relation_role_player_deltas = TripleHashMap::<_, _, _, Delta>::new();
        let mut links_index_deltas = DoubleHashMap::<_, _, Delta>::new();

        for (key, write) in commit_record.operations().iterate_writes() {
            let update = |delta: &mut Delta| {
                if write.is_delete() {
                    delta.deletes += 1
                } else if write.intends_insert() {
                    delta.inserts += 1
                } else if write.is_overwrite() {
                    delta.overwrites += 1
                } else {
                    #[cfg(debug_assertions)]
                    match write {
                        storage::snapshot::write::Write::Put { action, .. }
                            if action.load(std::sync::atomic::Ordering::Relaxed)
                                == storage::snapshot::write::PutAction::Nop => {}
                        write => unreachable!("Not a delete, insert, an overwrite, or a no-op: {write:?}"),
                    }
                }
            };

            match DecodableKey::try_decode(key.bytes()) {
                Some(DecodableKey::EntityVertex(entity_vertex)) => {
                    update(entity_deltas.entry(Entity::new(entity_vertex).type_()).or_default());
                }
                Some(DecodableKey::RelationVertex(relation_vertex)) => {
                    update(relation_deltas.entry(Relation::new(relation_vertex).type_()).or_default());
                }
                Some(DecodableKey::AttributeVertex(attribute_vertex)) => {
                    update(attribute_deltas.entry(Attribute::new(attribute_vertex).type_()).or_default());
                }
                Some(DecodableKey::ThingEdgeHas(has_edge)) => {
                    let owner = Object::new(has_edge.from()).type_();
                    let attribute = Attribute::new(has_edge.to()).type_();
                    update(has_attribute_deltas.double_entry(owner, attribute).or_default());
                }
                Some(DecodableKey::ThingEdgeHasReverse(_)) => (), // handled above
                Some(DecodableKey::ThingEdgeLinks(links_edge)) if !links_edge.is_reverse() => {
                    let relation = Relation::new(links_edge.from()).type_();
                    let role = RoleType::build_from_type_id(links_edge.role_id());
                    let player = Object::new(links_edge.to()).type_();
                    update(relation_role_player_deltas.triple_entry(relation, role, player).or_default());
                }
                Some(DecodableKey::ThingEdgeLinks(_)) => (), // handled above
                Some(DecodableKey::ThingEdgeIndexedRelation(index_relation_edge)) => {
                    let player1 = Object::new(index_relation_edge.from()).type_();
                    let player2 = Object::new(index_relation_edge.to()).type_();
                    update(links_index_deltas.double_entry(player1, player2).or_default());
                }

                Some(DecodableKey::VertexEntityType(_))
                | Some(DecodableKey::VertexRelationType(_))
                | Some(DecodableKey::VertexAttributeType(_))
                | Some(DecodableKey::VertexRoleType(_))
                | Some(DecodableKey::PropertyTypeVertex(_))
                | Some(DecodableKey::PropertyTypeEdge(_))
                | Some(DecodableKey::PropertyObjectVertex(_))
                | Some(DecodableKey::PropertyFunction(_))
                | Some(DecodableKey::IndexLabelToType(_))
                | Some(DecodableKey::IndexNameToDefinitionStruct(_))
                | Some(DecodableKey::IndexNameToDefinitionFunction(_))
                | Some(DecodableKey::IndexValueToStruct(_))
                | Some(DecodableKey::DefinitionStruct(_))
                | Some(DecodableKey::DefinitionFunction(_))
                | Some(DecodableKey::TypeEdgeSub(_))
                | Some(DecodableKey::TypeEdgeSubReverse(_))
                | Some(DecodableKey::TypeEdgeOwns(_))
                | Some(DecodableKey::TypeEdgeOwnsReverse(_))
                | Some(DecodableKey::TypeEdgePlays(_))
                | Some(DecodableKey::TypeEdgePlaysReverse(_))
                | Some(DecodableKey::TypeEdgeRelates(_))
                | Some(DecodableKey::TypeEdgeRelatesReverse(_))
                | None => (),
            }
        }

        Self {
            encoding_version: Self::ENCODING_VERSION,
            commit_sequence_number,
            entity_deltas,
            relation_deltas,
            attribute_deltas,
            has_attribute_deltas,
            relation_role_player_deltas,
            links_index_deltas,
        }
    }
}

impl DurabilityRecord for CommitDeltas {
    const RECORD_TYPE: u8 = 20;

    const RECORD_NAME: &'static str = "commit_deltas";

    fn serialise_into(&self, writer: &mut impl std::io::Write) -> bincode::Result<()> {
        bincode::serialize_into(writer, self)
    }

    fn deserialise_from(reader: &mut impl std::io::Read) -> bincode::Result<Self> {
        bincode::deserialize_from(reader)
    }
}

impl UnsequencedDurabilityRecord for CommitDeltas {}

mod serialize {
    use std::fmt;

    use serde::{
        Deserialize, Deserializer, Serialize, Serializer,
        de::{self, Visitor},
        ser::SerializeStruct,
    };

    use crate::thing::statistics::{
        DoubleHashMap, TripleHashMap,
        deltas::{CommitDeltas, CommitDeltasEncodingVersion, Delta},
        serialise::*,
    };

    impl Serialize for CommitDeltasEncodingVersion {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            u64::serialize(&(*self).into(), serializer)
        }
    }

    impl<'de> Deserialize<'de> for CommitDeltasEncodingVersion {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let u64 = u64::deserialize(deserializer)?;
            Self::try_from(u64).map_err(|_| de::Error::invalid_value(de::Unexpected::Unsigned(u64), &"0"))
        }
    }

    impl Serialize for Delta {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let Self { inserts, overwrites, deletes } = *self;
            [inserts, overwrites, deletes].serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for Delta {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let [inserts, overwrites, deletes] = Deserialize::deserialize(deserializer)?;
            Ok(Self { inserts, overwrites, deletes })
        }
    }

    enum CommitDeltasField {
        EncodingVersion,
        CommitSequenceNumber,
        EntityDeltas,
        RelationDeltas,
        AttributeDeltas,
        HasAttributeDeltas,
        RelationRolePlayerDeltas,
        LinksIndexDeltas,
    }

    impl CommitDeltasField {
        const NAMES: [&'static str; 8] = [
            Self::EncodingVersion.name(),
            Self::CommitSequenceNumber.name(),
            Self::EntityDeltas.name(),
            Self::RelationDeltas.name(),
            Self::AttributeDeltas.name(),
            Self::HasAttributeDeltas.name(),
            Self::RelationRolePlayerDeltas.name(),
            Self::LinksIndexDeltas.name(),
        ];

        const fn name(&self) -> &'static str {
            match self {
                CommitDeltasField::EncodingVersion => "EncodingVersion",
                CommitDeltasField::CommitSequenceNumber => "CommitSequenceNumber",
                CommitDeltasField::EntityDeltas => "EntityDeltas",
                CommitDeltasField::RelationDeltas => "RelationDeltas",
                CommitDeltasField::AttributeDeltas => "AttributeDeltas",
                CommitDeltasField::HasAttributeDeltas => "HasAttributeDeltas",
                CommitDeltasField::RelationRolePlayerDeltas => "RelationRolePlayerDeltas",
                CommitDeltasField::LinksIndexDeltas => "LinksIndexDeltas",
            }
        }

        fn try_from(str: &str) -> Option<Self> {
            match str {
                "EncodingVersion" => Some(CommitDeltasField::EncodingVersion),
                "CommitSequenceNumber" => Some(CommitDeltasField::CommitSequenceNumber),
                "EntityDeltas" => Some(CommitDeltasField::EntityDeltas),
                "RelationDeltas" => Some(CommitDeltasField::RelationDeltas),
                "AttributeDeltas" => Some(CommitDeltasField::AttributeDeltas),
                "HasAttributeDeltas" => Some(CommitDeltasField::HasAttributeDeltas),
                "RelationRolePlayerDeltas" => Some(CommitDeltasField::RelationRolePlayerDeltas),
                "LinksIndexDeltas" => Some(CommitDeltasField::LinksIndexDeltas),
                _ => None,
            }
        }
    }

    impl<'de> Deserialize<'de> for CommitDeltasField {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct FieldVisitor;

            impl Visitor<'_> for FieldVisitor {
                type Value = CommitDeltasField;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("Unrecognised field")
                }

                fn visit_str<E>(self, value: &str) -> Result<CommitDeltasField, E>
                where
                    E: de::Error,
                {
                    CommitDeltasField::try_from(value)
                        .ok_or_else(|| de::Error::unknown_field(value, &CommitDeltasField::NAMES))
                }
            }

            deserializer.deserialize_identifier(FieldVisitor)
        }
    }

    impl Serialize for CommitDeltas {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut state = serializer.serialize_struct("CommitDeltas", 8)?;
            state.serialize_field(CommitDeltasField::EncodingVersion.name(), &self.encoding_version)?;
            state.serialize_field(CommitDeltasField::CommitSequenceNumber.name(), &self.commit_sequence_number)?;
            state.serialize_field(CommitDeltasField::EntityDeltas.name(), &to_serialisable_map(&self.entity_deltas))?;
            state.serialize_field(
                CommitDeltasField::RelationDeltas.name(),
                &to_serialisable_map(&self.relation_deltas),
            )?;
            state.serialize_field(
                CommitDeltasField::AttributeDeltas.name(),
                &to_serialisable_map(&self.attribute_deltas),
            )?;
            state.serialize_field(
                CommitDeltasField::HasAttributeDeltas.name(),
                &to_serialisable_map_map(&self.has_attribute_deltas),
            )?;
            state.serialize_field(
                CommitDeltasField::RelationRolePlayerDeltas.name(),
                &to_serialisable_map_map_map(&self.relation_role_player_deltas),
            )?;
            state.serialize_field(
                CommitDeltasField::LinksIndexDeltas.name(),
                &to_serialisable_map_map(&self.links_index_deltas),
            )?;
            state.end()
        }
    }

    impl<'de> Deserialize<'de> for CommitDeltas {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct CommitDeltasVisitor;

            impl<'de> de::Visitor<'de> for CommitDeltasVisitor {
                type Value = CommitDeltas;

                fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    formatter.write_str("struct CommitDeltasVisitor")
                }

                fn visit_seq<V>(self, mut seq: V) -> Result<CommitDeltas, V::Error>
                where
                    V: de::SeqAccess<'de>,
                {
                    let encoding_version = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
                    let commit_sequence_number =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                    let entity_deltas =
                        into_entity_map(seq.next_element()?.ok_or_else(|| de::Error::invalid_length(2, &self))?);
                    let relation_deltas =
                        into_relation_map(seq.next_element()?.ok_or_else(|| de::Error::invalid_length(3, &self))?);
                    let attribute_deltas =
                        into_attribute_map(seq.next_element()?.ok_or_else(|| de::Error::invalid_length(4, &self))?);
                    let has_attribute_deltas = seq
                        .next_element::<DoubleHashMap<SerialisableType, SerialisableType, _>>()?
                        .ok_or_else(|| de::Error::invalid_length(5, &self))?
                        .into_iter()
                        .map(|(ty, map)| (ty.into_object_type(), into_attribute_map(map)))
                        .collect();
                    let relation_role_player_deltas = seq
                        .next_element::<TripleHashMap<SerialisableType, SerialisableType, SerialisableType, _>>()?
                        .ok_or_else(|| de::Error::invalid_length(6, &self))?
                        .into_iter()
                        .map(|(ty, map)| {
                            (
                                ty.into_relation_type(),
                                map.into_iter().map(|(ty, map)| (ty.into_role_type(), into_object_map(map))).collect(),
                            )
                        })
                        .collect();
                    let links_index_deltas = seq
                        .next_element::<DoubleHashMap<SerialisableType, SerialisableType, _>>()?
                        .ok_or_else(|| de::Error::invalid_length(7, &self))?
                        .into_iter()
                        .map(|(ty, map)| (ty.into_object_type(), into_object_map(map)))
                        .collect();

                    Ok(CommitDeltas {
                        encoding_version,
                        commit_sequence_number,
                        entity_deltas,
                        relation_deltas,
                        attribute_deltas,
                        has_attribute_deltas,
                        relation_role_player_deltas,
                        links_index_deltas,
                    })
                }

                fn visit_map<V>(self, mut map: V) -> Result<CommitDeltas, V::Error>
                where
                    V: de::MapAccess<'de>,
                {
                    let mut encoding_version = None;
                    let mut commit_sequence_number = None;
                    let mut entity_deltas = None;
                    let mut relation_deltas = None;
                    let mut attribute_deltas = None;
                    let mut has_attribute_deltas = None;
                    let mut relation_role_player_deltas = None;
                    let mut links_index_deltas = None;

                    while let Some(key) = map.next_key()? {
                        match key {
                            CommitDeltasField::EncodingVersion => encoding_version = Some(map.next_value()?),
                            CommitDeltasField::CommitSequenceNumber => commit_sequence_number = Some(map.next_value()?),
                            CommitDeltasField::EntityDeltas => entity_deltas = Some(into_entity_map(map.next_value()?)),
                            CommitDeltasField::RelationDeltas => {
                                relation_deltas = Some(into_relation_map(map.next_value()?))
                            }
                            CommitDeltasField::AttributeDeltas => {
                                attribute_deltas = Some(into_attribute_map(map.next_value()?))
                            }
                            CommitDeltasField::HasAttributeDeltas => {
                                let encoded: DoubleHashMap<SerialisableType, SerialisableType, Delta> =
                                    map.next_value()?;
                                has_attribute_deltas = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| (type_1.into_object_type(), into_attribute_map(map)))
                                        .collect(),
                                )
                            }
                            CommitDeltasField::RelationRolePlayerDeltas => {
                                let encoded: TripleHashMap<
                                    SerialisableType,
                                    SerialisableType,
                                    SerialisableType,
                                    Delta,
                                > = map.next_value()?;
                                relation_role_player_deltas = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| {
                                            (
                                                type_1.into_relation_type(),
                                                map.into_iter()
                                                    .map(|(type_2, map)| {
                                                        (type_2.into_role_type(), into_object_map(map))
                                                    })
                                                    .collect(),
                                            )
                                        })
                                        .collect(),
                                )
                            }
                            CommitDeltasField::LinksIndexDeltas => {
                                let encoded: DoubleHashMap<SerialisableType, SerialisableType, Delta> =
                                    map.next_value()?;
                                links_index_deltas = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| (type_1.into_object_type(), into_object_map(map)))
                                        .collect(),
                                )
                            }
                        }
                    }

                    Ok(CommitDeltas {
                        encoding_version: encoding_version
                            .ok_or_else(|| de::Error::missing_field(CommitDeltasField::EncodingVersion.name()))?,
                        commit_sequence_number: commit_sequence_number
                            .ok_or_else(|| de::Error::missing_field(CommitDeltasField::CommitSequenceNumber.name()))?,
                        entity_deltas: entity_deltas
                            .ok_or_else(|| de::Error::missing_field(CommitDeltasField::EntityDeltas.name()))?,
                        relation_deltas: relation_deltas
                            .ok_or_else(|| de::Error::missing_field(CommitDeltasField::RelationDeltas.name()))?,
                        attribute_deltas: attribute_deltas
                            .ok_or_else(|| de::Error::missing_field(CommitDeltasField::AttributeDeltas.name()))?,
                        has_attribute_deltas: has_attribute_deltas
                            .ok_or_else(|| de::Error::missing_field(CommitDeltasField::HasAttributeDeltas.name()))?,
                        relation_role_player_deltas: relation_role_player_deltas.ok_or_else(|| {
                            de::Error::missing_field(CommitDeltasField::RelationRolePlayerDeltas.name())
                        })?,
                        links_index_deltas: links_index_deltas
                            .ok_or_else(|| de::Error::missing_field(CommitDeltasField::LinksIndexDeltas.name()))?,
                    })
                }
            }

            deserializer.deserialize_struct("CommitDeltas", &CommitDeltasField::NAMES, CommitDeltasVisitor)
        }
    }
}
