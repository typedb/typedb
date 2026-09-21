/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

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

#[derive(Clone)]
pub struct CommitDeltas {
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
                    unreachable!("Not a delete, insert, or an overwrite: {write:?}");
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
        todo!()
    }

    fn deserialise_from(reader: &mut impl std::io::Read) -> bincode::Result<Self> {
        todo!()
    }
}

impl UnsequencedDurabilityRecord for CommitDeltas {}

mod serialize {
    use serde::{Deserialize, Serialize};

    use super::*;

    impl Serialize for CommitDeltas {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            todo!()
        }
    }

    impl<'de> Deserialize<'de> for CommitDeltas {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            todo!()
        }
    }
}
