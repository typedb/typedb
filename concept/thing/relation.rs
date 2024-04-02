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

use bytes::Bytes;
use encoding::{AsBytes, Prefixed};
use encoding::graph::thing::edge::ThingEdgeHas;
use encoding::graph::thing::vertex_object::ObjectVertex;
use encoding::graph::type_::vertex::{build_vertex_relation_type};
use encoding::graph::Typed;
use encoding::layout::prefix::Prefix;
use storage::{
    key_value::StorageKeyReference,
    snapshot::{iterator::SnapshotRangeIterator, SnapshotError},
};

use crate::{
    concept_iterator,
    error::{ConceptError, ConceptErrorKind},
    ByteReference, ConceptAPI,
};
use crate::error::ConceptWriteError;
use crate::thing::attribute::{Attribute, AttributeIterator};
use crate::thing::object::Object;
use crate::thing::thing_manager::ThingManager;
use crate::type_::annotation::AnnotationDuplicate;
use crate::type_::relation_type::RelationType;
use crate::type_::role_type::{RoleType, RoleTypeAnnotation};
use crate::type_::TypeAPI;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Relation<'a> {
    vertex: ObjectVertex<'a>,
}

impl<'a> Relation<'a> {
    pub fn new(vertex: ObjectVertex<'a>) -> Self {
        debug_assert_eq!(vertex.prefix(), Prefix::VertexRelation);
        Relation { vertex }
    }

    pub fn type_(&self) -> RelationType<'static> {
        RelationType::new(build_vertex_relation_type(self.vertex.type_id()))
    }

    pub fn iid(&self) -> ByteReference<'_> {
        self.vertex.bytes()
    }

    pub fn get_has<'m, D>(&self, thing_manager: &'m ThingManager<'_, '_, D>)
                          -> AttributeIterator<'m, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        thing_manager.storage_get_has(self.vertex())
    }

    pub fn set_has<D>(&self, thing_manager: &ThingManager<'_, '_, D>, attribute: &Attribute<'_>)
                      -> Result<(), ConceptWriteError> {
        // TODO: validate schema
        thing_manager.storage_set_has(self.vertex(), attribute.vertex())
    }

    pub fn delete_has<D>(&self, thing_manager: &ThingManager<'_, '_, D>, attribute: &Attribute<'_>)
                         -> Result<(), ConceptWriteError> {
        // TODO: validate schema
        todo!()
    }

    pub fn get_relations<'m, D>(&self, thing_manager: &'m ThingManager<'_, '_, D>) {
        // -> RelationRoleIterator<'m, _> {
        todo!()
        // TODO: fix return type prefix size
    }

    pub fn get_players<'m, D>(&self, thing_manager: &'m ThingManager<'_, '_, D>) {
        // -> ObjectIterator<'m, _> {
        todo!()
    }

    ///
    /// Semantics:
    ///   When duplicates are not allowed, we use set semantics and put the edge idempotently, which cannot fail other txn's
    ///   When duplicates are allowed, we increment the count of the role player edge and fail other txn's doing the same
    ///
    pub fn add_player<D>(&self, thing_manager: &ThingManager<'_, '_, D>, role_type: RoleType<'static>, player: &Object<'_>) {
        // TODO: validate schema

        let role_annotations = role_type.get_annotations(thing_manager.type_manager()).unwrap();
        let duplicates_allowed = role_annotations.contains(&RoleTypeAnnotation::Duplicate(AnnotationDuplicate::new()));
        let mut player_count = 0;
        if duplicates_allowed {
            // TODO: handle error
            player_count = thing_manager.storage_increment_role_player(self.vertex(), player.vertex(), role_type.clone().into_vertex()).unwrap();
        } else {
            // TODO: handle error
            thing_manager.storage_set_role_player(self.vertex(), player.vertex(), role_type.clone().into_vertex()).unwrap();
            player_count = 1;
        }

        /*
        ### Handling relation index ###
        On commit we could get all modified relations + role players
        for 'small' relation types, we generate indexes to (new and existing) role player pairs.
        On commit this is more efficient (read existing role players only once) and avoids races.

        However! This means we cannot use these indexes during write transactions, which makes traversal a pain!
        Then, we have to solve race conditions - perhaps locks held in the ThingManager?
        ...
        ???
        Risk: operation 1: add player 1, concurrent operation 2: add player 2 -- miss player 1 - 2 index. Subsequent operation 3: traversal using indexes...


        Option 3: we take a single relation write lock on the whole snapshot, since concurrent writes like this are uncommon
         */

        let optimised_relation_type = true; // TODO: check whether relation type is optimised
        // TODO handle error
        thing_manager.storage_relation_index_new_player(self.vertex(), player.vertex(), role_type.into_vertex()).unwrap();
    }


    pub(crate) fn vertex<'this: 'a>(&'this self) -> ObjectVertex<'this> {
        self.vertex.as_reference()
    }

    pub(crate) fn into_owned(self) -> Relation<'static> {
        Relation { vertex: self.vertex.into_owned() }
    }
}

impl<'a> ConceptAPI<'a> for Relation<'a> {}

// TODO: can we inline this into the macro invocation?
fn storage_key_ref_to_entity(storage_key_ref: StorageKeyReference<'_>) -> Relation<'_> {
    Relation::new(ObjectVertex::new(Bytes::Reference(storage_key_ref.byte_ref())))
}
concept_iterator!(RelationIterator, Relation, storage_key_ref_to_entity);
