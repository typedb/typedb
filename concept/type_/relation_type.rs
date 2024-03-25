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

use std::collections::HashSet;
use std::ops::Deref;

use bytes::{byte_reference::ByteReference, Bytes};
use encoding::{
    graph::type_::vertex::{new_vertex_relation_type, TypeVertex},
    layout::prefix::PrefixType,
    Prefixed,
};
use encoding::value::label::Label;
use primitive::maybe_owns::MaybeOwns;
use storage::{
    key_value::StorageKeyReference,
    snapshot::{error::SnapshotError, iterator::SnapshotRangeIterator},
};

use crate::{
    concept_iterator,
    ConceptAPI,
    error::{ConceptError, ConceptErrorKind},
    type_::{
        annotation::{Annotation, AnnotationAbstract},
        attribute_type::AttributeType,
        object_type::ObjectType,
        OwnerAPI,
        PlayerAPI,
        owns::Owns,
        role_type::RoleType,
        plays::Plays,
        relates::Relates,
        type_manager::TypeManager, TypeAPI,
    },
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RelationType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> RelationType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> RelationType<'_> {
        if vertex.prefix() != PrefixType::VertexRelationType {
            panic!(
                "Type IID prefix was expected to be Prefix::RelationType ({:?}) but was {:?}",
                PrefixType::VertexRelationType,
                vertex.prefix()
            )
        }
        RelationType { vertex }
    }
}

impl<'a> ConceptAPI<'a> for RelationType<'a> {}

impl<'a> TypeAPI<'a> for RelationType<'a> {
    fn vertex<'this>(&'this self) -> &'this TypeVertex<'a> {
        &self.vertex
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }
}

impl<'a> RelationType<'a> {
    pub fn is_root<D>(&self, type_manager: &TypeManager<'_, '_, D>) -> bool {
        type_manager.get_relation_type_is_root(self.clone().into_owned())
    }

    pub fn get_label<'m, D>(&self, type_manager: &'m TypeManager<'_, '_, D>) -> MaybeOwns<'m, Label<'static>> {
        type_manager.get_relation_type_label(self.clone().into_owned())
    }

    fn set_label<D>(&self, type_manager: &TypeManager<'_, '_, D>, label: &Label<'_>) {
        // TODO: setLabel should fail is setting label on Root type
        type_manager.set_storage_label(self.vertex().clone().into_owned(), label);
    }

    pub fn get_supertype<D>(&self, type_manager: &TypeManager<'_, '_, D>) -> Option<RelationType<'static>> {
        type_manager.get_relation_type_supertype(self.clone().into_owned())
    }

    fn set_supertype<D>(&self, type_manager: &TypeManager<'_, '_, D>, supertype: RelationType<'static>) {
        type_manager.set_storage_supertype(self.vertex().clone().into_owned(), supertype.vertex().clone().into_owned())
    }

    pub fn get_supertypes<'m, D>(
        &self,
        type_manager: &'m TypeManager<'_, '_, D>,
    ) -> MaybeOwns<'m, Vec<RelationType<'static>>> {
        type_manager.get_relation_type_supertypes(self.clone().into_owned())
    }

    // fn get_subtypes(&self) -> MaybeOwns<'m, Vec<RelationType<'static>>>;

    pub fn get_annotations<'m, D>(
        &self,
        type_manager: &'m TypeManager<'_, '_, D>,
    ) -> MaybeOwns<'m, HashSet<RelationTypeAnnotation>> {
        type_manager.get_relation_type_annotations(self.clone().into_owned())
    }

    pub(crate) fn set_annotation<D>(&self, type_manager: &TypeManager<'_, '_, D>, annotation: RelationTypeAnnotation) {
        match annotation {
            RelationTypeAnnotation::Abstract(_) => {
                type_manager.set_storage_annotation_abstract(self.vertex().clone().into_owned())
            }
        }
    }

    fn delete_annotation<D>(&self, type_manager: &TypeManager<'_, '_, D>, annotation: RelationTypeAnnotation) {
        match annotation {
            RelationTypeAnnotation::Abstract(_) => {
                type_manager.delete_storage_annotation_abstract(self.vertex().clone().into_owned())
            }
        }
    }

    pub fn get_role<D>(&self, type_manager: &TypeManager<'_, '_, D>, name: &str) -> Option<RoleType<'static>> {
        let label = Label::build_scoped(name, self.get_label(type_manager).name().as_str());
        type_manager.get_role_type(&label)
    }

    pub fn create_relates<D>(&self, type_manager: &TypeManager<'_, '_, D>, name: &str) -> Relates<'static> {
        let label = Label::build_scoped(name, self.get_label(type_manager).name().as_str());
        type_manager.create_role_type(&label, self.clone().into_owned(), false);
        self.get_relates_role(type_manager, name).unwrap()
    }

    fn delete_relates<D>(&self, type_manager: &TypeManager<'_, '_, D>, role_type: RoleType<'static>) {
        type_manager.delete_storage_relates(self.vertex().clone().into_owned(), role_type.into_vertex());
    }

    fn get_relates<'m, D>(&self, type_manager: &'m TypeManager<'_, '_, D>) -> MaybeOwns<'m, HashSet<Relates<'static>>> {
        type_manager.get_relation_type_relates(self.clone().into_owned())
    }

    pub fn get_relates_role<D>(&self, type_manager: &TypeManager<'_, '_, D>, name: &str) -> Option<Relates<'static>> {
        self.get_role(type_manager, name).map(|role_type| Relates::new(self.clone().into_owned(), role_type))
    }

    fn has_relates_role<D>(&self, type_manager: &TypeManager<'_, '_, D>, name: &str) -> bool {
        self.get_relates_role(type_manager, name).is_some()
    }

    fn into_owned(self) -> RelationType<'static> {
        RelationType { vertex: self.vertex.into_owned() }
    }
}

impl<'a> OwnerAPI<'a> for RelationType<'a> {
    fn set_owns<D>(&self, type_manager: &TypeManager<'_, '_, D>, attribute_type: AttributeType<'static>) -> Owns<'static> {
        type_manager.set_storage_owns(self.vertex().clone().into_owned(), attribute_type.clone().into_vertex());
        self.get_owns_attribute(type_manager, attribute_type).unwrap()
    }

    fn delete_owns<D>(&self, type_manager: &TypeManager<'_, '_, D>, attribute_type: AttributeType<'static>) {
        // TODO: error if not owned?
        type_manager.delete_storage_owns(self.vertex().clone().into_owned(), attribute_type.into_vertex());
    }

    fn get_owns<'m, D>(&self, type_manager: &'m TypeManager<'_, '_, D>) -> MaybeOwns<'m, HashSet<Owns<'static>>> {
        type_manager.get_relation_type_owns(self.clone().into_owned())
    }

    fn get_owns_attribute<D>(&self, type_manager: &TypeManager<'_, '_, D>, attribute_type: AttributeType<'static>) -> Option<Owns<'static>> {
        let expected_owns = Owns::new(ObjectType::Relation(self.clone().into_owned()), attribute_type);
        if self.get_owns(type_manager).deref().contains(&expected_owns) {
            Some(expected_owns)
        } else {
            None
        }
    }
}

impl<'a> PlayerAPI<'a> for RelationType<'a> {
    fn set_plays<D>(&self, type_manager: &TypeManager<'_, '_, D>, role_type: RoleType<'static>) -> Plays<'static> {
        // TODO: decide behaviour (ok or error) if already playing
        type_manager.set_storage_plays(self.vertex().clone().into_owned(), role_type.clone().into_vertex());
        self.get_plays_role(type_manager, role_type).unwrap()
    }

    fn delete_plays<D>(&self, type_manager: &TypeManager<'_, '_, D>, role_type: RoleType<'static>) {
        // TODO: error if not playing?
        type_manager.delete_storage_plays(self.vertex().clone().into_owned(), role_type.into_vertex())
    }

    fn get_plays<'m, D>(&self, type_manager: &'m TypeManager<'_, '_, D>) -> MaybeOwns<'m, HashSet<Plays<'static>>> {
        todo!()
        // type_manager.get_relation_type_plays(self.clone().into_owned())
    }

    fn get_plays_role<D>(&self, type_manager: &TypeManager<'_, '_, D>, role_type: RoleType<'static>) -> Option<Plays<'static>> {
        let expected_plays = Plays::new(ObjectType::Relation(self.clone().into_owned()), role_type);
        if self.get_plays(type_manager).deref().contains(&expected_plays) {
            Some(expected_plays)
        } else {
            None
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RelationTypeAnnotation {
    Abstract(AnnotationAbstract),
}

impl From<Annotation> for RelationTypeAnnotation {
    fn from(annotation: Annotation) -> Self {
        match annotation {
            Annotation::Abstract(annotation) => RelationTypeAnnotation::Abstract(annotation),
        }
    }
}

// impl<'a> IIDAPI<'a> for RelationType<'a> {
//     fn iid(&'a self) -> ByteReference<'a> {
//         self.vertex.bytes()
//     }
// }

// TODO: can we inline this into the macro invocation?
fn storage_key_to_relation_type(storage_key_ref: StorageKeyReference<'_>) -> RelationType<'_> {
    RelationType::new(new_vertex_relation_type(Bytes::Reference(storage_key_ref.byte_ref())))
}

concept_iterator!(RelationTypeIterator, RelationType, storage_key_to_relation_type);
