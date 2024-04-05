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

use bytes::Bytes;
use encoding::{
    graph::type_::vertex::{new_vertex_relation_type, TypeVertex},
    layout::prefix::Prefix,
    Prefixed,
    value::label::Label,
};
use primitive::maybe_owns::MaybeOwns;
use storage::key_value::StorageKeyReference;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{concept_iterator, ConceptAPI, error::{ConceptReadError, ConceptWriteError}, type_::{
    annotation::{Annotation, AnnotationAbstract},
    attribute_type::AttributeType,
    object_type::ObjectType,
    OwnerAPI,
    owns::Owns,
    PlayerAPI,
    plays::Plays,
    relates::Relates,
    role_type::RoleType, type_manager::TypeManager, TypeAPI,
}};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RelationType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> RelationType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> RelationType<'_> {
        if vertex.prefix() != Prefix::VertexRelationType {
            panic!(
                "Type IID prefix was expected to be Prefix::RelationType ({:?}) but was {:?}",
                Prefix::VertexRelationType,
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
    pub fn is_root(&self, type_manager: &TypeManager<'_, impl ReadableSnapshot>) -> Result<bool, ConceptReadError> {
        type_manager.get_relation_type_is_root(self.clone().into_owned())
    }

    pub fn get_label<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_relation_type_label(self.clone().into_owned())
    }

    fn set_label(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, label: &Label<'_>) {
        // TODO: setLabel should fail is setting label on Root type
        type_manager.set_storage_label(self.vertex().clone().into_owned(), label)
    }

    pub fn get_supertype(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<Option<RelationType<'static>>, ConceptReadError> {
        type_manager.get_relation_type_supertype(self.clone().into_owned())
    }

    fn set_supertype(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, supertype: RelationType<'static>) {
        type_manager.set_storage_supertype(self.vertex().clone().into_owned(), supertype.vertex().clone().into_owned())
    }

    pub fn get_supertypes<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, Vec<RelationType<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_supertypes(self.clone().into_owned())
    }

    // fn get_subtypes(&self) -> MaybeOwns<'m, Vec<RelationType<'static>>>;

    pub fn get_annotations<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<RelationTypeAnnotation>>, ConceptReadError> {
        type_manager.get_relation_type_annotations(self.clone().into_owned())
    }

    pub(crate) fn set_annotation(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, annotation: RelationTypeAnnotation) {
        match annotation {
            RelationTypeAnnotation::Abstract(_) => {
                type_manager.set_storage_annotation_abstract(self.vertex().clone().into_owned())
            }
        }
    }

    fn delete_annotation(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, annotation: RelationTypeAnnotation) {
        match annotation {
            RelationTypeAnnotation::Abstract(_) => {
                type_manager.delete_storage_annotation_abstract(self.vertex().clone().into_owned())
            }
        }
    }

    pub fn get_role(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
        name: &str,
    ) -> Result<Option<RoleType<'static>>, ConceptReadError> {
        let label = Label::build_scoped(name, self.get_label(type_manager)?.name().as_str());
        type_manager.get_role_type(&label)
    }

    pub fn create_relates(
        &self,
        type_manager: &TypeManager<'_, impl WritableSnapshot>,
        name: &str,
    ) {
        let label = Label::build_scoped(name, self.get_label(type_manager).unwrap().name().as_str());
        type_manager.create_role_type(&label, self.clone().into_owned(), false);
    }

    fn delete_relates(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, role_type: RoleType<'static>) {
        type_manager.delete_storage_relates(self.vertex().clone().into_owned(), role_type.into_vertex())
    }

    fn get_relates<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Relates<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_relates(self.clone().into_owned())
    }

    pub fn get_relates_role(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
        name: &str,
    ) -> Result<Option<Relates<'static>>, ConceptReadError> {
        Ok(self.get_role(type_manager, name)?.map(|role_type| Relates::new(self.clone().into_owned(), role_type)))
    }

    fn has_relates_role(&self, type_manager: &TypeManager<'_, impl ReadableSnapshot>, name: &str) -> Result<bool, ConceptReadError> {
        Ok(self.get_relates_role(type_manager, name)?.is_some())
    }

    fn into_owned(self) -> RelationType<'static> {
        RelationType { vertex: self.vertex.into_owned() }
    }
}

impl<'a> OwnerAPI<'a> for RelationType<'a> {
    fn set_owns(
        &self,
        type_manager: &TypeManager<'_, impl WritableSnapshot>,
        attribute_type: AttributeType<'static>,
    ) {
        type_manager.set_storage_owns(self.vertex().clone().into_owned(), attribute_type.clone().into_vertex());
    }

    fn delete_owns(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, attribute_type: AttributeType<'static>) {
        // TODO: error if not owned?
        type_manager.delete_storage_owns(self.vertex().clone().into_owned(), attribute_type.into_vertex())
    }

    fn get_owns<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_owns(self.clone().into_owned())
    }

    fn get_owns_attribute(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<Owns<'static>>, ConceptReadError> {
        let expected_owns = Owns::new(ObjectType::Relation(self.clone().into_owned()), attribute_type);
        Ok(self.get_owns(type_manager)?.contains(&expected_owns).then_some(expected_owns))
    }
}

impl<'a> PlayerAPI<'a> for RelationType<'a> {
    fn set_plays(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, role_type: RoleType<'static>) {
        // TODO: decide behaviour (ok or error) if already playing
        type_manager.set_storage_plays(self.vertex().clone().into_owned(), role_type.clone().into_vertex());
    }

    fn delete_plays(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, role_type: RoleType<'static>) {
        // TODO: error if not playing?
        type_manager.delete_storage_plays(self.vertex().clone().into_owned(), role_type.into_vertex())
    }

    fn get_plays<'m>(
        &self,
        _type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        todo!()
        // type_manager.get_relation_type_plays(self.clone().into_owned())
    }

    fn get_plays_role(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
        role_type: RoleType<'static>,
    ) -> Result<Option<Plays<'static>>, ConceptReadError> {
        let expected_plays = Plays::new(ObjectType::Relation(self.clone().into_owned()), role_type);
        Ok(self.get_plays(type_manager)?.contains(&expected_plays).then_some(expected_plays))
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
            Annotation::Duplicate(_) => unreachable!("Duplicate annotation not available for Relation type."),
            Annotation::Independent(_) => unreachable!("Independent annotation not available for Relation type."),
        }
    }
}

// TODO: can we inline this into the macro invocation?
fn storage_key_to_relation_type(storage_key_ref: StorageKeyReference<'_>) -> RelationType<'_> {
    RelationType::new(new_vertex_relation_type(Bytes::Reference(storage_key_ref.byte_ref())))
}

concept_iterator!(RelationTypeIterator, RelationType, storage_key_to_relation_type);
