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
    graph::type_::vertex::{new_vertex_entity_type, TypeVertex},
    layout::prefix::Prefix,
    value::label::Label,
    Prefixed,
};
use primitive::maybe_owns::MaybeOwns;
use storage::key_value::StorageKeyReference;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    concept_iterator,
    error::{ConceptReadError, ConceptWriteError},
    type_::{
        annotation::{Annotation, AnnotationAbstract},
        attribute_type::AttributeType,
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        role_type::RoleType,
        type_manager::TypeManager,
        OwnerAPI, PlayerAPI, TypeAPI,
    },
    ConceptAPI,
};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EntityType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> EntityType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> EntityType<'_> {
        if vertex.prefix() != Prefix::VertexEntityType {
            panic!(
                "Type IID prefix was expected to be Prefix::EntityType ({:?}) but was {:?}",
                Prefix::VertexEntityType,
                vertex.prefix()
            )
        }
        EntityType { vertex }
    }
}

impl<'a> ConceptAPI<'a> for EntityType<'a> {}

impl<'a> TypeAPI<'a> for EntityType<'a> {
    fn vertex<'this>(&'this self) -> &'this TypeVertex<'a> {
        &self.vertex
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }
}

impl<'a> EntityType<'a> {
    pub fn is_root(&self, type_manager: &TypeManager<'_, impl ReadableSnapshot>) -> Result<bool, ConceptReadError> {
        type_manager.get_entity_type_is_root(self.clone().into_owned())
    }

    pub fn get_label<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_entity_type_label(self.clone().into_owned())
    }

    fn set_label(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, label: &Label<'_>) {
        // TODO: setLabel should fail is setting label on Root type
        type_manager.set_storage_label(self.vertex().clone().into_owned(), label)
    }

    pub fn get_supertype(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<Option<EntityType<'_>>, ConceptReadError> {
        type_manager.get_entity_type_supertype(self.clone().into_owned())
    }

    pub fn set_supertype(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, supertype: EntityType<'static>) {
        type_manager.set_storage_supertype(self.vertex().clone().into_owned(), supertype.vertex().clone().into_owned())
    }

    pub fn get_supertypes<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, Vec<EntityType<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_supertypes(self.clone().into_owned())
    }

    // fn get_subtypes(&self) -> MaybeOwns<'m, Vec<EntityType<'static>>>;

    pub fn get_annotations<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<EntityTypeAnnotation>>, ConceptReadError> {
        type_manager.get_entity_type_annotations(self.clone().into_owned())
    }

    pub fn set_annotation(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, annotation: EntityTypeAnnotation) {
        match annotation {
            EntityTypeAnnotation::Abstract(_) => {
                type_manager.set_storage_annotation_abstract(self.vertex().clone().into_owned())
            }
        }
    }

    fn delete_annotation(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, annotation: EntityTypeAnnotation) {
        match annotation {
            EntityTypeAnnotation::Abstract(_) => {
                type_manager.delete_storage_annotation_abstract(self.vertex().clone().into_owned())
            }
        }
    }

    fn into_owned(self) -> EntityType<'static> {
        EntityType { vertex: self.vertex.into_owned() }
    }
}

impl<'a> OwnerAPI<'a> for EntityType<'a> {
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
        type_manager.get_entity_type_owns(self.clone().into_owned())
    }

    fn get_owns_attribute(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<Owns<'static>>, ConceptReadError> {
        let expected_owns = Owns::new(ObjectType::Entity(self.clone().into_owned()), attribute_type);
        Ok(self.get_owns(type_manager)?.contains(&expected_owns).then_some(expected_owns))
    }
}

impl<'a> PlayerAPI<'a> for EntityType<'a> {
    fn set_plays(
        &self,
        type_manager: &TypeManager<'_, impl WritableSnapshot>,
        role_type: RoleType<'static>,
    ) {
        // TODO: decide behaviour (ok or error) if already playing
        type_manager.set_storage_plays(self.vertex().clone().into_owned(), role_type.clone().into_vertex());
    }

    fn delete_plays(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, role_type: RoleType<'static>) {
        // TODO: error if not playing
        type_manager.delete_storage_plays(self.vertex().clone().into_owned(), role_type.into_vertex())
    }

    fn get_plays<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_plays(self.clone().into_owned())
    }

    fn get_plays_role(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
        role_type: RoleType<'static>,
    ) -> Result<Option<Plays<'static>>, ConceptReadError> {
        let expected_plays = Plays::new(ObjectType::Entity(self.clone().into_owned()), role_type);
        Ok(self.get_plays(type_manager)?.contains(&expected_plays).then_some(expected_plays))
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum EntityTypeAnnotation {
    Abstract(AnnotationAbstract),
}

impl From<Annotation> for EntityTypeAnnotation {
    fn from(annotation: Annotation) -> Self {
        match annotation {
            Annotation::Abstract(annotation) => EntityTypeAnnotation::Abstract(annotation),
            Annotation::Duplicate(_) => unreachable!("Duplicate annotation not available for Entity type."),
            Annotation::Independent(_) => unreachable!("Independent annotation not available for Entity type."),
        }
    }
}

// impl<'a> IIDAPI<'a> for EntityType<'a> {
//     fn iid(&'a self) -> ByteReference<'a> {
//         self.vertex.bytes()
//     }
// }

// TODO: can we inline this into the macro invocation?
fn storage_key_ref_to_entity_type(storage_key_ref: StorageKeyReference<'_>) -> EntityType<'_> {
    EntityType::new(new_vertex_entity_type(Bytes::Reference(storage_key_ref.byte_ref())))
}

concept_iterator!(EntityTypeIterator, EntityType, storage_key_ref_to_entity_type);
