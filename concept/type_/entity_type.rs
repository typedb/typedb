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
use std::{collections::HashSet, ops::Deref};

use bytes::{byte_reference::ByteReference, Bytes};
use encoding::{
    graph::type_::vertex::{new_vertex_entity_type, TypeVertex},
    layout::prefix::Prefix,
    value::label::Label,
    Prefixed,
};
use primitive::maybe_owns::MaybeOwns;
use storage::{
    key_value::StorageKeyReference,
    snapshot::{iterator::SnapshotRangeIterator, SnapshotError},
};

use crate::{
    concept_iterator,
    error::{ConceptError, ConceptErrorKind, ConceptReadError, ConceptWriteError},
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
    pub fn is_root<D>(&self, type_manager: &TypeManager<'_, '_, D>) -> bool {
        type_manager.get_entity_type_is_root(self.clone().into_owned())
    }

    pub fn get_label<'m, D>(&self, type_manager: &'m TypeManager<'_, '_, D>) -> MaybeOwns<'m, Label<'static>> {
        type_manager.get_entity_type_label(self.clone().into_owned())
    }

    fn set_label<D>(&self, type_manager: &TypeManager<'_, '_, D>, label: &Label<'_>) -> Result<(), ConceptWriteError> {
        // TODO: setLabel should fail is setting label on Root type
        type_manager.set_storage_label(self.vertex().clone().into_owned(), label)
    }

    pub fn get_supertype<D>(&self, type_manager: &TypeManager<'_, '_, D>) -> Option<EntityType<'static>> {
        type_manager.get_entity_type_supertype(self.clone().into_owned())
    }

    pub fn set_supertype<D>(
        &self,
        type_manager: &TypeManager<'_, '_, D>,
        supertype: EntityType<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_storage_supertype(self.vertex().clone().into_owned(), supertype.vertex().clone().into_owned())
    }

    pub fn get_supertypes<'m, D>(
        &self,
        type_manager: &'m TypeManager<'_, '_, D>,
    ) -> MaybeOwns<'m, Vec<EntityType<'static>>> {
        type_manager.get_entity_type_supertypes(self.clone().into_owned())
    }

    // fn get_subtypes(&self) -> MaybeOwns<'m, Vec<EntityType<'static>>>;

    pub fn get_annotations<'m, D>(
        &self,
        type_manager: &'m TypeManager<'_, '_, D>,
    ) -> Result<MaybeOwns<'m, HashSet<EntityTypeAnnotation>>, ConceptReadError> {
        type_manager.get_entity_type_annotations(self.clone().into_owned())
    }

    pub fn set_annotation<D>(
        &self,
        type_manager: &TypeManager<'_, '_, D>,
        annotation: EntityTypeAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            EntityTypeAnnotation::Abstract(_) => {
                type_manager.set_storage_annotation_abstract(self.vertex().clone().into_owned())
            }
        }
    }

    fn delete_annotation<D>(&self, type_manager: &TypeManager<'_, '_, D>, annotation: EntityTypeAnnotation) {
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
    fn set_owns<D>(
        &self,
        type_manager: &TypeManager<'_, '_, D>,
        attribute_type: AttributeType<'static>,
    ) -> Result<Owns<'static>, ConceptWriteError> {
        type_manager.set_storage_owns(self.vertex().clone().into_owned(), attribute_type.clone().into_vertex())?;
        Ok(self.get_owns_attribute(type_manager, attribute_type).unwrap())
    }

    fn delete_owns<D>(&self, type_manager: &TypeManager<'_, '_, D>, attribute_type: AttributeType<'static>) {
        // TODO: error if not owned?
        type_manager.delete_storage_owns(self.vertex().clone().into_owned(), attribute_type.into_vertex());
    }

    fn get_owns<'m, D>(&self, type_manager: &'m TypeManager<'_, '_, D>) -> MaybeOwns<'m, HashSet<Owns<'static>>> {
        type_manager.get_entity_type_owns(self.clone().into_owned())
    }

    fn get_owns_attribute<D>(
        &self,
        type_manager: &TypeManager<'_, '_, D>,
        attribute_type: AttributeType<'static>,
    ) -> Option<Owns<'static>> {
        let expected_owns = Owns::new(ObjectType::Entity(self.clone().into_owned()), attribute_type);
        if self.get_owns(type_manager).deref().contains(&expected_owns) {
            Some(expected_owns)
        } else {
            None
        }
    }
}

impl<'a> PlayerAPI<'a> for EntityType<'a> {
    fn set_plays<D>(
        &self,
        type_manager: &TypeManager<'_, '_, D>,
        role_type: RoleType<'static>,
    ) -> Result<Plays<'static>, ConceptWriteError> {
        // TODO: decide behaviour (ok or error) if already playing
        type_manager.set_storage_plays(self.vertex().clone().into_owned(), role_type.clone().into_vertex())?;
        Ok(self.get_plays_role(type_manager, role_type).unwrap())
    }

    fn delete_plays<D>(&self, type_manager: &TypeManager<'_, '_, D>, role_type: RoleType<'static>) {
        // TODO: error if not playing
        type_manager.delete_storage_plays(self.vertex().clone().into_owned(), role_type.into_vertex())
    }

    fn get_plays<'m, D>(&self, type_manager: &'m TypeManager<'_, '_, D>) -> MaybeOwns<'m, HashSet<Plays<'static>>> {
        type_manager.get_entity_type_plays(self.clone().into_owned())
    }

    fn get_plays_role<D>(
        &self,
        type_manager: &TypeManager<'_, '_, D>,
        role_type: RoleType<'static>,
    ) -> Option<Plays<'static>> {
        let expected_plays = Plays::new(ObjectType::Entity(self.clone().into_owned()), role_type);
        if self.get_plays(type_manager).deref().contains(&expected_plays) {
            Some(expected_plays)
        } else {
            None
        }
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
