/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use encoding::{
    error::{EncodingError, EncodingError::UnexpectedPrefix},
    graph::type_::{
        vertex::{PrefixedTypeVertexEncoding, TypeVertex, TypeVertexEncoding},
        Kind,
    },
    layout::prefix::{Prefix, Prefix::VertexEntityType},
    value::label::Label,
    Prefixed,
};
use primitive::maybe_owns::MaybeOwns;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    key_value::StorageKey,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

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
        KindAPI, ObjectTypeAPI, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
    },
    ConceptAPI,
};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EntityType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> EntityType<'a> {}

impl<'a> ConceptAPI<'a> for EntityType<'a> {}

impl<'a> PrefixedTypeVertexEncoding<'a> for EntityType<'a> {
    const PREFIX: Prefix = VertexEntityType;
}
impl<'a> TypeVertexEncoding<'a> for EntityType<'a> {
    fn from_vertex(vertex: TypeVertex<'a>) -> Result<Self, EncodingError> {
        debug_assert!(Self::PREFIX == VertexEntityType);
        if vertex.prefix() != Prefix::VertexEntityType {
            Err(UnexpectedPrefix { expected_prefix: Prefix::VertexEntityType, actual_prefix: vertex.prefix() })
        } else {
            Ok(EntityType { vertex })
        }
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }
}

impl<'a> TypeAPI<'a> for EntityType<'a> {
    type SelfStatic = EntityType<'static>;
    fn new(vertex: TypeVertex<'a>) -> EntityType<'a> {
        Self::from_vertex(vertex).unwrap()
    }

    fn vertex<'this>(&'this self) -> TypeVertex<'this> {
        self.vertex.as_reference()
    }

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        let annotations = self.get_annotations(snapshot, type_manager)?;
        Ok(annotations.contains(&EntityTypeAnnotation::Abstract(AnnotationAbstract)))
    }

    fn delete(self, snapshot: &mut impl WritableSnapshot, type_manager: &TypeManager) -> Result<(), ConceptWriteError> {
        // todo!("Validation");
        type_manager.delete_entity_type(snapshot, self)
    }

    fn get_label<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_entity_type_label(snapshot, self.clone().into_owned())
    }
}

impl<'a> ObjectTypeAPI<'a> for EntityType<'a> {
    fn into_owned_object_type(self) -> ObjectType<'static> {
        ObjectType::Entity(self.into_owned())
    }
}

impl<'a> KindAPI<'a> for EntityType<'a> {
    type AnnotationType = EntityTypeAnnotation;
    const ROOT_KIND: Kind = Kind::Entity;
}

impl<'a> EntityType<'a> {
    pub fn is_root(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        type_manager.get_entity_type_is_root(snapshot, self.clone().into_owned())
    }

    pub fn set_label(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        label: &Label<'_>,
    ) -> Result<(), ConceptWriteError> {
        if self.is_root(snapshot, type_manager)? {
            Err(ConceptWriteError::RootModification)
        } else {
            type_manager.set_label(snapshot, self.clone().into_owned(), label)
        }
    }

    pub fn get_supertype(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<EntityType<'_>>, ConceptReadError> {
        type_manager.get_entity_type_supertype(snapshot, self.clone().into_owned())
    }

    pub fn set_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        supertype: EntityType<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_entity_type_supertype(snapshot, self.clone().into_owned(), supertype)
    }

    pub fn get_supertypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<EntityType<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_supertypes(snapshot, self.clone().into_owned())
    }

    pub fn get_subtypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<EntityType<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_subtypes(snapshot, self.clone().into_owned())
    }

    pub fn get_subtypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<EntityType<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_subtypes_transitive(snapshot, self.clone().into_owned())
    }

    pub fn get_annotations<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<EntityTypeAnnotation>>, ConceptReadError> {
        type_manager.get_entity_type_annotations(snapshot, self.clone().into_owned())
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        annotation: EntityTypeAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            EntityTypeAnnotation::Abstract(_) => {
                type_manager.set_annotation_abstract(snapshot, self.clone().into_owned())
            }
        };
        Ok(())
    }

    fn delete_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        annotation: EntityTypeAnnotation,
    ) {
        match annotation {
            EntityTypeAnnotation::Abstract(_) => {
                type_manager.delete_annotation_abstract(snapshot, self.clone().into_owned())
            }
        }
    }

    pub fn into_owned(self) -> EntityType<'static> {
        EntityType { vertex: self.vertex.into_owned() }
    }
}

impl<'a> OwnerAPI<'a> for EntityType<'a> {
    fn set_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
        ordering: Ordering,
    ) -> Result<Owns<'static>, ConceptWriteError> {
        type_manager.set_owns(snapshot, self.clone().into_owned(), attribute_type.clone(), ordering);
        Ok(Owns::new(ObjectType::Entity(self.clone().into_owned()), attribute_type))
    }

    fn delete_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        // TODO: error if not owned?
        type_manager.delete_owns(snapshot, self.clone().into_owned(), attribute_type);
        Ok(())
    }

    fn get_owns<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_owns(snapshot, self.clone().into_owned())
    }

    fn get_owns_attribute(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<Owns<'static>>, ConceptReadError> {
        let expected_owns = Owns::new(ObjectType::Entity(self.clone().into_owned()), attribute_type);
        Ok(self.get_owns(snapshot, type_manager)?.contains(&expected_owns).then_some(expected_owns))
    }

    fn get_owns_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashMap<AttributeType<'static>, Owns<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_owns_transitive(snapshot, self.clone().into_owned())
    }
}

impl<'a> PlayerAPI<'a> for EntityType<'a> {
    fn set_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<Plays<'static>, ConceptWriteError> {
        // TODO: decide behaviour (ok or error) if already playing
        let plays = type_manager.set_plays(snapshot, self.clone().into_owned(), role_type.clone())?;
        Ok(Plays::new(ObjectType::Entity(self.clone().into_owned()), role_type))
    }

    fn delete_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<(), ConceptWriteError> {
        // TODO: error if not playing
        type_manager.delete_plays(snapshot, self.clone().into_owned(), role_type)
    }

    fn get_plays<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_plays(snapshot, self.clone().into_owned())
    }

    fn get_plays_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<Option<Plays<'static>>, ConceptReadError> {
        let expected_plays = Plays::new(ObjectType::Entity(self.clone().into_owned()), role_type);
        Ok(self.get_plays(snapshot, type_manager)?.contains(&expected_plays).then_some(expected_plays))
    }

    fn get_plays_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashMap<RoleType<'static>, Plays<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_plays_transitive(snapshot, self.clone().into_owned())
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

            Annotation::Distinct(_) => unreachable!("Distinct annotation not available for Entity type."),
            Annotation::Independent(_) => unreachable!("Independent annotation not available for Entity type."),
            Annotation::Unique(_) => unreachable!("Unique annotation not available for Entity type."),
            Annotation::Key(_) => unreachable!("Key annotation not available for Entity type."),
            Annotation::Cardinality(_) => unreachable!("Cardinality annotation not available for Entity type."),
            Annotation::Regex(_) => unreachable!("Regex annotation not available for Entity type."),
        }
    }
}

// impl<'a> IIDAPI<'a> for EntityType<'a> {
//     fn iid(&'a self) -> ByteReference<'a> {
//         self.vertex.bytes()
//     }
// }

// TODO: can we inline this into the macro invocation?
fn storage_key_to_entity_type(storage_key: StorageKey<'_, BUFFER_KEY_INLINE>) -> EntityType<'_> {
    EntityType::read_from(storage_key.into_bytes())
}

concept_iterator!(EntityTypeIterator, EntityType, storage_key_to_entity_type);
