/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    sync::Arc,
};

use encoding::{
    error::{EncodingError, EncodingError::UnexpectedPrefix},
    graph::{
        type_::{
            vertex::{PrefixedTypeVertexEncoding, TypeVertex, TypeVertexEncoding},
            Kind,
        },
        Typed,
    },
    layout::prefix::Prefix,
    value::label::Label,
    Prefixed,
};
use lending_iterator::higher_order::Hkt;
use primitive::maybe_owns::MaybeOwns;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    key_value::StorageKey,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use super::{Capability, Ordering};
use crate::{
    concept_iterator,
    error::{ConceptReadError, ConceptWriteError},
    thing::thing_manager::ThingManager,
    type_::{
        annotation::{Annotation, AnnotationError},
        constraint::{CapabilityConstraint, TypeConstraint},
        object_type::ObjectType,
        plays::Plays,
        relates::Relates,
        relation_type::RelationType,
        type_manager::TypeManager,
        KindAPI, TypeAPI,
    },
    ConceptAPI,
};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RoleType<'a> {
    vertex: TypeVertex<'a>,
}

impl Hkt for RoleType<'static> {
    type HktSelf<'a> = Self;
}

impl<'a> ConceptAPI<'a> for RoleType<'a> {}

impl<'a> PrefixedTypeVertexEncoding<'a> for RoleType<'a> {
    const PREFIX: Prefix = Prefix::VertexRoleType;
}

impl<'a> TypeVertexEncoding<'a> for RoleType<'a> {
    fn from_vertex(vertex: TypeVertex<'a>) -> Result<Self, EncodingError> {
        debug_assert!(Self::PREFIX == Prefix::VertexRoleType);
        if vertex.prefix() != Prefix::VertexRoleType {
            Err(UnexpectedPrefix { expected_prefix: Prefix::VertexRoleType, actual_prefix: vertex.prefix() })
        } else {
            Ok(RoleType { vertex })
        }
    }

    fn vertex(&self) -> TypeVertex<'_> {
        self.vertex.as_reference()
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }
}

impl<'a> TypeAPI<'a> for RoleType<'a> {
    type SelfStatic = RoleType<'static>;

    fn new(vertex: TypeVertex<'a>) -> RoleType<'_> {
        Self::from_vertex(vertex).unwrap()
    }

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        self.get_relates_root(snapshot, type_manager)?.is_abstract(snapshot, type_manager)
    }

    fn delete(
        self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptWriteError> {
        type_manager.delete_role_type(snapshot, thing_manager, self.into_owned())
    }

    fn get_label<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_role_type_label(snapshot, self.clone().into_owned())
    }

    fn get_label_arc(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Arc<Label<'static>>, ConceptReadError> {
        type_manager.get_role_type_label_arc(snapshot, self.clone().into_owned())
    }

    fn get_supertype(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<RoleType<'static>>, ConceptReadError> {
        type_manager.get_role_type_supertype(snapshot, self.clone().into_owned())
    }

    fn get_supertypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<RoleType<'static>>>, ConceptReadError> {
        type_manager.get_role_type_supertypes(snapshot, self.clone().into_owned())
    }

    fn get_subtypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<RoleType<'static>>>, ConceptReadError> {
        type_manager.get_role_type_subtypes(snapshot, self.clone().into_owned())
    }

    fn get_subtypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<RoleType<'static>>>, ConceptReadError> {
        type_manager.get_role_type_subtypes_transitive(snapshot, self.clone().into_owned())
    }
}

impl<'a> KindAPI<'a> for RoleType<'a> {
    type AnnotationType = RoleTypeAnnotation;
    const KIND: Kind = Kind::Role;

    fn get_annotations_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<RoleTypeAnnotation>>, ConceptReadError> {
        type_manager.get_role_type_annotations_declared(snapshot, self.clone().into_owned())
    }

    fn get_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<TypeConstraint<RoleType<'static>>>>, ConceptReadError>
    where
        'a: 'static,
    {
        type_manager.get_role_type_constraints(snapshot, self.clone().into_owned())
    }
}

impl<'a> RoleType<'a> {
    pub fn set_name(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        name: &str,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_role_type_name(snapshot, self.clone().into_owned(), name)
    }

    pub fn get_ordering(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Ordering, ConceptReadError> {
        type_manager.get_role_type_ordering(snapshot, self.clone().into_owned())
    }

    pub fn set_ordering(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        ordering: Ordering,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_role_ordering(snapshot, thing_manager, self.clone().into_owned(), ordering)
    }

    pub fn into_owned(self) -> RoleType<'static> {
        RoleType { vertex: self.vertex.into_owned() }
    }
}

// --- Related API ---
impl<'a> RoleType<'a> {
    pub fn get_relates_root(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Relates<'static>, ConceptReadError> {
        type_manager.get_role_type_relates_root(snapshot, self.clone().into_owned())
    }

    pub fn get_relates<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Relates<'static>>>, ConceptReadError> {
        type_manager.get_role_type_relates(snapshot, self.clone().into_owned())
    }

    pub fn get_relation_types<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashMap<RelationType<'static>, Relates<'static>>>, ConceptReadError> {
        type_manager.get_role_type_relation_types(snapshot, self.clone().into_owned())
    }

    pub fn get_constraints_for_relation<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Relates<'static>>>>, ConceptReadError> {
        type_manager.get_relation_type_related_role_type_constraints(snapshot, relation_type, self.clone().into_owned())
    }
}

// --- Played API ---
impl<'a> RoleType<'a> {
    pub fn get_plays<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        type_manager.get_role_type_plays(snapshot, self.clone().into_owned())
    }

    pub fn get_player_types<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashMap<ObjectType<'static>, Plays<'static>>>, ConceptReadError> {
        type_manager.get_role_type_player_types(snapshot, self.clone().into_owned())
    }

    pub fn get_constraints_for_player<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        player_type: ObjectType<'static>,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Plays<'static>>>>, ConceptReadError> {
        match player_type {
            ObjectType::Entity(entity_type) => type_manager.get_entity_type_played_role_type_constraints(
                snapshot,
                entity_type,
                self.clone().into_owned(),
            ),
            ObjectType::Relation(relation_type) => type_manager.get_relation_type_played_role_type_constraints(
                snapshot,
                relation_type,
                self.clone().into_owned(),
            ),
        }
    }
}

impl<'a> Display for RoleType<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[RoleType:{}]", self.vertex.type_id_())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RoleTypeAnnotation {}

impl TryFrom<Annotation> for RoleTypeAnnotation {
    type Error = AnnotationError;
    fn try_from(annotation: Annotation) -> Result<RoleTypeAnnotation, AnnotationError> {
        match annotation {
            | Annotation::Abstract(_)
            | Annotation::Independent(_)
            | Annotation::Distinct(_)
            | Annotation::Cardinality(_)
            | Annotation::Unique(_)
            | Annotation::Key(_)
            | Annotation::Regex(_)
            | Annotation::Cascade(_)
            | Annotation::Range(_)
            | Annotation::Values(_) => Err(AnnotationError::UnsupportedAnnotationForRoleType(annotation.category())),
        }
    }
}

impl From<RoleTypeAnnotation> for Annotation {
    fn from(_anno: RoleTypeAnnotation) -> Self {
        unreachable!("RoleTypes do not have annotations!")
    }
}

// TODO: can we inline this into the macro invocation?
fn storage_key_to_role_type(storage_key: StorageKey<'_, BUFFER_KEY_INLINE>) -> RoleType<'_> {
    RoleType::read_from(storage_key.into_bytes())
}

concept_iterator!(RoleTypeIterator, RoleType, storage_key_to_role_type);
