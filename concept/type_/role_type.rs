/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use encoding::{
    graph::type_::vertex::TypeVertex,
    layout::prefix::Prefix,
    value::label::Label,
    Prefixed,
};
use lending_iterator::higher_order::Hkt;
use encoding::graph::type_::vertex::{EncodableTypeVertex, PrefixedEncodableTypeVertex};
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
        annotation::{Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationDistinct},
        plays::Plays,
        relates::Relates,
        type_manager::TypeManager,
        TypeAPI,
    },
    ConceptAPI,
};
use crate::type_::object_type::ObjectType;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RoleType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> RoleType<'a> {
    pub fn get_players<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        type_manager.get_plays_for_role_type(snapshot, self.clone().into_owned())
    }

    pub fn get_players_transitive<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashMap<Plays<'static>, Vec<ObjectType<'static>>>>, ConceptReadError> {
        type_manager.get_plays_for_role_type_transitive(snapshot, self.clone().into_owned())
    }
}

impl Hkt for RoleType<'static> {
    type HktSelf<'a> = Self;
}

impl<'a> ConceptAPI<'a> for RoleType<'a> {}

impl<'a> PrefixedEncodableTypeVertex<'a> for RoleType<'a> {
    const PREFIX: Prefix = Prefix::VertexRoleType;
}

impl<'a> EncodableTypeVertex<'a> for RoleType<'a> {
    fn from_vertex(vertex: TypeVertex<'a>) -> Self {
        debug_assert_eq!(vertex.prefix(), Prefix::VertexRoleType);
        RoleType { vertex }
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }
}

impl<'a> TypeAPI<'a> for RoleType<'a> {
    type SelfStatic = RoleType<'static>;

    fn new(vertex: TypeVertex<'a>) -> RoleType<'_> {
        Self::from_vertex(vertex)
    }
    fn vertex<'this>(&'this self) -> TypeVertex<'this> {
        self.vertex.as_reference()
    }
    fn is_abstract<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<bool, ConceptReadError> {
        let annotations = self.get_annotations(snapshot, type_manager)?;
        Ok(annotations.contains(&RoleTypeAnnotation::Abstract(AnnotationAbstract)))
    }

    fn delete<Snapshot: WritableSnapshot>(
        self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<(), ConceptWriteError> {
        // TODO: validation (Or better it in type_manager)
        type_manager.delete_role_type(snapshot, self)
    }

    fn get_label<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_role_type_label(snapshot, self.clone().into_owned())
    }
}

impl<'a> RoleType<'a> {
    pub fn is_root<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<bool, ConceptReadError> {
        type_manager.get_role_type_is_root(snapshot, self.clone().into_owned())
    }

    pub fn set_name<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        _type_manager: &TypeManager<Snapshot>,
        _name: &str,
    ) {
        // // TODO: setLabel should fail is setting label on Root type
        // type_manager.set_storage_label(self.clone().into_owned(), label);
        todo!()
    }

    pub fn get_supertype<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<Option<RoleType<'_>>, ConceptReadError> {
        type_manager.get_role_type_supertype(snapshot, self.clone().into_owned())
    }

    pub fn set_supertype<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        supertype: RoleType<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_role_type_supertype(snapshot, self.clone().into_owned(), supertype)
    }

    pub fn get_supertypes<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Vec<RoleType<'static>>>, ConceptReadError> {
        type_manager.get_role_type_supertypes(snapshot, self.clone().into_owned())
    }

    pub fn get_subtypes<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Vec<RoleType<'static>>>, ConceptReadError> {
        type_manager.get_role_type_subtypes(snapshot, self.clone().into_owned())
    }

    pub fn get_subtypes_transitive<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Vec<RoleType<'static>>>, ConceptReadError> {
        type_manager.get_role_type_subtypes_transitive(snapshot, self.clone().into_owned())
    }

    pub fn get_cardinality<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<AnnotationCardinality, ConceptReadError> {
        let annotations = self.get_annotations(snapshot, type_manager)?;
        let card: AnnotationCardinality = annotations
            .iter()
            .filter_map(|annotation| match annotation {
                RoleTypeAnnotation::Cardinality(card) => Some(card.clone()),
                _ => None,
            })
            .next()
            .unwrap_or_else(|| type_manager.role_default_cardinality());
        Ok(card)
    }

    pub fn get_annotations<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<RoleTypeAnnotation>>, ConceptReadError> {
        type_manager.get_role_type_annotations(snapshot, self.clone().into_owned())
    }

    pub fn set_annotation<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        annotation: RoleTypeAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            RoleTypeAnnotation::Abstract(_) => {
                type_manager.storage_set_annotation_abstract(snapshot, self.clone().into_owned())
            }
            RoleTypeAnnotation::Distinct(_) => {
                type_manager.storage_set_annotation_distinct(snapshot, self.clone().into_owned())
            }
            RoleTypeAnnotation::Cardinality(cardinality) => {
                type_manager.storage_set_annotation_cardinality(snapshot, self.clone().into_owned(), cardinality)
            }
        };
        Ok(())
    }

    fn delete_annotation<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        annotation: RoleTypeAnnotation,
    ) {
        match annotation {
            RoleTypeAnnotation::Abstract(_) => {
                type_manager.storage_delete_annotation_abstract(snapshot, self.clone().into_owned())
            }
            RoleTypeAnnotation::Distinct(_) => {
                type_manager.storage_delete_annotation_distinct(snapshot, self.clone().into_owned())
            }
            RoleTypeAnnotation::Cardinality(_) => {
                type_manager.storage_delete_annotation_cardinality(snapshot, self.clone().into_owned())
            }
        }
    }

    fn get_relates<Snapshot: ReadableSnapshot>(&self, _type_manager: &TypeManager<Snapshot>) -> Relates<'static> {
        todo!()
    }

    pub fn into_owned(self) -> RoleType<'static> {
        RoleType { vertex: self.vertex.into_owned() }
    }
}

// --- Played API ---
impl<'a> RoleType<'a> {
    pub fn get_plays<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        type_manager.get_plays_for_role_type(snapshot, self.clone().into_owned())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RoleTypeAnnotation {
    Abstract(AnnotationAbstract),
    Distinct(AnnotationDistinct),
    Cardinality(AnnotationCardinality),
}

impl From<Annotation> for RoleTypeAnnotation {
    fn from(annotation: Annotation) -> Self {
        match annotation {
            Annotation::Abstract(annotation) => RoleTypeAnnotation::Abstract(annotation),
            Annotation::Distinct(annotation) => RoleTypeAnnotation::Distinct(annotation),
            Annotation::Cardinality(annotation) => RoleTypeAnnotation::Cardinality(annotation),

            Annotation::Independent(_) => unreachable!("Independent annotation not available for Role type."),
            Annotation::Unique(_) => unreachable!("Unique annotation not available for Role type."),
            Annotation::Key(_) => unreachable!("Key annotation not available for Role type."),
            Annotation::Regex(_) => unreachable!("Regex annotation not available for Role type."),
        }
    }
}

// impl<'a> IIDAPI<'a> for RoleType<'a> {
//     fn iid(&'a self) -> ByteReference<'a> {
//         self.vertex.bytes()
//     }
// }

// TODO: can we inline this into the macro invocation?
fn storage_key_to_role_type(storage_key: StorageKey<'_, BUFFER_KEY_INLINE>) -> RoleType<'_> {
    RoleType::decode(storage_key.into_bytes())
}

concept_iterator!(RoleTypeIterator, RoleType, storage_key_to_role_type);
