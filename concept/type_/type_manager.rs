/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    marker::PhantomData,
    sync::Arc,
};

use bytes::{byte_array::ByteArray, Bytes};
use durability::DurabilityService;
use encoding::{
    graph::type_::{
        edge::{
            build_edge_owns, build_edge_owns_reverse, build_edge_plays, build_edge_plays_reverse, build_edge_relates,
            build_edge_relates_reverse, build_edge_sub, build_edge_sub_reverse, TypeEdge,
        },
        index::LabelToTypeVertexIndex,
        property::{
            build_property_type_annotation_abstract, build_property_type_annotation_cardinality,
            build_property_type_annotation_distinct, build_property_type_annotation_independent,
            build_property_type_annotation_regex, build_property_type_edge_annotation_cardinality,
            build_property_type_edge_annotation_distinct, build_property_type_edge_annotation_key,
            build_property_type_edge_ordering, build_property_type_edge_override, build_property_type_label,
            build_property_type_ordering, build_property_type_value_type,
        },
        vertex::{new_vertex_attribute_type, new_vertex_entity_type, new_vertex_relation_type, new_vertex_role_type},
        vertex_generator::TypeVertexGenerator,
        Kind,
    },
    layout::prefix::Prefix,
    value::{label::Label, value_type::ValueType},
    AsBytes, Keyable,
};
use primitive::maybe_owns::MaybeOwns;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    snapshot::{CommittableSnapshot, ReadableSnapshot, WritableSnapshot, WriteSnapshot},
    MVCCStorage,
};

use super::annotation::AnnotationRegex;
use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::ObjectAPI,
    type_::{
        annotation::{Annotation, AnnotationAbstract, AnnotationCardinality},
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        entity_type::{EntityType, EntityTypeAnnotation},
        owns::{Owns, OwnsAnnotation},
        plays::Plays,
        relates::Relates,
        relation_type::{RelationType, RelationTypeAnnotation},
        role_type::{RoleType, RoleTypeAnnotation},
        serialise_annotation_cardinality, serialise_ordering,
        type_cache::TypeCache,
        type_reader::TypeReader,
        IntoCanonicalTypeEdge, ObjectTypeAPI, Ordering, TypeAPI,
    },
};

// TODO: this should be parametrised into the database options? Would be great to have it be changable at runtime!
pub(crate) const RELATION_INDEX_THRESHOLD: u64 = 8;

pub struct TypeManager<Snapshot> {
    vertex_generator: Arc<TypeVertexGenerator>,
    type_cache: Option<Arc<TypeCache>>,
    snapshot: PhantomData<Snapshot>,
}

impl<Snapshot> TypeManager<Snapshot> {
    pub fn initialise_types<D: DurabilityService>(
        storage: Arc<MVCCStorage<D>>,
        vertex_generator: Arc<TypeVertexGenerator>,
    ) -> Result<(), ConceptWriteError> {
        let mut snapshot = storage.clone().open_snapshot_write();
        {
            let type_manager = TypeManager::<WriteSnapshot<D>>::new(vertex_generator.clone(), None);
            let root_entity = type_manager.create_entity_type(&mut snapshot, &Kind::Entity.root_label(), true)?;
            root_entity.set_annotation(
                &mut snapshot,
                &type_manager,
                EntityTypeAnnotation::Abstract(AnnotationAbstract),
            )?;
            let root_relation = type_manager.create_relation_type(&mut snapshot, &Kind::Relation.root_label(), true)?;
            root_relation.set_annotation(
                &mut snapshot,
                &type_manager,
                RelationTypeAnnotation::Abstract(AnnotationAbstract),
            )?;
            let root_role = type_manager.create_role_type(
                &mut snapshot,
                &Kind::Role.root_label(),
                root_relation.clone(),
                true,
                Ordering::Unordered,
            )?;
            root_role.set_annotation(&mut snapshot, &type_manager, RoleTypeAnnotation::Abstract(AnnotationAbstract))?;
            let root_attribute =
                type_manager.create_attribute_type(&mut snapshot, &Kind::Attribute.root_label(), true)?;
            root_attribute.set_annotation(
                &mut snapshot,
                &type_manager,
                AttributeTypeAnnotation::Abstract(AnnotationAbstract),
            )?;
        }
        // TODO: pass error up
        snapshot.commit().unwrap();
        Ok(())
    }

    pub fn finalise(self) -> Result<(), Vec<ConceptWriteError>> {
        // todo!("Do we need to finalise anything here?");
        Ok(())
    }
}

macro_rules! get_type_methods {
    ($(
        fn $method_name:ident() -> $output_type:ident = $cache_method:ident;
    )*) => {
        $(
            pub fn $method_name(
                &self, snapshot: &Snapshot, label: &Label<'_>
            ) -> Result<Option<$output_type<'static>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(label))
                } else {
                    TypeReader::get_labelled_type::<$output_type<'static>>(snapshot, label)
                }
            }
        )*
    }
}

macro_rules! get_supertype_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &Snapshot, type_: $type_<'static>
            ) -> Result<Option<$type_<'static>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(type_))
                } else {
                    TypeReader::get_supertype(snapshot, type_)
                }
            }
        )*
    }
}

macro_rules! get_supertypes_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            // WARN: supertypes currently do NOT include themselves
            pub(crate) fn $method_name(
                &self, snapshot: &Snapshot, type_: $type_<'static>
            ) -> Result<MaybeOwns<'_, Vec<$type_<'static>>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::Borrowed(cache.$cache_method(type_)))
                } else {
                    let supertypes = TypeReader::get_supertypes_transitive(snapshot, type_)?;
                    Ok(MaybeOwns::Owned(supertypes))
                }
            }
        )*
    }
}

macro_rules! get_subtypes_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &Snapshot, type_: $type_<'static>
            ) -> Result<MaybeOwns<'_, Vec<$type_<'static>>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::Borrowed(cache.$cache_method(type_)))
                } else {
                    let subtypes = TypeReader::get_subtypes(snapshot, type_)?;
                    Ok(MaybeOwns::Owned(subtypes))
                }
            }
        )*
    }
}

macro_rules! get_subtypes_transitive_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            // WARN: supertypes currently do NOT include themselves
            pub(crate) fn $method_name(
                &self, snapshot: &Snapshot, type_: $type_<'static>
            ) -> Result<MaybeOwns<'_, Vec<$type_<'static>>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::Borrowed(cache.$cache_method(type_)))
                } else {
                    let subtypes = TypeReader::get_subtypes_transitive(snapshot, type_)?;
                    Ok(MaybeOwns::Owned(subtypes))
                }
            }
        )*
    }
}

macro_rules! get_type_is_root_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &Snapshot, type_: $type_<'static>
            ) -> Result<bool, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(type_))
                } else {
                    let type_label = TypeReader::get_label(snapshot, type_)?.unwrap();
                    Ok(Self::check_type_is_root(&type_label, $type_::ROOT_KIND))
                }
            }
        )*
    }
}

macro_rules! get_type_label_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &Snapshot, type_: $type_<'static>
            ) -> Result<MaybeOwns<'_, Label<'static>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::Borrowed(cache.$cache_method(type_)))
                } else {
                    Ok(MaybeOwns::Owned(TypeReader::get_label(snapshot, type_)?.unwrap()))
                }
            }
        )*
    }
}

macro_rules! get_type_annotations {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident | $annotation_type:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &Snapshot, type_: $type_<'static>
            ) -> Result<MaybeOwns<'_, HashSet<$annotation_type>>, ConceptReadError> {
                 if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::Borrowed(cache.$cache_method(type_)))
                } else {
                    let annotations = TypeReader::get_type_annotations(snapshot, type_)?;
                    Ok(MaybeOwns::Owned(annotations))
                }
            }
        )*
    }
}

impl<Snapshot: ReadableSnapshot> TypeManager<Snapshot> {
    pub fn new(vertex_generator: Arc<TypeVertexGenerator>, schema_cache: Option<Arc<TypeCache>>) -> Self {
        TypeManager { vertex_generator, type_cache: schema_cache, snapshot: PhantomData }
    }

    pub(crate) fn check_type_is_root(type_label: &Label<'_>, kind: Kind) -> bool {
        type_label == &kind.root_label()
    }

    pub fn resolve_relates(
        &self,
        snapshot: &Snapshot,
        relation: RelationType<'static>,
        role_name: &str,
    ) -> Result<Option<Relates<'static>>, ConceptReadError> {
        // TODO: Efficiency. We could build an index in TypeCache.
        Ok(self.get_relation_type_relates_transitive(snapshot, relation)?.iter().find_map(|(_role, relates)| {
            if self.get_role_type_label(snapshot, relates.role()).unwrap().name.as_str() == role_name {
                Some(relates.clone())
            } else {
                None
            }
        }))
    }

    get_type_methods! {
        fn get_entity_type() -> EntityType = get_entity_type;
        fn get_relation_type() -> RelationType = get_relation_type;
        fn get_role_type() -> RoleType = get_role_type;
        fn get_attribute_type() -> AttributeType = get_attribute_type;
    }

    get_supertype_methods! {
        fn get_entity_type_supertype() -> EntityType = get_supertype;
        fn get_relation_type_supertype() -> RelationType = get_supertype;
        fn get_role_type_supertype() -> RoleType = get_supertype;
        fn get_attribute_type_supertype() -> AttributeType = get_supertype;
    }

    get_supertypes_methods! {
        fn get_entity_type_supertypes() -> EntityType = get_supertypes;
        fn get_relation_type_supertypes() -> RelationType = get_supertypes;
        fn get_role_type_supertypes() -> RoleType = get_supertypes;
        fn get_attribute_type_supertypes() -> AttributeType = get_supertypes;
    }

    get_subtypes_methods! {
        fn get_entity_type_subtypes() -> EntityType = get_subtypes;
        fn get_relation_type_subtypes() -> RelationType = get_subtypes;
        fn get_role_type_subtypes() -> RoleType = get_subtypes;
        fn get_attribute_type_subtypes() -> AttributeType = get_subtypes;
    }

    get_subtypes_transitive_methods! {
        fn get_entity_type_subtypes_transitive() -> EntityType = get_subtypes_transitive;
        fn get_relation_type_subtypes_transitive() -> RelationType = get_subtypes_transitive;
        fn get_role_type_subtypes_transitive() -> RoleType = get_subtypes_transitive;
        fn get_attribute_type_subtypes_transitive() -> AttributeType = get_subtypes_transitive;
    }

    get_type_is_root_methods! {
        fn get_entity_type_is_root() -> EntityType = is_root;
        fn get_relation_type_is_root() -> RelationType = is_root;
        fn get_role_type_is_root() -> RoleType = is_root;
        fn get_attribute_type_is_root() -> AttributeType = is_root;
    }

    get_type_label_methods! {
        fn get_entity_type_label() -> EntityType = get_label;
        fn get_relation_type_label() -> RelationType = get_label;
        fn get_role_type_label() -> RoleType = get_label;
        fn get_attribute_type_label() -> AttributeType = get_label;
    }

    pub(crate) fn get_entity_type_owns(
        &self,
        snapshot: &Snapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns(entity_type)))
        } else {
            let owns = TypeReader::get_owns(snapshot, entity_type.clone())?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_relation_type_owns(
        &self,
        snapshot: &Snapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns(relation_type)))
        } else {
            let owns = TypeReader::get_owns(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_relation_type_relates(
        &self,
        snapshot: &Snapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Relates<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_relation_type_relates(relation_type)))
        } else {
            let relates = TypeReader::get_relates(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_relation_type_relates_transitive(
        &self,
        snapshot: &Snapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<RoleType<'static>, Relates<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_relation_type_relates_transitive(relation_type)))
        } else {
            let relates = TypeReader::get_relates_transitive(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_entity_type_owns_transitive(
        &self,
        snapshot: &Snapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<AttributeType<'static>, Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_transitive(entity_type)))
        } else {
            let owns = TypeReader::get_owns_transitive(snapshot, entity_type.clone())?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_relation_type_owns_transitive(
        &self,
        snapshot: &Snapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<AttributeType<'static>, Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_transitive(relation_type)))
        } else {
            let owns = TypeReader::get_owns_transitive(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn relation_index_available(
        &self,
        snapshot: &Snapshot,
        relation_type: RelationType<'_>,
    ) -> Result<bool, ConceptReadError> {
        // TODO: it would be good if this doesn't require recomputation
        let mut max_card = 0;
        let relates = relation_type.get_relates(snapshot, self)?;
        for relates in relates.iter() {
            let card = relates.role().get_cardinality(snapshot, self)?;
            match card.end() {
                None => return Ok(false),
                Some(end) => max_card += end,
            }
        }
        Ok(max_card <= RELATION_INDEX_THRESHOLD)
    }

    pub(crate) fn get_entity_type_plays<'this>(
        &'this self,
        snapshot: &Snapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'this, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays(entity_type)))
        } else {
            let plays = TypeReader::get_plays(snapshot, entity_type.clone())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }
    pub(crate) fn get_entity_type_plays_transitive(
        &self,
        snapshot: &Snapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<RoleType<'static>, Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_transitive(entity_type)))
        } else {
            let plays = TypeReader::get_plays_transitive(snapshot, entity_type.clone())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_relation_type_plays<'this>(
        &'this self,
        snapshot: &Snapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'this, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays(relation_type)))
        } else {
            let plays = TypeReader::get_plays(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_relation_type_plays_transitive(
        &self,
        snapshot: &Snapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<RoleType<'static>, Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_transitive(relation_type)))
        } else {
            let plays = TypeReader::get_plays_transitive(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_plays_overridden(
        &self,
        snapshot: &Snapshot,
        plays: Plays<'static>,
    ) -> Result<MaybeOwns<'_, Option<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_override(plays)))
        } else {
            Ok(MaybeOwns::Owned(TypeReader::get_plays_override(snapshot, plays)?))
        }
    }

    pub(crate) fn get_attribute_type_value_type(
        &self,
        snapshot: &Snapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_attribute_type_value_type(attribute_type))
        } else {
            TypeReader::get_value_type(snapshot, attribute_type)
        }
    }

    get_type_annotations! {
        fn get_entity_type_annotations() -> EntityType = get_annotations | EntityTypeAnnotation;
        fn get_relation_type_annotations() -> RelationType = get_annotations | RelationTypeAnnotation;
        fn get_role_type_annotations() -> RoleType = get_annotations | RoleTypeAnnotation;
        fn get_attribute_type_annotations() -> AttributeType = get_annotations | AttributeTypeAnnotation;
    }

    pub(crate) fn get_owns_overridden(
        &self,
        snapshot: &Snapshot,
        owns: Owns<'static>,
    ) -> Result<MaybeOwns<'_, Option<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_override(owns)))
        } else {
            Ok(MaybeOwns::Owned(TypeReader::get_owns_override(snapshot, owns)?))
        }
    }

    pub(crate) fn get_owns_annotations<'this>(
        &'this self,
        snapshot: &Snapshot,
        owns: Owns<'this>,
    ) -> Result<MaybeOwns<'this, HashSet<OwnsAnnotation>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_annotations(owns)))
        } else {
            let annotations: HashSet<OwnsAnnotation> = TypeReader::get_type_edge_annotations(snapshot, owns)?
                .into_iter()
                .map(|annotation| OwnsAnnotation::from(annotation))
                .collect();
            Ok(MaybeOwns::Owned(annotations))
        }
    }

    pub(crate) fn get_owns_ordering(
        &self,
        snapshot: &Snapshot,
        owns: Owns<'static>,
    ) -> Result<Ordering, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_owns_ordering(owns))
        } else {
            TypeReader::get_type_edge_ordering(snapshot, owns)
        }
    }

    pub(crate) const fn role_default_cardinality(&self) -> AnnotationCardinality {
        // TODO: read from database properties the default role cardinality the db was created with
        AnnotationCardinality::new(1, Some(1))
    }
}

// TODO: Move this somewhere too?
impl<Snapshot: WritableSnapshot> TypeManager<Snapshot> {
    pub fn create_entity_type(
        &self,
        snapshot: &mut Snapshot,
        label: &Label<'_>,
        is_root: bool,
    ) -> Result<EntityType<'static>, ConceptWriteError> {
        // TODO: validate type doesn't exist already
        let type_vertex = self
            .vertex_generator
            .create_entity_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let entity = EntityType::new(type_vertex);
        self.storage_set_label(snapshot, entity.clone(), label);
        if !is_root {
            self.storage_set_supertype(
                snapshot,
                entity.clone(),
                self.get_entity_type(snapshot, &Kind::Entity.root_label()).unwrap().unwrap(),
            );
        }
        Ok(entity)
    }

    pub fn create_relation_type(
        &self,
        snapshot: &mut Snapshot,
        label: &Label<'_>,
        is_root: bool,
    ) -> Result<RelationType<'static>, ConceptWriteError> {
        // TODO: validate type doesn't exist already
        let type_vertex = self
            .vertex_generator
            .create_relation_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let relation = RelationType::new(type_vertex);
        self.storage_set_label(snapshot, relation.clone(), label);
        if !is_root {
            self.storage_set_supertype(
                snapshot,
                relation.clone(),
                self.get_relation_type(snapshot, &Kind::Relation.root_label()).unwrap().unwrap(),
            );
        }
        Ok(relation)
    }

    pub(crate) fn create_role_type(
        &self,
        snapshot: &mut Snapshot,
        label: &Label<'_>,
        relation_type: RelationType<'static>,
        is_root: bool,
        ordering: Ordering,
    ) -> Result<RoleType<'static>, ConceptWriteError> {
        // TODO: validate type doesn't exist already
        let type_vertex = self
            .vertex_generator
            .create_role_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let role = RoleType::new(type_vertex);
        self.storage_set_label(snapshot, role.clone(), label);
        self.storage_set_relates(snapshot, relation_type, role.clone());
        self.storage_set_role_ordering(snapshot, role.clone(), ordering);
        if !is_root {
            self.storage_set_supertype(
                snapshot,
                role.clone(),
                self.get_role_type(snapshot, &Kind::Role.root_label()).unwrap().unwrap(),
            );
        }
        Ok(role)
    }

    pub fn create_attribute_type(
        &self,
        snapshot: &mut Snapshot,
        label: &Label<'_>,
        is_root: bool,
    ) -> Result<AttributeType<'static>, ConceptWriteError> {
        // TODO: validate type doesn't exist already
        let type_vertex = self
            .vertex_generator
            .create_attribute_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let attribute_type = AttributeType::new(type_vertex);
        self.storage_set_label(snapshot, attribute_type.clone(), label);
        if !is_root {
            self.storage_set_supertype(
                snapshot,
                attribute_type.clone(),
                self.get_attribute_type(snapshot, &Kind::Attribute.root_label()).unwrap().unwrap(),
            );
        }
        Ok(attribute_type)
    }

    pub(crate) fn delete_entity_type(&self, snapshot: &mut Snapshot, entity_type: EntityType<'_>) {
        self.storage_may_delete_label(snapshot, entity_type.clone().into_owned());
        self.storage_may_delete_supertype(snapshot, entity_type.clone().into_owned());
    }

    pub(crate) fn delete_relation_type(&self, snapshot: &mut Snapshot, relation_type: RelationType<'_>) {
        let declared_relates = self.get_relation_type_relates(snapshot, relation_type.clone().into_owned()).unwrap();
        for relates in declared_relates.iter() {
            self.delete_role_type(snapshot, relates.role().clone().into_owned());
        }
        self.storage_may_delete_label(snapshot, relation_type.clone().into_owned());
        self.storage_may_delete_supertype(snapshot, relation_type.clone().into_owned());
    }

    pub(crate) fn delete_attribute_type(&self, snapshot: &mut Snapshot, attribute_type: AttributeType<'_>) {
        self.storage_may_delete_label(snapshot, attribute_type.clone().into_owned());
        self.storage_may_delete_supertype(snapshot, attribute_type.clone().into_owned());
    }

    pub(crate) fn delete_role_type(&self, snapshot: &mut Snapshot, role_type: RoleType<'_>) {
        let relates = TypeReader::get_relation(snapshot, role_type.clone().into_owned()).unwrap();
        let relation = relates.relation();
        let role = relates.role();
        self.storage_delete_relates(snapshot, relation.clone(), role.clone());

        self.storage_may_delete_label(snapshot, role.clone().into_owned());
        self.storage_may_delete_supertype(snapshot, role.clone().into_owned());
    }

    pub(crate) fn storage_set_label(&self, snapshot: &mut Snapshot, owner: impl KindAPI<'static>, label: &Label<'_>) {
        self.storage_may_delete_label(snapshot, owner.clone());

        let vertex_to_label_key = build_property_type_label(owner.clone().into_vertex());
        let label_value = ByteArray::from(label.scoped_name().bytes());
        snapshot.put_val(vertex_to_label_key.into_storage_key().into_owned_array(), label_value);

        let label_to_vertex_key = LabelToTypeVertexIndex::build(label);
        let vertex_value = ByteArray::from(owner.into_vertex().bytes());
        snapshot.put_val(label_to_vertex_key.into_storage_key().into_owned_array(), vertex_value);
    }

    fn storage_may_delete_label(&self, snapshot: &mut Snapshot, owner: impl KindAPI<'static>) {
        let existing_label = TypeReader::get_label(snapshot, owner.clone()).unwrap();
        if let Some(label) = existing_label {
            let vertex_to_label_key = build_property_type_label(owner.into_vertex());
            snapshot.delete(vertex_to_label_key.into_storage_key().into_owned_array());
            let label_to_vertex_key = LabelToTypeVertexIndex::build(&label);
            snapshot.delete(label_to_vertex_key.into_storage_key().into_owned_array());
        }
    }

    fn storage_set_role_ordering(&self, snapshot: &mut Snapshot, role: RoleType<'_>, ordering: Ordering) {
        snapshot.put_val(
            build_property_type_ordering(role.into_vertex()).into_storage_key().into_owned_array(),
            ByteArray::boxed(serialise_ordering(ordering)),
        )
    }

    pub(crate) fn storage_set_supertype<K: TypeAPI<'static>>(&self, snapshot: &mut Snapshot, subtype: K, supertype: K) {
        self.storage_may_delete_supertype(snapshot, subtype.clone());
        let sub = build_edge_sub(subtype.clone().into_vertex(), supertype.clone().into_vertex());
        snapshot.put(sub.into_storage_key().into_owned_array());
        let sub_reverse = build_edge_sub_reverse(supertype.into_vertex(), subtype.into_vertex());
        snapshot.put(sub_reverse.into_storage_key().into_owned_array());
    }

    fn storage_may_delete_supertype(&self, snapshot: &mut Snapshot, subtype: impl TypeAPI<'static>) {
        let supertype_vertex = TypeReader::get_supertype_vertex(snapshot, subtype.clone().into_vertex()).unwrap();
        if let Some(supertype) = supertype_vertex {
            let sub = build_edge_sub(subtype.clone().into_vertex(), supertype.clone());
            snapshot.delete(sub.into_storage_key().into_owned_array());
            let sub_reverse = build_edge_sub_reverse(supertype, subtype.into_vertex());
            snapshot.delete(sub_reverse.into_storage_key().into_owned_array());
        }
    }

    pub(crate) fn storage_set_owns(
        &self,
        snapshot: &mut Snapshot,
        owner: impl ObjectTypeAPI<'static>,
        attribute: AttributeType<'static>,
        ordering: Ordering,
    ) {
        let owns = build_edge_owns(owner.clone().into_vertex(), attribute.clone().into_vertex());
        snapshot.put(owns.clone().into_storage_key().into_owned_array());
        let owns_reverse = build_edge_owns_reverse(attribute.into_vertex(), owner.into_vertex());
        snapshot.put(owns_reverse.into_storage_key().into_owned_array());
        self.storage_set_owns_ordering(snapshot, owns, ordering);
    }

    pub(crate) fn storage_set_owns_overridden(
        &self,
        snapshot: &mut Snapshot,
        owns: Owns<'static>,
        overridden: Owns<'static>,
    ) {
        let property_key =
            build_property_type_edge_override(owns.into_type_edge()).into_storage_key().into_owned_array();
        let overridden_owns = ByteArray::copy(overridden.into_type_edge().into_bytes().bytes());
        snapshot.put_val(property_key, overridden_owns);
    }

    pub(crate) fn storage_set_owns_ordering(
        &self,
        snapshot: &mut Snapshot,
        owns_edge: TypeEdge<'_>,
        ordering: Ordering,
    ) {
        debug_assert_eq!(owns_edge.prefix(), Prefix::EdgeOwns);
        snapshot.put_val(
            build_property_type_edge_ordering(owns_edge).into_storage_key().into_owned_array(),
            ByteArray::boxed(serialise_ordering(ordering)),
        )
    }

    pub(crate) fn storage_delete_owns(
        &self,
        snapshot: &mut Snapshot,
        owner: impl ObjectTypeAPI<'static>,
        attribute: AttributeType<'static>,
    ) {
        let owns_edge = build_edge_owns(owner.clone().into_vertex(), attribute.clone().into_vertex());
        snapshot.delete(owns_edge.as_storage_key().into_owned_array());
        let owns_reverse = build_edge_owns_reverse(attribute.into_vertex(), owner.into_vertex());
        snapshot.delete(owns_reverse.into_storage_key().into_owned_array());
        self.storage_delete_owns_ordering(snapshot, owns_edge);
    }

    pub(crate) fn storage_delete_owns_ordering(&self, snapshot: &mut Snapshot, owns_edge: TypeEdge<'_>) {
        debug_assert_eq!(owns_edge.prefix(), Prefix::EdgeOwns);
        snapshot.delete(build_property_type_edge_ordering(owns_edge).into_storage_key().into_owned_array())
    }
    pub(crate) fn storage_set_plays(
        &self,
        snapshot: &mut Snapshot,
        player: impl ObjectTypeAPI<'static>,
        role: RoleType<'static>,
    ) {
        let plays = build_edge_plays(player.clone().into_vertex(), role.clone().into_vertex());
        snapshot.put(plays.into_storage_key().into_owned_array());
        let plays_reverse = build_edge_plays_reverse(role.into_vertex(), player.into_vertex());
        snapshot.put(plays_reverse.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_plays(
        &self,
        snapshot: &mut Snapshot,
        player: impl ObjectTypeAPI<'static>,
        role: RoleType<'static>,
    ) {
        let plays = build_edge_plays(player.clone().into_vertex(), role.clone().into_vertex());
        snapshot.delete(plays.into_storage_key().into_owned_array());
        let plays_reverse = build_edge_plays_reverse(role.into_vertex(), player.into_vertex());
        snapshot.delete(plays_reverse.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_plays_overridden(
        &self,
        snapshot: &mut Snapshot,
        plays: Plays<'static>,
        overridden: Plays<'static>,
    ) {
        let property_key =
            build_property_type_edge_override(plays.into_type_edge()).into_storage_key().into_owned_array();
        let overridden_plays = ByteArray::copy(overridden.into_type_edge().into_bytes().bytes());
        snapshot.put_val(property_key, overridden_plays);
    }

    fn storage_set_relates(&self, snapshot: &mut Snapshot, relation: RelationType<'static>, role: RoleType<'static>) {
        let relates = build_edge_relates(relation.clone().into_vertex(), role.clone().into_vertex());
        snapshot.put(relates.into_storage_key().into_owned_array());
        let relates_reverse = build_edge_relates_reverse(role.into_vertex(), relation.into_vertex());
        snapshot.put(relates_reverse.into_storage_key().into_owned_array());
    }

    fn storage_delete_relates(
        &self,
        snapshot: &mut Snapshot,
        relation: RelationType<'static>,
        role: RoleType<'static>,
    ) {
        let relates = build_edge_relates(relation.clone().into_vertex(), role.clone().into_vertex());
        snapshot.delete(relates.into_storage_key().into_owned_array());
        let relates_reverse = build_edge_relates_reverse(role.into_vertex(), relation.into_vertex());
        snapshot.delete(relates_reverse.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_value_type(
        &self,
        snapshot: &mut Snapshot,
        attribute: AttributeType<'static>,
        value_type: ValueType,
    ) {
        let property_key =
            build_property_type_value_type(attribute.into_vertex()).into_storage_key().into_owned_array();
        let property_value = ByteArray::copy(&value_type.value_type_id().bytes());
        snapshot.put_val(property_key, property_value);
    }

    pub(crate) fn storage_set_annotation_abstract(&self, snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
        let annotation_property = build_property_type_annotation_abstract(type_.into_vertex());
        snapshot.put(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_annotation_abstract(&self, snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
        let annotation_property = build_property_type_annotation_abstract(type_.into_vertex());
        snapshot.delete(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_annotation_distinct(&self, snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
        let annotation_property = build_property_type_annotation_distinct(type_.into_vertex());
        snapshot.put(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_annotation_distinct(&self, snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
        let annotation_property = build_property_type_annotation_distinct(type_.into_vertex());
        snapshot.delete(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_edge_annotation_distinct<'b>(
        &self,
        snapshot: &mut Snapshot,
        edge: impl IntoCanonicalTypeEdge<'b>,
    ) {
        let annotation_property = build_property_type_edge_annotation_distinct(edge.into_type_edge());
        snapshot.put(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_edge_annotation_distinct<'b>(
        &self,
        snapshot: &mut Snapshot,
        edge: impl IntoCanonicalTypeEdge<'b>,
    ) {
        let annotation_property = build_property_type_edge_annotation_distinct(edge.into_type_edge());
        snapshot.delete(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_edge_annotation_key<'b>(
        &self,
        snapshot: &mut Snapshot,
        edge: impl IntoCanonicalTypeEdge<'b>,
    ) {
        let annotation_property = build_property_type_edge_annotation_key(edge.into_type_edge());
        snapshot.put(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_edge_annotation_key<'b>(
        &self,
        snapshot: &mut Snapshot,
        edge: impl IntoCanonicalTypeEdge<'b>,
    ) {
        let annotation_property = build_property_type_edge_annotation_key(edge.into_type_edge());
        snapshot.delete(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_annotation_independent(&self, snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
        let annotation_property = build_property_type_annotation_independent(type_.into_vertex());
        snapshot.put(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_annotation_independent(&self, snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
        let annotation_property = build_property_type_annotation_independent(type_.into_vertex());
        snapshot.delete(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_annotation_cardinality(
        &self,
        snapshot: &mut Snapshot,
        type_: impl TypeAPI<'static>,
        annotation: AnnotationCardinality,
    ) {
        snapshot.put_val(
            build_property_type_annotation_cardinality(type_.into_vertex()).into_storage_key().into_owned_array(),
            ByteArray::boxed(serialise_annotation_cardinality(annotation)),
        );
    }

    pub(crate) fn storage_delete_annotation_cardinality(&self, snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
        let annotation_property = build_property_type_annotation_cardinality(type_.into_vertex());
        snapshot.delete(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_edge_annotation_cardinality<'b>(
        &self,
        snapshot: &mut Snapshot,
        edge: impl IntoCanonicalTypeEdge<'b>,
        annotation: AnnotationCardinality,
    ) {
        snapshot.put_val(
            build_property_type_edge_annotation_cardinality(edge.into_type_edge())
                .into_storage_key()
                .into_owned_array(),
            ByteArray::boxed(serialise_annotation_cardinality(annotation)),
        );
    }

    pub(crate) fn storage_delete_edge_annotation_cardinality<'b>(
        &self,
        snapshot: &mut Snapshot,
        edge: impl IntoCanonicalTypeEdge<'b>,
    ) {
        let annotation_property = build_property_type_edge_annotation_cardinality(edge.into_type_edge());
        snapshot.delete(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_annotation_regex(
        &self,
        snapshot: &mut Snapshot,
        type_: impl TypeAPI<'static>,
        regex: AnnotationRegex,
    ) {
        let annotation_property = build_property_type_annotation_regex(type_.into_vertex());
        snapshot.put_val(
            annotation_property.into_storage_key().into_owned_array(),
            ByteArray::copy(regex.regex().as_bytes()),
        );
    }

    pub(crate) fn storage_delete_annotation_regex(
        &self,
        snapshot: &mut Snapshot,
        type_: impl TypeAPI<'static>,
        regex: AnnotationRegex,
    ) {
        // TODO debug assert that stored regex matches
        let annotation_property = build_property_type_annotation_regex(type_.into_vertex());
        snapshot.delete(annotation_property.into_storage_key().into_owned_array());
    }
}

pub trait ReadableType {
    // Consider replacing 'bytes with 'static
    type ReadOutput<'bytes>: 'bytes;
    fn read_from<'bytes>(b: Bytes<'bytes, BUFFER_KEY_INLINE>) -> Self::ReadOutput<'bytes>;
}

impl<'a> ReadableType for AttributeType<'a> {
    type ReadOutput<'bytes> = AttributeType<'bytes>;
    fn read_from<'bytes>(b: Bytes<'bytes, BUFFER_KEY_INLINE>) -> Self::ReadOutput<'bytes> {
        AttributeType::new(new_vertex_attribute_type(b))
    }
}

impl<'a> ReadableType for EntityType<'a> {
    type ReadOutput<'bytes> = EntityType<'bytes>;
    fn read_from<'bytes>(b: Bytes<'bytes, BUFFER_KEY_INLINE>) -> Self::ReadOutput<'bytes> {
        EntityType::new(new_vertex_entity_type(b))
    }
}

impl<'a> ReadableType for RelationType<'a> {
    type ReadOutput<'bytes> = RelationType<'bytes>;
    fn read_from<'bytes>(b: Bytes<'bytes, BUFFER_KEY_INLINE>) -> Self::ReadOutput<'bytes> {
        RelationType::new(new_vertex_relation_type(b))
    }
}

impl<'a> ReadableType for RoleType<'a> {
    type ReadOutput<'bytes> = RoleType<'bytes>;
    fn read_from<'bytes>(b: Bytes<'bytes, BUFFER_KEY_INLINE>) -> Self::ReadOutput<'bytes> {
        RoleType::new(new_vertex_role_type(b))
    }
}

pub trait KindAPI<'a>: TypeAPI<'a> {
    type SelfStatic: KindAPI<'static> + 'static;
    type AnnotationType: Hash + Eq + From<Annotation>;
    const ROOT_KIND: Kind;
}

impl<'a> KindAPI<'a> for AttributeType<'a> {
    type SelfStatic = AttributeType<'static>;
    type AnnotationType = AttributeTypeAnnotation;
    const ROOT_KIND: Kind = Kind::Attribute;
}

impl<'a> KindAPI<'a> for EntityType<'a> {
    type SelfStatic = EntityType<'static>;
    type AnnotationType = EntityTypeAnnotation;
    const ROOT_KIND: Kind = Kind::Entity;
}

impl<'a> KindAPI<'a> for RelationType<'a> {
    type SelfStatic = RelationType<'static>;
    type AnnotationType = RelationTypeAnnotation;
    const ROOT_KIND: Kind = Kind::Relation;
}

impl<'a> KindAPI<'a> for RoleType<'a> {
    type SelfStatic = RoleType<'static>;
    type AnnotationType = RoleTypeAnnotation;
    const ROOT_KIND: Kind = Kind::Role;
}
