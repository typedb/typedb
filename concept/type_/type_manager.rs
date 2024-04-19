/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, sync::Arc};

use bytes::{byte_array::ByteArray, Bytes};
use durability::DurabilityService;
use encoding::{
    AsBytes,
    graph::type_::{
        edge::{
            build_edge_owns, build_edge_owns_prefix_from, build_edge_owns_reverse, build_edge_plays,
            build_edge_plays_prefix_from, build_edge_plays_reverse, build_edge_relates, build_edge_relates_prefix_from,
            build_edge_relates_reverse, build_edge_sub, build_edge_sub_prefix_from, build_edge_sub_reverse,
            new_edge_owns, new_edge_plays, new_edge_relates, new_edge_sub,
        },
        index::LabelToTypeVertexIndex,
        Kind,
        property::{
            build_property_type_annotation_abstract, build_property_type_annotation_cardinality,
            build_property_type_annotation_distinct, build_property_type_annotation_independent,
            build_property_type_label, build_property_type_value_type,
        },
        vertex::{
            new_vertex_attribute_type, new_vertex_entity_type, new_vertex_relation_type, new_vertex_role_type,
            TypeVertex,
        },
        vertex_generator::TypeVertexGenerator,
    },
    Keyable, value::{
        label::Label,
        string::StringBytes,
        value_type::{ValueType, ValueTypeID},
    },
};
use encoding::graph::type_::property::{TypeEdgeProperty, TypeVertexProperty};
use encoding::layout::infix::Infix;
use primitive::{maybe_owns::MaybeOwns, prefix_range::PrefixRange};
use resource::constants::{encoding::LABEL_SCOPED_NAME_STRING_INLINE, snapshot::BUFFER_KEY_INLINE};
use storage::{
    MVCCStorage,
    snapshot::{CommittableSnapshot, ReadableSnapshot, WritableSnapshot},
};

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::{AnnotationAbstract, AnnotationCardinality, AnnotationDistinct, AnnotationIndependent},
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        entity_type::{EntityType, EntityTypeAnnotation},
        object_type::ObjectType,
        ObjectTypeAPI,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        relation_type::{RelationType, RelationTypeAnnotation},
        role_type::{RoleType, RoleTypeAnnotation},
        type_cache::TypeCache, TypeAPI,
    },
};
use crate::error::ConceptWriteError;
use crate::type_::{deserialise_annotation_cardinality, IntoCanonicalTypeEdge, OwnerAPI, PlayerAPI};
use crate::type_::annotation::Annotation;
use crate::type_::owns::OwnsAnnotation;

// TODO: this should be parametrised into the database options? Would be great to have it be changable at runtime!
pub(crate) const RELATION_INDEX_THRESHOLD: u64 = 8;

pub struct TypeManager<Snapshot> {
    snapshot: Arc<Snapshot>,
    vertex_generator: Arc<TypeVertexGenerator>,
    type_cache: Option<Arc<TypeCache>>,
}

impl<Snapshot> TypeManager<Snapshot> {
    pub fn initialise_types<D: DurabilityService>(
        storage: Arc<MVCCStorage<D>>,
        vertex_generator: Arc<TypeVertexGenerator>,
    ) -> Result<(), ConceptWriteError> {
        let snapshot = Arc::new(storage.clone().open_snapshot_write());
        {
            let type_manager = TypeManager::new(snapshot.clone(), vertex_generator.clone(), None);
            let root_entity = type_manager.create_entity_type(&Kind::Entity.root_label(), true)?;
            root_entity.set_annotation(&type_manager, EntityTypeAnnotation::Abstract(AnnotationAbstract::new()));
            let root_relation = type_manager.create_relation_type(&Kind::Relation.root_label(), true)?;
            root_relation.set_annotation(&type_manager, RelationTypeAnnotation::Abstract(AnnotationAbstract::new()));
            let root_role = type_manager.create_role_type(&Kind::Role.root_label(), root_relation.clone(), true)?;
            root_role.set_annotation(&type_manager, RoleTypeAnnotation::Abstract(AnnotationAbstract::new()));
            let root_attribute = type_manager.create_attribute_type(&Kind::Attribute.root_label(), true)?;
            root_attribute.set_annotation(&type_manager, AttributeTypeAnnotation::Abstract(AnnotationAbstract::new()));
        }
        Arc::try_unwrap(snapshot).ok().unwrap().commit().unwrap();
        Ok(())
    }
}

// TODO:
//   if we drop/close without committing, then we need to release all the IDs taken back to the IDGenerator
//   this is only applicable for type manager where we can only have 1 concurrent txn and IDs are precious

macro_rules! get_type_methods {
    ($(
        fn $method_name:ident() -> $output_type:ident = $cache_method:ident | $new_vertex_method:ident;
    )*) => {
        $(
            pub fn $method_name(&self, label: &Label<'_>) -> Result<Option<$output_type<'static>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(label))
                } else {
                    self.get_labelled_type(label, |bytes| $output_type::new($new_vertex_method(bytes)))
                }
            }
        )*

        fn get_labelled_type<M, U>(&self, label: &Label<'_>, mapper: M) -> Result<Option<U>, ConceptReadError>
            where
                M: FnOnce(Bytes<'static, BUFFER_KEY_INLINE>) -> U,
        {
            let key = LabelToTypeVertexIndex::build(label).into_storage_key();
            match self.snapshot.get::<BUFFER_KEY_INLINE>(key.as_reference()) {
                Ok(None) => Ok(None),
                Ok(Some(value)) => Ok(Some(mapper(Bytes::Array(value)))),
                Err(error) => Err(ConceptReadError::SnapshotGet { source: error })
            }
        }
    }
}

macro_rules! get_supertype_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            // WARN: supertypes currently do NOT include themselves
            pub(crate) fn $method_name(&self, type_: $type_<'static>) -> Result<Option<$type_<'static>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(type_))
                } else {
                    Ok(self
                        .snapshot
                        .iterate_range(PrefixRange::new_within(build_edge_sub_prefix_from(type_.into_vertex().clone())))
                        .first_cloned()
                        .map_err(|error| ConceptReadError::SnapshotIterate { source: error })?
                        .map(|(key, _)| $type_::new(new_edge_sub(key.into_byte_array_or_ref()).to().into_owned())))
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
            pub(crate) fn $method_name(&self, type_: $type_<'static>) -> Result<MaybeOwns<'_, Vec<$type_<'static>>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::borrowed(cache.$cache_method(type_)))
                } else {
                    let mut supertypes = Vec::new();
                    let mut super_vertex = self.storage_get_supertype_vertex(type_);
                    while super_vertex.is_some() {
                        let super_type = $type_::new(super_vertex.as_ref().unwrap().clone());
                        super_vertex = self.storage_get_supertype_vertex(super_type.clone());
                        supertypes.push(super_type);
                    }
                    Ok(MaybeOwns::owned(supertypes))
                }
            }
        )*
    }
}

macro_rules! get_type_is_root_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident | $base_variant:expr;
    )*) => {
        $(
            pub(crate) fn $method_name(&self, type_: $type_<'static>) -> Result<bool, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(type_))
                } else {
                    Ok(*type_.get_label(self)? == $base_variant.root_label())
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
            pub(crate) fn $method_name(&self, type_: $type_<'static>) -> Result<MaybeOwns<'_, Label<'static>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::borrowed(cache.$cache_method(type_)))
                } else {
                    Ok(MaybeOwns::owned(self.storage_get_label(type_)?.unwrap()))
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
                &self, type_: $type_<'static>
            ) -> Result<MaybeOwns<'_, HashSet<$annotation_type>>, ConceptReadError> {
                 if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::borrowed(cache.$cache_method(type_)))
                } else {
                    let mut annotations: HashSet<$annotation_type> = HashSet::new();
                    let annotations = self.storage_get_type_annotations(type_)?
                        .into_iter()
                        .map(|annotation| $annotation_type::from(annotation))
                        .collect();
                    Ok(MaybeOwns::owned(annotations))
                }
            }
        )*
    }
}

impl<Snapshot: ReadableSnapshot> TypeManager<Snapshot> {
    pub fn new(
        snapshot: Arc<Snapshot>,
        vertex_generator: Arc<TypeVertexGenerator>,
        schema_cache: Option<Arc<TypeCache>>,
    ) -> Self {
        TypeManager { snapshot, vertex_generator, type_cache: schema_cache }
    }

    get_type_methods! {
        fn get_entity_type() -> EntityType = get_entity_type | new_vertex_entity_type;
        fn get_relation_type() -> RelationType = get_relation_type | new_vertex_relation_type;
        fn get_role_type() -> RoleType = get_role_type | new_vertex_role_type;
        fn get_attribute_type() -> AttributeType = get_attribute_type | new_vertex_attribute_type;
    }

    get_supertype_methods! {
        fn get_entity_type_supertype() -> EntityType = get_entity_type_supertype;
        fn get_relation_type_supertype() -> RelationType = get_relation_type_supertype;
        fn get_role_type_supertype() -> RoleType = get_role_type_supertype;
        fn get_attribute_type_supertype() -> AttributeType = get_attribute_type_supertype;
    }

    get_supertypes_methods! {
        fn get_entity_type_supertypes() -> EntityType = get_entity_type_supertypes;
        fn get_relation_type_supertypes() -> RelationType = get_relation_type_supertypes;
        fn get_role_type_supertypes() -> RoleType = get_role_type_supertypes;
        fn get_attribute_type_supertypes() -> AttributeType = get_attribute_type_supertypes;
    }

    get_type_is_root_methods! {
        fn get_entity_type_is_root() -> EntityType = get_entity_type_is_root | Kind::Entity;
        fn get_relation_type_is_root() -> RelationType = get_relation_type_is_root | Kind::Relation;
        fn get_role_type_is_root() -> RoleType = get_role_type_is_root | Kind::Role;
        fn get_attribute_type_is_root() -> AttributeType = get_attribute_type_is_root | Kind::Attribute;
    }

    get_type_label_methods! {
        fn get_entity_type_label() -> EntityType = get_entity_type_label;
        fn get_relation_type_label() -> RelationType = get_relation_type_label;
        fn get_role_type_label() -> RoleType = get_role_type_label;
        fn get_attribute_type_label() -> AttributeType = get_attribute_type_label;
    }

    pub(crate) fn get_entity_type_owns(
        &self,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::borrowed(cache.get_entity_type_owns(entity_type)))
        } else {
            let owns = self.storage_get_owns(entity_type.clone(), |attr_vertex| {
                Owns::new(ObjectType::Entity(entity_type.clone()), AttributeType::new(attr_vertex.clone().into_owned()))
            })?;
            Ok(MaybeOwns::owned(owns))
        }
    }

    pub(crate) fn get_relation_type_owns(
        &self,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::borrowed(cache.get_relation_type_owns(relation_type)))
        } else {
            let owns = self.storage_get_owns(relation_type.clone(), |attr_vertex| {
                Owns::new(
                    ObjectType::Relation(relation_type.clone()),
                    AttributeType::new(attr_vertex.clone().into_owned()),
                )
            })?;
            Ok(MaybeOwns::owned(owns))
        }
    }

    pub(crate) fn get_relation_type_relates(
        &self,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Relates<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::borrowed(cache.get_relation_type_relates(relation_type)))
        } else {
            let relates = self.storage_get_relates(relation_type.clone(), |role_vertex| {
                Relates::new(relation_type.clone(), RoleType::new(role_vertex.clone().into_owned()))
            })?;
            Ok(MaybeOwns::owned(relates))
        }
    }

    pub(crate) fn relation_index_available(&self, relation_type: RelationType<'_>) -> Result<bool, ConceptReadError> {
        // TODO: it would be good if this doesn't require recomputation
        let mut max_card = 0;
        let relates = relation_type.get_relates(self)?;
        for relates in relates.iter() {
            let card = relates.role().get_cardinality(self)?;
            match card.end() {
                None => return Ok(false),
                Some(end) => max_card += end,
            }
        };
        Ok(max_card <= RELATION_INDEX_THRESHOLD)
    }

    pub(crate) fn get_entity_type_plays<'this>(
        &'this self,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'this, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::borrowed(cache.get_entity_type_plays(entity_type)))
        } else {
            let plays = self.storage_get_plays(entity_type.clone(), |role_vertex| {
                Plays::new(ObjectType::Entity(entity_type.clone()), RoleType::new(role_vertex.clone().into_owned()))
            })?;
            Ok(MaybeOwns::owned(plays))
        }
    }

    pub(crate) fn get_attribute_type_value_type(
        &self,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_attribute_type_value_type(attribute_type))
        } else {
            self.storage_get_value_type(attribute_type)
        }
    }

    get_type_annotations! {
        fn get_entity_type_annotations() -> EntityType = get_entity_type_annotations | EntityTypeAnnotation;
        fn get_relation_type_annotations() -> RelationType = get_relation_type_annotations | RelationTypeAnnotation;
        fn get_role_type_annotations() -> RoleType = get_role_type_annotations | RoleTypeAnnotation;
        fn get_attribute_type_annotations() -> AttributeType = get_attribute_type_annotations | AttributeTypeAnnotation;
    }

    pub(crate) fn get_owns_annotations<'this>(
        &'this self, owns: Owns<'this>,
    ) -> Result<MaybeOwns<'this, HashSet<OwnsAnnotation>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::borrowed(cache.get_owns_annotations(owns)))
        } else {
            let annotations: HashSet<OwnsAnnotation> = self.storage_get_type_edge_annotations(owns)?
                .into_iter()
                .map(|annotation| OwnsAnnotation::from(annotation))
                .collect();
            Ok(MaybeOwns::owned(annotations))
        }
    }

    fn storage_get_label(&self, type_: impl TypeAPI<'static>) -> Result<Option<Label<'static>>, ConceptReadError> {
        let key = build_property_type_label(type_.into_vertex());
        self.snapshot
            .get_mapped(key.into_storage_key().as_reference(), |reference| {
                let value = StringBytes::new(Bytes::<LABEL_SCOPED_NAME_STRING_INLINE>::Reference(reference));
                Label::parse_from(value)
            })
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    fn storage_get_supertype_vertex(&self, subtype: impl TypeAPI<'static>) -> Option<TypeVertex<'static>> {
        // TODO: handle possible errors
        self.snapshot
            .iterate_range(PrefixRange::new_within(build_edge_sub_prefix_from(subtype.into_vertex())))
            .first_cloned()
            .unwrap()
            .map(|(key, _)| new_edge_sub(key.into_byte_array_or_ref()).to().into_owned())
    }

    fn storage_get_owns<F>(
        &self,
        owner: impl OwnerAPI<'static>,
        mapper: F,
    ) -> Result<HashSet<Owns<'static>>, ConceptReadError>
        where
            F: for<'b> Fn(TypeVertex<'b>) -> Owns<'static>,
    {
        let owns_prefix = build_edge_owns_prefix_from(owner.into_vertex());
        // TODO: handle possible errors
        self.snapshot
            .iterate_range(PrefixRange::new_within(owns_prefix))
            .collect_cloned_hashset(|key, _| {
                let owns_edge = new_edge_owns(Bytes::Reference(key.byte_ref()));
                mapper(owns_edge.to())
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    fn storage_get_plays<F>(
        &self,
        player: impl PlayerAPI<'static>,
        mapper: F,
    ) -> Result<HashSet<Plays<'static>>, ConceptReadError>
        where
            F: for<'b> Fn(TypeVertex<'b>) -> Plays<'static>,
    {
        let plays_prefix = build_edge_plays_prefix_from(player.into_vertex());
        self.snapshot
            .iterate_range(PrefixRange::new_within(plays_prefix))
            .collect_cloned_hashset(|key, _| {
                let plays_edge = new_edge_plays(Bytes::Reference(key.byte_ref()));
                mapper(plays_edge.to())
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    fn storage_get_relates<F>(
        &self,
        relation: RelationType<'static>,
        mapper: F,
    ) -> Result<HashSet<Relates<'static>>, ConceptReadError>
        where
            F: for<'b> Fn(TypeVertex<'b>) -> Relates<'static>,
    {
        let relates_prefix = build_edge_relates_prefix_from(relation.into_vertex());
        self.snapshot
            .iterate_range(PrefixRange::new_within(relates_prefix))
            .collect_cloned_hashset(|key, _| {
                let relates_edge = new_edge_relates(Bytes::Reference(key.byte_ref()));
                mapper(relates_edge.to())
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    fn storage_get_value_type(&self, type_: AttributeType<'static>) -> Result<Option<ValueType>, ConceptReadError> {
        self.snapshot
            .get_mapped(
                build_property_type_value_type(type_.into_vertex()).into_storage_key().as_reference(),
                |bytes| {
                    ValueType::from_value_type_id(ValueTypeID::new(bytes.bytes().try_into().unwrap()))
                },
            )
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    fn storage_get_type_annotations(
        &self,
        type_: impl TypeAPI<'static>,
    ) -> Result<HashSet<Annotation>, ConceptReadError> {
        self.snapshot
            .iterate_range(PrefixRange::new_inclusive(
                TypeVertexProperty::build(type_.vertex(), Infix::ANNOTATION_MIN).into_storage_key(),
                TypeVertexProperty::build(type_.vertex(), Infix::ANNOTATION_MAX).into_storage_key(),
            ))
            .collect_cloned_hashset(|key, value| {
                let annotation_key = TypeVertexProperty::new(Bytes::Reference(key.byte_ref()));
                match annotation_key.infix() {
                    Infix::PropertyAnnotationAbstract => Annotation::Abstract(AnnotationAbstract::new()),
                    Infix::PropertyAnnotationDistinct => Annotation::Distinct(AnnotationDistinct::new()),
                    Infix::PropertyAnnotationIndependent => Annotation::Independent(AnnotationIndependent::new()),
                    Infix::PropertyAnnotationCardinality => {
                        Annotation::Cardinality(deserialise_annotation_cardinality(value))
                    }
                    Infix::_PropertyAnnotationLast | Infix::PropertyLabel | Infix::PropertyValueType => {
                        unreachable!("Retrieved unexpected infixes while reading annotations.")
                    }
                }
            })
            .map_err(|err| ConceptReadError::SnapshotIterate { source: err.clone() })
    }

    // TODO: this is currently breaking our architectural pattern that none of the Manager methods should operate graphs
    fn storage_get_type_edge_annotations<'a>(
        &self,
        into_type_edge: impl IntoCanonicalTypeEdge<'a>,
    ) -> Result<HashSet<Annotation>, ConceptReadError> {
        let type_edge = into_type_edge.into_type_edge();
        self.snapshot
            .iterate_range(PrefixRange::new_inclusive(
                TypeEdgeProperty::build(type_edge.clone(), Infix::ANNOTATION_MIN).into_storage_key(),
                TypeEdgeProperty::build(type_edge, Infix::ANNOTATION_MAX).into_storage_key(),
            ))
            .collect_cloned_hashset(|key, value| {
                let annotation_key = TypeEdgeProperty::new(Bytes::Reference(key.byte_ref()));
                match annotation_key.infix() {
                    Infix::PropertyAnnotationAbstract => Annotation::Abstract(AnnotationAbstract::new()),
                    Infix::PropertyAnnotationDistinct => Annotation::Distinct(AnnotationDistinct::new()),
                    Infix::PropertyAnnotationIndependent => Annotation::Independent(AnnotationIndependent::new()),
                    Infix::PropertyAnnotationCardinality => {
                        Annotation::Cardinality(deserialise_annotation_cardinality(value))
                    }
                    Infix::_PropertyAnnotationLast | Infix::PropertyLabel | Infix::PropertyValueType => {
                        unreachable!("Retrieved unexpected infixes while reading annotations.")
                    }
                }
            })
            .map_err(|err| ConceptReadError::SnapshotIterate { source: err.clone() })
    }

    pub(crate) const fn role_default_cardinality(&self) -> AnnotationCardinality {
        // TODO: read from database properties the default role cardinality the db was created with
        AnnotationCardinality::new(1, Some(1))
    }
}

impl<Snapshot: WritableSnapshot> TypeManager<Snapshot> {
    pub fn create_entity_type(&self, label: &Label<'_>, is_root: bool) -> Result<EntityType<'static>, ConceptWriteError> {
        // TODO: validate type doesn't exist already
        let type_vertex = self.vertex_generator.create_entity_type(self.snapshot.as_ref())
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let entity = EntityType::new(type_vertex);
        self.storage_set_label(entity.clone(), label);
        if !is_root {
            self.storage_set_supertype(
                entity.clone(),
                self.get_entity_type(&Kind::Entity.root_label()).unwrap().unwrap(),
            );
        }
        Ok(entity)
    }

    pub fn create_relation_type(&self, label: &Label<'_>, is_root: bool) -> Result<RelationType<'static>, ConceptWriteError> {
        // TODO: validate type doesn't exist already
        let type_vertex = self.vertex_generator.create_relation_type(self.snapshot.as_ref()).map_err(|err| ConceptWriteError::Encoding { source: err })?;
        ;
        let relation = RelationType::new(type_vertex);
        self.storage_set_label(relation.clone(), label);
        if !is_root {
            self.storage_set_supertype(
                relation.clone(),
                self.get_relation_type(&Kind::Relation.root_label()).unwrap().unwrap(),
            );
        }
        Ok(relation)
    }

    pub(crate) fn create_role_type(
        &self,
        label: &Label<'_>,
        relation_type: RelationType<'static>,
        is_root: bool,
    ) -> Result<RoleType<'static>, ConceptWriteError> {
        // TODO: validate type doesn't exist already
        let type_vertex = self.vertex_generator.create_role_type(self.snapshot.as_ref()).map_err(|err| ConceptWriteError::Encoding { source: err })?;
        ;
        let role = RoleType::new(type_vertex);
        self.storage_set_label(role.clone(), label);
        self.storage_set_relates(relation_type, role.clone());
        if !is_root {
            self.storage_set_supertype(role.clone(), self.get_role_type(&Kind::Role.root_label()).unwrap().unwrap());
        }
        Ok(role)
    }

    pub fn create_attribute_type(&self, label: &Label<'_>, is_root: bool) -> Result<AttributeType<'static>, ConceptWriteError> {
        // TODO: validate type doesn't exist already
        let type_vertex = self.vertex_generator.create_attribute_type(self.snapshot.as_ref()).map_err(|err| ConceptWriteError::Encoding { source: err })?;
        ;
        let attribute_type = AttributeType::new(type_vertex);
        self.storage_set_label(attribute_type.clone(), label);
        if !is_root {
            self.storage_set_supertype(
                attribute_type.clone(),
                self.get_attribute_type(&Kind::Attribute.root_label()).unwrap().unwrap(),
            );
        }
        Ok(attribute_type)
    }

    pub(crate) fn storage_set_label(&self, owner: impl TypeAPI<'static>, label: &Label<'_>) {
        self.storage_may_delete_label(owner.clone());

        let vertex_to_label_key = build_property_type_label(owner.clone().into_vertex());
        let label_value = ByteArray::from(label.scoped_name().bytes());
        self.snapshot.as_ref().put_val(vertex_to_label_key.into_storage_key().into_owned_array(), label_value);

        let label_to_vertex_key = LabelToTypeVertexIndex::build(label);
        let vertex_value = ByteArray::from(owner.into_vertex().bytes());
        self.snapshot.as_ref().put_val(label_to_vertex_key.into_storage_key().into_owned_array(), vertex_value);
    }

    fn storage_may_delete_label(&self, owner: impl TypeAPI<'static>) {
        let existing_label = self.storage_get_label(owner.clone()).unwrap();
        if let Some(label) = existing_label {
            let vertex_to_label_key = build_property_type_label(owner.into_vertex());
            self.snapshot.as_ref().delete(vertex_to_label_key.into_storage_key().into_owned_array());
            let label_to_vertex_key = LabelToTypeVertexIndex::build(&label);
            self.snapshot.as_ref().delete(label_to_vertex_key.into_storage_key().into_owned_array());
        }
    }

    pub(crate) fn storage_set_supertype<K: TypeAPI<'static>>(&self, subtype: K, supertype: K) {
        self.storage_may_delete_supertype(subtype.clone());
        let sub = build_edge_sub(subtype.clone().into_vertex(), supertype.clone().into_vertex());
        self.snapshot.as_ref().put(sub.into_storage_key().into_owned_array());
        let sub_reverse = build_edge_sub_reverse(supertype.into_vertex(), subtype.into_vertex());
        self.snapshot.as_ref().put(sub_reverse.into_storage_key().into_owned_array());
    }

    fn storage_may_delete_supertype(&self, subtype: impl TypeAPI<'static>) {
        let supertype_vertex = self.storage_get_supertype_vertex(subtype.clone());
        if let Some(supertype) = supertype_vertex {
            let sub = build_edge_sub(subtype.clone().into_vertex(), supertype.clone());
            self.snapshot.as_ref().delete(sub.into_storage_key().into_owned_array());
            let sub_reverse = build_edge_sub_reverse(supertype, subtype.into_vertex());
            self.snapshot.as_ref().delete(sub_reverse.into_storage_key().into_owned_array());
        }
    }

    pub(crate) fn storage_set_owns(&self, owner: impl ObjectTypeAPI<'static>, attribute: AttributeType<'static>) {
        let owns = build_edge_owns(owner.clone().into_vertex(), attribute.clone().into_vertex());
        self.snapshot.as_ref().put(owns.into_storage_key().into_owned_array());
        let owns_reverse = build_edge_owns_reverse(attribute.into_vertex(), owner.into_vertex());
        self.snapshot.as_ref().put(owns_reverse.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_owns(&self, owner: impl ObjectTypeAPI<'static>, attribute: AttributeType<'static>) {
        let owns = build_edge_owns(owner.clone().into_vertex(), attribute.clone().into_vertex());
        self.snapshot.as_ref().delete(owns.into_storage_key().into_owned_array());
        let owns_reverse = build_edge_owns_reverse(attribute.into_vertex(), owner.into_vertex());
        self.snapshot.as_ref().delete(owns_reverse.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_plays(&self, player: impl ObjectTypeAPI<'static>, role: RoleType<'static>) {
        let plays = build_edge_plays(player.clone().into_vertex(), role.clone().into_vertex());
        self.snapshot.as_ref().put(plays.into_storage_key().into_owned_array());
        let plays_reverse = build_edge_plays_reverse(role.into_vertex(), player.into_vertex());
        self.snapshot.as_ref().put(plays_reverse.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_plays(&self, player: impl ObjectTypeAPI<'static>, role: RoleType<'static>) {
        let plays = build_edge_plays(player.clone().into_vertex(), role.clone().into_vertex());
        self.snapshot.as_ref().delete(plays.into_storage_key().into_owned_array());
        let plays_reverse = build_edge_plays_reverse(role.into_vertex(), player.into_vertex());
        self.snapshot.as_ref().delete(plays_reverse.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_relates(&self, relation: RelationType<'static>, role: RoleType<'static>) {
        let relates = build_edge_relates(relation.clone().into_vertex(), role.clone().into_vertex());
        self.snapshot.as_ref().put(relates.into_storage_key().into_owned_array());
        let relates_reverse = build_edge_relates_reverse(role.into_vertex(), relation.into_vertex());
        self.snapshot.as_ref().put(relates_reverse.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_relates(&self, relation: RelationType<'static>, role: RoleType<'static>) {
        let relates = build_edge_relates(relation.clone().into_vertex(), role.clone().into_vertex());
        self.snapshot.as_ref().delete(relates.into_storage_key().into_owned_array());
        let relates_reverse = build_edge_relates_reverse(role.into_vertex(), relation.into_vertex());
        self.snapshot.as_ref().delete(relates_reverse.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_value_type(&self, attribute: AttributeType<'static>, value_type: ValueType) {
        let property_key =
            build_property_type_value_type(attribute.into_vertex()).into_storage_key().into_owned_array();
        let property_value = ByteArray::copy(&value_type.value_type_id().bytes());
        self.snapshot.as_ref().put_val(property_key, property_value);
    }

    pub(crate) fn storage_set_annotation_abstract(&self, type_: impl TypeAPI<'static>) {
        let annotation_property = build_property_type_annotation_abstract(type_.into_vertex());
        self.snapshot.as_ref().put(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_annotation_abstract(&self, type_: impl TypeAPI<'static>) {
        let annotation_property = build_property_type_annotation_abstract(type_.into_vertex());
        self.snapshot.as_ref().delete(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_annotation_distinct(&self, type_: impl TypeAPI<'static>) {
        let annotation_property = build_property_type_annotation_distinct(type_.into_vertex());
        self.snapshot.as_ref().put(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_annotation_distinct(&self, type_: impl TypeAPI<'static>) {
        let annotation_property = build_property_type_annotation_distinct(type_.into_vertex());
        self.snapshot.as_ref().delete(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_annotation_independent(&self, type_: impl TypeAPI<'static>) {
        let annotation_property = build_property_type_annotation_independent(type_.into_vertex());
        self.snapshot.as_ref().put(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_storage_annotation_independent(&self, type_: impl TypeAPI<'static>) {
        let annotation_property = build_property_type_annotation_independent(type_.into_vertex());
        self.snapshot.as_ref().delete(annotation_property.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_annotation_cardinality(
        &self,
        type_: impl TypeAPI<'static>,
        annotation: AnnotationCardinality,
    ) {
        let annotation_property = build_property_type_annotation_cardinality(type_.into_vertex());
        let serialized_annotation = ByteArray::boxed(bincode::serialize(&annotation).unwrap().into_boxed_slice());
        self.snapshot
            .as_ref()
            .put_val(annotation_property.into_storage_key().into_owned_array(), serialized_annotation);
    }

    pub(crate) fn storage_delete_annotation_cardinality(&self, type_: impl TypeAPI<'static>) {
        let annotation_property = build_property_type_annotation_cardinality(type_.into_vertex());
        self.snapshot.as_ref().delete(annotation_property.into_storage_key().into_owned_array());
    }
}
