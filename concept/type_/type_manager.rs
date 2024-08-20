/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use encoding::{
    error::EncodingError,
    graph::{
        definition::{
            definition_key::DefinitionKey, definition_key_generator::DefinitionKeyGenerator, r#struct::StructDefinition,
        },
        type_::{
            property::{TypeEdgePropertyEncoding, TypeVertexPropertyEncoding},
            vertex::TypeVertexEncoding,
            vertex_generator::TypeVertexGenerator,
            Kind,
        },
    },
    value::{label::Label, value_type::ValueType},
};
use itertools::Itertools;
use primitive::maybe_owns::MaybeOwns;
use resource::constants::encoding::StructFieldIDUInt;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use type_cache::TypeCache;
use type_writer::TypeWriter;
use validation::{commit_time_validation::CommitTimeValidation, operation_time_validation::OperationTimeValidation};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::thing_manager::ThingManager,
    type_::{
        annotation::{
            Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCascade, AnnotationCategory,
            AnnotationDistinct, AnnotationIndependent, AnnotationKey, AnnotationRange, AnnotationRegex,
            AnnotationUnique, AnnotationValues,
        },
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        constraint::Constraint,
        entity_type::{EntityType, EntityTypeAnnotation},
        object_type::{with_object_type, ObjectType},
        owns::{Owns, OwnsAnnotation},
        plays::{Plays, PlaysAnnotation},
        relates::{Relates, RelatesAnnotation},
        relation_type::{RelationType, RelationTypeAnnotation},
        role_type::{RoleType, RoleTypeAnnotation},
        type_manager::{
            type_reader::TypeReader, validation::validation::capability_get_declared_annotation_by_category,
        },
        Capability, KindAPI, ObjectTypeAPI, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
    },
};

pub mod type_cache;
pub mod type_reader;
mod type_writer;
pub mod validation;

// TODO: this should be parametrised into the database options? Would be great to have it be changable at runtime!
pub(crate) const RELATION_INDEX_THRESHOLD: u64 = 8;

pub struct TypeManager {
    vertex_generator: Arc<TypeVertexGenerator>,
    definition_key_generator: Arc<DefinitionKeyGenerator>,
    type_cache: Option<Arc<TypeCache>>,
}

macro_rules! get_type_methods {
    ($(
        fn $method_name:ident() -> $output_type:ident = $cache_method:ident;
    )*) => {
        $(
            pub fn $method_name(
                &self, snapshot: &impl ReadableSnapshot, label: &Label<'_>
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

macro_rules! get_types_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $reader_method:ident | $cache_method:ident;
    )*) => {
        $(
            pub fn $method_name(&self, snapshot: &impl ReadableSnapshot) -> Result<Vec<$type_<'static>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method())
                } else {
                    TypeReader::$reader_method(snapshot)
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
                &self, snapshot: &impl ReadableSnapshot, type_: $type_<'static>
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

macro_rules! get_supertypes_transitive_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            // WARN: supertypes currently do NOT include themselves
            pub(crate) fn $method_name(
                &self, snapshot: &impl ReadableSnapshot, type_: $type_<'static>
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
                &self, snapshot: &impl ReadableSnapshot, type_: $type_<'static>
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
                &self, snapshot: &impl ReadableSnapshot, type_: $type_<'static>
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

macro_rules! get_type_label_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &impl ReadableSnapshot, type_: $type_<'static>
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

macro_rules! get_type_annotations_declared {
    ($(
        fn $method_name:ident() -> $type_:ident = $reader_method:ident | $cache_method:ident | $annotation_type:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &impl ReadableSnapshot, type_: $type_<'static>
            ) -> Result<MaybeOwns<'_, HashSet<$annotation_type>>, ConceptReadError> {
                 if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::Borrowed(cache.$cache_method(type_)))
                } else {
                    let annotations = TypeReader::$reader_method(snapshot, type_)?;
                    Ok(MaybeOwns::Owned(annotations))
                }
            }
        )*
    }
}

macro_rules! get_type_annotations {
    ($(
        fn $method_name:ident() -> $type_:ident = $reader_method:ident | $cache_method:ident | $annotation_type:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &impl ReadableSnapshot, type_: $type_<'static>
            ) -> Result<MaybeOwns<'_, HashMap<$annotation_type, $type_<'static>>>, ConceptReadError> {
                 if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::Borrowed(cache.$cache_method(type_)))
                } else {
                    let annotations = TypeReader::$reader_method(snapshot, type_)?;
                    Ok(MaybeOwns::Owned(annotations))
                }
            }
        )*
    }
}

macro_rules! storage_save_annotation {
    ($snapshot:ident, $type_:ident, $annotation:ident, $save_func:path) => {
        match $annotation {
            Annotation::Abstract(_) => $save_func($snapshot, $type_, None::<AnnotationAbstract>),
            Annotation::Distinct(_) => $save_func($snapshot, $type_, None::<AnnotationDistinct>),
            Annotation::Independent(_) => $save_func($snapshot, $type_, None::<AnnotationIndependent>),
            Annotation::Unique(_) => $save_func($snapshot, $type_, None::<AnnotationUnique>),
            Annotation::Key(_) => $save_func($snapshot, $type_, None::<AnnotationKey>),
            Annotation::Cascade(_) => $save_func($snapshot, $type_, None::<AnnotationCascade>),
            Annotation::Cardinality(card) => $save_func($snapshot, $type_, Some(card)),
            Annotation::Regex(regex) => $save_func($snapshot, $type_, Some(regex)),
            Annotation::Range(range) => $save_func($snapshot, $type_, Some(range)),
            Annotation::Values(values) => $save_func($snapshot, $type_, Some(values)),
        }
    };
}

macro_rules! storage_delete_annotation {
    ($snapshot:ident, $type_:ident, $annotation_category:ident, $get_func:path, $delete_func:ident) => {
        let annotations = $get_func($snapshot, $type_.clone())?;
        let annotation_exists = annotations
            .into_iter()
            .find(|annotation| annotation.clone().into().category() == $annotation_category)
            .is_some();

        if annotation_exists {
            match $annotation_category {
                AnnotationCategory::Abstract => TypeWriter::$delete_func::<AnnotationAbstract>($snapshot, $type_),
                AnnotationCategory::Distinct => TypeWriter::$delete_func::<AnnotationDistinct>($snapshot, $type_),
                AnnotationCategory::Independent => TypeWriter::$delete_func::<AnnotationIndependent>($snapshot, $type_),
                AnnotationCategory::Unique => TypeWriter::$delete_func::<AnnotationUnique>($snapshot, $type_),
                AnnotationCategory::Key => TypeWriter::$delete_func::<AnnotationKey>($snapshot, $type_),
                AnnotationCategory::Cardinality => TypeWriter::$delete_func::<AnnotationCardinality>($snapshot, $type_),
                AnnotationCategory::Regex => TypeWriter::$delete_func::<AnnotationRegex>($snapshot, $type_),
                AnnotationCategory::Cascade => TypeWriter::$delete_func::<AnnotationCascade>($snapshot, $type_),
                AnnotationCategory::Range => TypeWriter::$delete_func::<AnnotationRange>($snapshot, $type_),
                AnnotationCategory::Values => TypeWriter::$delete_func::<AnnotationValues>($snapshot, $type_),
            }
        }
    };
}

macro_rules! get_has_constraint {
    ($(
        fn $method_name:ident<'a>() -> $type_:ty = $constraint_method:path;
    )*) => {
        $(
            pub(crate) fn $method_name<'a>(
                &self, snapshot: &impl ReadableSnapshot, type_: $type_
            ) -> Result<bool, ConceptReadError> {
                let constraint = $constraint_method(type_.get_annotations(snapshot, self)?.keys());
                Ok(constraint.is_some())
            }
        )*
    }
}

macro_rules! get_constraint {
    ($(
        fn $method_name:ident<'a>() -> $type_:ty = $constraint_type:ident | $constraint_method:path;
    )*) => {
        $(
            pub(crate) fn $method_name<'a>(
                &self, snapshot: &impl ReadableSnapshot, type_: $type_
            ) -> Result<Option<$constraint_type>, ConceptReadError> {
                Ok($constraint_method(type_.get_annotations(snapshot, self)?.keys()))
            }
        )*
    }
}

impl TypeManager {
    pub fn new(
        definition_key_generator: Arc<DefinitionKeyGenerator>,
        vertex_generator: Arc<TypeVertexGenerator>,
        schema_cache: Option<Arc<TypeCache>>,
    ) -> Self {
        TypeManager { definition_key_generator, vertex_generator, type_cache: schema_cache }
    }

    pub fn resolve_relates(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation: RelationType<'static>,
        role_name: &str,
    ) -> Result<Option<Relates<'static>>, ConceptReadError> {
        // TODO: Efficiency. We could build an index in TypeCache.
        Ok(self.get_relation_type_relates(snapshot, relation)?.iter().find_map(|relates| {
            if self.get_role_type_label(snapshot, relates.role()).unwrap().name.as_str() == role_name {
                Some(relates.clone())
            } else {
                None
            }
        }))
    }

    pub fn get_struct_definition_key(
        &self,
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<Option<DefinitionKey<'static>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_struct_definition_key(name))
        } else {
            TypeReader::get_struct_definition_key(snapshot, name)
        }
    }

    pub fn get_struct_definition(
        &self,
        snapshot: &impl ReadableSnapshot,
        definition_key: DefinitionKey<'static>,
    ) -> Result<MaybeOwns<'_, StructDefinition>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_struct_definition(definition_key.clone())))
        } else {
            Ok(MaybeOwns::Owned(TypeReader::get_struct_definition(snapshot, definition_key.clone())?))
        }
    }

    pub fn resolve_struct_field(
        &self,
        snapshot: &impl ReadableSnapshot,
        fields_path: &[&str],
        definition: StructDefinition,
    ) -> Result<Vec<StructFieldIDUInt>, ConceptReadError> {
        let mut resolved = Vec::with_capacity(fields_path.len());
        let maybe_owns_definition = MaybeOwns::Borrowed(&definition);
        let mut at = maybe_owns_definition;
        for (i, f) in fields_path.iter().enumerate() {
            let field_idx_opt = at.field_names.get(*f);
            if let Some(field_idx) = field_idx_opt {
                resolved.push(*field_idx);
                let next_def = &at.fields[field_idx];
                match &next_def.value_type {
                    ValueType::Struct(definition_key) => {
                        at = self.get_struct_definition(snapshot, definition_key.clone())?;
                    }
                    _ => {
                        return if (i + 1) == fields_path.len() {
                            Ok(resolved)
                        } else {
                            Err(ConceptReadError::Encoding {
                                source: EncodingError::IndexingIntoNonStructField {
                                    struct_name: definition.name,
                                    field_path: fields_path.iter().map(|&str| str.to_owned()).collect(),
                                },
                            })
                        };
                    }
                }
            } else {
                return Err(ConceptReadError::Encoding {
                    source: EncodingError::StructFieldUnresolvable {
                        struct_name: definition.name,
                        field_path: fields_path.iter().map(|&str| str.to_owned()).collect(),
                    },
                });
            }
        }

        Err(ConceptReadError::Encoding {
            source: EncodingError::StructPathIncomplete {
                struct_name: definition.name,
                field_path: fields_path.iter().map(|&str| str.to_owned()).collect(),
            },
        })
    }

    get_type_methods! {
        fn get_object_type() -> ObjectType = get_object_type;
        fn get_entity_type() -> EntityType = get_entity_type;
        fn get_relation_type() -> RelationType = get_relation_type;
        fn get_role_type() -> RoleType = get_role_type;
        fn get_attribute_type() -> AttributeType = get_attribute_type;
    }

    get_types_methods! {
        fn get_object_types() -> ObjectType = get_object_types | get_object_types;
        fn get_entity_types() -> EntityType = get_entity_types | get_entity_types;
        fn get_relation_types() -> RelationType = get_relation_types | get_relation_types;
        fn get_role_types() -> RoleType = get_role_types | get_role_types;
        fn get_attribute_types() -> AttributeType = get_attribute_types | get_attribute_types;
    }

    get_supertype_methods! {
        fn get_entity_type_supertype() -> EntityType = get_supertype;
        fn get_relation_type_supertype() -> RelationType = get_supertype;
        fn get_role_type_supertype() -> RoleType = get_supertype;
        fn get_attribute_type_supertype() -> AttributeType = get_supertype;
    }

    get_supertypes_transitive_methods! {
        fn get_entity_type_supertypes() -> EntityType = get_supertypes_transitive;
        fn get_relation_type_supertypes() -> RelationType = get_supertypes_transitive;
        fn get_role_type_supertypes() -> RoleType = get_supertypes_transitive;
        fn get_attribute_type_supertypes() -> AttributeType = get_supertypes_transitive;
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

    get_type_label_methods! {
        fn get_entity_type_label() -> EntityType = get_label;
        fn get_relation_type_label() -> RelationType = get_label;
        fn get_role_type_label() -> RoleType = get_label;
        fn get_attribute_type_label() -> AttributeType = get_label;
    }

    pub fn get_roles_by_name(
        &self,
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<Option<MaybeOwns<'_, Vec<RoleType<'static>>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_roles_by_name(name).map(|roles| MaybeOwns::Borrowed(roles)))
        } else {
            let roles = TypeReader::get_roles_by_name(snapshot, name.to_owned())?;
            if roles.is_empty() {
                Ok(None)
            } else {
                Ok(Some(MaybeOwns::Owned(roles)))
            }
        }
    }

    pub(crate) fn get_entity_type_owns_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_declared(entity_type)))
        } else {
            let owns = TypeReader::get_capabilities_declared(snapshot, entity_type.clone())?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_relation_type_owns_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_declared(relation_type)))
        } else {
            let owns = TypeReader::get_capabilities_declared(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_owns_for_attribute_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_for_attribute_type_declared(attribute_type.clone())))
        } else {
            let plays =
                TypeReader::get_capabilities_for_interface_declared::<Owns<'static>>(snapshot, attribute_type.clone())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_owns_for_attribute(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<ObjectType<'static>, Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_for_attribute_type(attribute_type.clone())))
        } else {
            let owns = TypeReader::get_capabilities_for_interface::<Owns<'static>>(snapshot, attribute_type.clone())?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_relation_type_relates_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Relates<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_relation_type_relates_declared(relation_type)))
        } else {
            let relates = TypeReader::get_capabilities_declared::<Relates<'static>>(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_relation_type_relates(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Relates<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_relation_type_relates(relation_type)))
        } else {
            let relates = TypeReader::get_capabilities::<Relates<'static>>(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_plays_for_role_type_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_for_role_type_declared(role_type.clone())))
        } else {
            let plays =
                TypeReader::get_capabilities_for_interface_declared::<Plays<'static>>(snapshot, role_type.clone())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_relations_for_role_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Relates<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_role_type_relates(role_type.clone())))
        } else {
            let relates = TypeReader::get_role_type_relates(snapshot, role_type.clone())?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_plays_for_role_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<ObjectType<'static>, Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_for_role_type(role_type.clone())))
        } else {
            let plays = TypeReader::get_capabilities_for_interface::<Plays<'static>>(snapshot, role_type.clone())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_entity_type_owns(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns(entity_type)))
        } else {
            let owns = TypeReader::get_capabilities::<Owns<'static>>(snapshot, entity_type.into_owned_object_type())?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_relation_type_owns(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns(relation_type)))
        } else {
            let owns = TypeReader::get_capabilities::<Owns<'static>>(snapshot, relation_type.into_owned_object_type())?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_entity_type_owns_overrides(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<Owns<'static>, Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_object_owns_overrides(entity_type)))
        } else {
            let plays = TypeReader::get_object_capabilities_overrides::<Owns<'static>>(
                snapshot,
                entity_type.into_owned_object_type(),
            )?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_relation_type_owns_overrides(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<Owns<'static>, Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_object_owns_overrides(relation_type)))
        } else {
            let plays = TypeReader::get_object_capabilities_overrides::<Owns<'static>>(
                snapshot,
                relation_type.into_owned_object_type(),
            )?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn relation_index_available(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'_>,
    ) -> Result<bool, ConceptReadError> {
        // TODO: it would be good if this doesn't require recomputation
        let mut max_card = 0;
        let relates = relation_type.get_relates(snapshot, self)?;
        for relates in relates.iter() {
            let card = relates.get_cardinality(snapshot, self)?;
            match card.end() {
                None => return Ok(false),
                Some(end) => max_card += end,
            }
        }
        Ok(max_card <= RELATION_INDEX_THRESHOLD)
    }

    pub(crate) fn get_entity_type_plays_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'this, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_declared(entity_type)))
        } else {
            let plays = TypeReader::get_capabilities_declared(snapshot, entity_type.clone())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_entity_type_plays(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays(entity_type)))
        } else {
            let plays = TypeReader::get_capabilities::<Plays<'static>>(snapshot, entity_type.into_owned_object_type())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_entity_type_plays_overrides(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<Plays<'static>, Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_object_plays_overrides(entity_type)))
        } else {
            let plays = TypeReader::get_object_capabilities_overrides::<Plays<'static>>(
                snapshot,
                entity_type.into_owned_object_type(),
            )?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_relation_type_plays_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'this, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_declared(relation_type)))
        } else {
            let plays = TypeReader::get_capabilities_declared(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_relation_type_plays(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays(relation_type)))
        } else {
            let plays =
                TypeReader::get_capabilities::<Plays<'static>>(snapshot, relation_type.into_owned_object_type())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_relation_type_plays_overrides(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<Plays<'static>, Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_object_plays_overrides(relation_type)))
        } else {
            let plays = TypeReader::get_object_capabilities_overrides::<Plays<'static>>(
                snapshot,
                relation_type.into_owned_object_type(),
            )?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_plays_override(
        &self,
        snapshot: &impl ReadableSnapshot,
        plays: Plays<'static>,
    ) -> Result<MaybeOwns<'_, Option<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_override(plays)))
        } else {
            Ok(MaybeOwns::Owned(TypeReader::get_capability_override(snapshot, plays)?))
        }
    }

    pub(crate) fn get_plays_overriding(
        &self,
        snapshot: &impl ReadableSnapshot,
        plays: Plays<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_overriding(plays)))
        } else {
            Ok(MaybeOwns::Owned(TypeReader::get_overriding_capabilities(snapshot, plays)?))
        }
    }

    pub(crate) fn get_plays_overriding_transitive(
        &self,
        snapshot: &impl ReadableSnapshot,
        plays: Plays<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_overriding_transitive(plays)))
        } else {
            Ok(MaybeOwns::Owned(TypeReader::get_overriding_capabilities_transitive(snapshot, plays)?))
        }
    }

    pub(crate) fn get_attribute_type_value_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<(ValueType, AttributeType<'static>)>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_attribute_type_value_type(attribute_type).clone())
        } else {
            Ok(TypeReader::get_value_type(snapshot, attribute_type)?)
        }
    }

    pub(crate) fn get_attribute_type_value_type_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_attribute_type_value_type_declared(attribute_type).clone())
        } else {
            Ok(TypeReader::get_value_type_declared(snapshot, attribute_type)?)
        }
    }

    get_type_annotations_declared! {
        fn get_entity_type_annotations_declared() -> EntityType = get_type_annotations_declared | get_annotations_declared | EntityTypeAnnotation;
        fn get_relation_type_annotations_declared() -> RelationType = get_type_annotations_declared | get_annotations_declared | RelationTypeAnnotation;
        fn get_role_type_annotations_declared() -> RoleType = get_type_annotations_declared | get_annotations_declared | RoleTypeAnnotation;
        fn get_attribute_type_annotations_declared() -> AttributeType = get_type_annotations_declared | get_annotations_declared | AttributeTypeAnnotation;
    }

    get_type_annotations! {
        fn get_entity_type_annotations() -> EntityType = get_type_annotations | get_annotations | EntityTypeAnnotation;
        fn get_relation_type_annotations() -> RelationType = get_type_annotations | get_annotations | RelationTypeAnnotation;
        fn get_role_type_annotations() -> RoleType = get_type_annotations | get_annotations | RoleTypeAnnotation;
        fn get_attribute_type_annotations() -> AttributeType = get_type_annotations | get_annotations | AttributeTypeAnnotation;
    }

    pub(crate) fn get_owns_override(
        &self,
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
    ) -> Result<MaybeOwns<'_, Option<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_override(owns)))
        } else {
            Ok(MaybeOwns::Owned(TypeReader::get_capability_override(snapshot, owns)?))
        }
    }

    pub(crate) fn get_owns_overriding(
        &self,
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_overriding(owns)))
        } else {
            Ok(MaybeOwns::Owned(TypeReader::get_overriding_capabilities(snapshot, owns)?))
        }
    }

    pub(crate) fn get_owns_overriding_transitive(
        &self,
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_overriding_transitive(owns)))
        } else {
            Ok(MaybeOwns::Owned(TypeReader::get_overriding_capabilities_transitive(snapshot, owns)?))
        }
    }

    pub(crate) fn get_owns_ordering(
        &self,
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
    ) -> Result<Ordering, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_owns_ordering(owns))
        } else {
            Ok(TypeReader::get_type_edge_ordering(snapshot, owns)?)
        }
    }

    pub(crate) fn get_role_ordering(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType<'static>,
    ) -> Result<Ordering, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_role_type_ordering(role_type))
        } else {
            Ok(TypeReader::get_type_ordering(snapshot, role_type)?)
        }
    }

    pub(crate) fn get_role_type_relates(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType<'static>,
    ) -> Result<MaybeOwns<'_, Relates<'static>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_role_type_relates_declared(role_type)))
        } else {
            let relates = TypeReader::get_role_type_relates_declared(snapshot, role_type.clone())?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_relates_override(
        &self,
        snapshot: &impl ReadableSnapshot,
        relates: Relates<'static>,
    ) -> Result<MaybeOwns<'_, Option<Relates<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_relates_override(relates)))
        } else {
            Ok(MaybeOwns::Owned(TypeReader::get_capability_override(snapshot, relates)?))
        }
    }

    pub(crate) fn get_relates_overriding(
        &self,
        snapshot: &impl ReadableSnapshot,
        relates: Relates<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Relates<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_relates_overriding(relates)))
        } else {
            Ok(MaybeOwns::Owned(TypeReader::get_overriding_capabilities(snapshot, relates)?))
        }
    }

    pub(crate) fn get_relates_overriding_transitive(
        &self,
        snapshot: &impl ReadableSnapshot,
        relates: Relates<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Relates<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_relates_overriding_transitive(relates)))
        } else {
            Ok(MaybeOwns::Owned(TypeReader::get_overriding_capabilities_transitive(snapshot, relates)?))
        }
    }

    pub(crate) fn get_owns_annotations_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
    ) -> Result<MaybeOwns<'this, HashSet<OwnsAnnotation>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_annotations_declared(owns)))
        } else {
            let annotations = TypeReader::get_type_edge_annotations_declared(snapshot, owns)?
                .into_iter()
                .map(|annotation| OwnsAnnotation::try_from(annotation).unwrap())
                .collect();
            Ok(MaybeOwns::Owned(annotations))
        }
    }

    pub(crate) fn get_owns_annotations<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
    ) -> Result<MaybeOwns<'this, HashMap<OwnsAnnotation, Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_annotations(owns)))
        } else {
            let annotations = TypeReader::get_type_edge_annotations(snapshot, owns)?
                .into_iter()
                .map(|(annotation, owns)| (OwnsAnnotation::try_from(annotation).unwrap(), owns))
                .collect();
            Ok(MaybeOwns::Owned(annotations))
        }
    }

    pub(crate) fn get_plays_annotations_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        plays: Plays<'static>,
    ) -> Result<MaybeOwns<'this, HashSet<PlaysAnnotation>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_annotations_declared(plays)))
        } else {
            let annotations = TypeReader::get_type_edge_annotations_declared(snapshot, plays)?
                .into_iter()
                .map(|annotation| PlaysAnnotation::try_from(annotation).unwrap())
                .collect();
            Ok(MaybeOwns::Owned(annotations))
        }
    }

    pub(crate) fn get_plays_annotations<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        plays: Plays<'static>,
    ) -> Result<MaybeOwns<'this, HashMap<PlaysAnnotation, Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_annotations(plays)))
        } else {
            let annotations = TypeReader::get_type_edge_annotations(snapshot, plays)?
                .into_iter()
                .map(|(annotation, plays)| (PlaysAnnotation::try_from(annotation).unwrap(), plays))
                .collect();
            Ok(MaybeOwns::Owned(annotations))
        }
    }

    pub(crate) fn get_relates_annotations_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        relates: Relates<'static>,
    ) -> Result<MaybeOwns<'this, HashSet<RelatesAnnotation>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_relates_annotations_declared(relates)))
        } else {
            let annotations = TypeReader::get_type_edge_annotations_declared(snapshot, relates)?
                .into_iter()
                .map(|annotation| RelatesAnnotation::try_from(annotation).unwrap())
                .collect();
            Ok(MaybeOwns::Owned(annotations))
        }
    }

    pub(crate) fn get_relates_annotations<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        relates: Relates<'static>,
    ) -> Result<MaybeOwns<'this, HashMap<RelatesAnnotation, Relates<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_relates_annotations(relates)))
        } else {
            let annotations = TypeReader::get_type_edge_annotations(snapshot, relates)?
                .into_iter()
                .map(|(annotation, relates)| (RelatesAnnotation::try_from(annotation).unwrap(), relates))
                .collect();
            Ok(MaybeOwns::Owned(annotations))
        }
    }

    pub fn get_cardinality<'a, CAP: Capability<'a>>(
        &self,
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
    ) -> Result<AnnotationCardinality, ConceptReadError> {
        let cardinality = Constraint::compute_cardinality(
            capability.get_annotations(snapshot, self)?.keys(),
            Some(capability.get_default_cardinality(snapshot, self)?),
        );
        match cardinality {
            Some(cardinality) => Ok(cardinality),
            None => Err(ConceptReadError::CorruptMissingMandatoryCardinality),
        }
    }

    pub fn get_cardinality_declared<'a, CAP: Capability<'a>>(
        &self,
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
    ) -> Result<Option<AnnotationCardinality>, ConceptReadError> {
        let cardinality = Constraint::compute_cardinality(
            capability.get_annotations_declared(snapshot, self)?.iter(),
            None, // no default
        );
        Ok(cardinality)
    }

    pub(crate) fn get_cardinality_inherited_or_default<CAP: Capability<'static>>(
        &self,
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
        set_capability_override: Option<Option<CAP>>,
    ) -> Result<AnnotationCardinality, ConceptReadError> {
        let capability_override = match set_capability_override {
            None => TypeReader::get_capability_override(snapshot, capability.clone())?,
            Some(set_capability_override) => set_capability_override,
        };

        if let Some(capability_override) = capability_override {
            capability_override.get_cardinality(snapshot, self)
        } else {
            capability.get_default_cardinality(snapshot, self)
        }
    }

    pub fn get_owns_default_cardinality<'a>(&self, ordering: Ordering) -> AnnotationCardinality {
        match ordering {
            Ordering::Unordered => Owns::DEFAULT_UNORDERED_CARDINALITY,
            Ordering::Ordered => Owns::DEFAULT_ORDERED_CARDINALITY,
        }
    }

    pub fn get_plays_default_cardinality<'a>(&self) -> AnnotationCardinality {
        Plays::DEFAULT_CARDINALITY
    }

    pub fn get_relates_default_cardinality<'a>(&self, role_ordering: Ordering) -> AnnotationCardinality {
        match role_ordering {
            Ordering::Unordered => Relates::DEFAULT_UNORDERED_CARDINALITY,
            Ordering::Ordered => Relates::DEFAULT_ORDERED_CARDINALITY,
        }
    }

    pub fn get_owns_is_distinct<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'a>,
    ) -> Result<bool, ConceptReadError> {
        let default = match owns.get_ordering(snapshot, self)? {
            Ordering::Ordered => None,
            Ordering::Unordered => Some(AnnotationDistinct),
        };

        Ok(Constraint::compute_distinct(owns.get_annotations(snapshot, self)?.keys(), default).is_some())
    }

    pub fn get_relates_is_distinct<'a>(
        &self,
        snapshot: &impl ReadableSnapshot,
        relates: Relates<'a>,
    ) -> Result<bool, ConceptReadError> {
        let default = match relates.role().get_ordering(snapshot, self)? {
            Ordering::Ordered => None,
            Ordering::Unordered => Some(AnnotationDistinct),
        };

        Ok(Constraint::compute_distinct(relates.get_annotations(snapshot, self)?.keys(), default).is_some())
    }

    get_has_constraint! {
        fn get_type_is_abstract<'a>() -> impl KindAPI<'a> = Constraint::compute_abstract;
        fn get_owns_is_unique<'a>() -> Owns<'a> = Constraint::compute_unique;
        fn get_owns_is_key<'a>() -> Owns<'a> = Constraint::compute_key;
        fn get_attribute_type_is_independent<'a>() -> AttributeType<'a> = Constraint::compute_independent;
        fn get_relation_type_is_cascade<'a>() -> RelationType<'a> = Constraint::compute_cascade;
    }

    get_constraint! {
        fn get_owns_regex<'a>() -> Owns<'a> = AnnotationRegex | Constraint::compute_regex;
        fn get_owns_range<'a>() -> Owns<'a> = AnnotationRange | Constraint::compute_range;
        fn get_owns_values<'a>() -> Owns<'a> = AnnotationValues | Constraint::compute_values;
        fn get_attribute_type_regex<'a>() -> AttributeType<'a> = AnnotationRegex | Constraint::compute_regex;
        fn get_attribute_type_range<'a>() -> AttributeType<'a> = AnnotationRange | Constraint::compute_range;
        fn get_attribute_type_values<'a>() -> AttributeType<'a> = AnnotationValues | Constraint::compute_values;
    }

    pub(crate) fn get_type_annotation_source<T: KindAPI<'static>>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_: T,
        annotation: T::AnnotationType,
    ) -> Result<Option<T>, ConceptReadError> {
        Ok(type_.get_annotations(snapshot, self)?.get(&annotation).cloned())
    }

    pub(crate) fn get_capability_annotation_source<CAP: Capability<'static>>(
        &self,
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
        annotation: CAP::AnnotationType,
    ) -> Result<Option<CAP>, ConceptReadError> {
        Ok(capability.get_annotations(snapshot, self)?.get(&annotation).cloned())
    }

    pub(crate) fn get_independent_attribute_types(
        &self,
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Arc<HashSet<AttributeType<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_independent_attribute_types())
        } else {
            let mut independent = HashSet::new();
            for type_ in self.get_attribute_types(snapshot)?.into_iter() {
                if type_.is_independent(snapshot, self)? {
                    independent.insert(type_.clone());
                }
            }
            Ok(Arc::new(independent))
        }
    }
}

impl TypeManager {
    pub fn validate(&self, snapshot: &impl WritableSnapshot) -> Result<(), Vec<ConceptWriteError>> {
        let type_errors = CommitTimeValidation::validate(&self, snapshot);
        match type_errors {
            Ok(errors) => {
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errors.into_iter().map(|error| ConceptWriteError::SchemaValidation { source: error }).collect())
                }
            }
            Err(error) => Err(vec![ConceptWriteError::ConceptRead { source: error }]),
        }
    }

    pub fn create_struct(
        &self,
        snapshot: &mut impl WritableSnapshot,
        name: String,
    ) -> Result<DefinitionKey<'static>, ConceptWriteError> {
        OperationTimeValidation::validate_struct_name_uniqueness(snapshot, &name)
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let definition_key = self
            .definition_key_generator
            .create_struct(snapshot)
            .map_err(|source| ConceptWriteError::Encoding { source })?;

        TypeWriter::storage_insert_struct(snapshot, definition_key.clone(), StructDefinition::new(name));
        Ok(definition_key)
    }

    pub fn create_struct_field(
        &self,
        snapshot: &mut impl WritableSnapshot,
        definition_key: DefinitionKey<'static>,
        field_name: &str,
        value_type: ValueType,
        is_optional: bool,
    ) -> Result<(), ConceptWriteError> {
        let mut struct_definition = TypeReader::get_struct_definition(snapshot, definition_key.clone())?;
        struct_definition
            .add_field(field_name, value_type, is_optional)
            .map_err(|source| ConceptWriteError::Encoding { source })?;

        TypeWriter::storage_insert_struct(snapshot, definition_key.clone(), struct_definition);
        Ok(())
    }

    pub fn delete_struct_field(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        definition_key: DefinitionKey<'static>,
        field_name: String,
    ) -> Result<(), ConceptWriteError> {
        let mut struct_definition = TypeReader::get_struct_definition(snapshot, definition_key.clone())?;
        struct_definition.delete_field(field_name).map_err(|source| ConceptWriteError::Encoding { source })?;

        TypeWriter::storage_insert_struct(snapshot, definition_key.clone(), struct_definition);
        Ok(())
    }

    pub fn delete_struct(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        definition_key: &DefinitionKey<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_deleted_struct_is_not_used_in_schema(snapshot, definition_key)
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_delete_struct(snapshot, definition_key);
        Ok(())
    }

    pub fn create_entity_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        label: &Label<'_>,
    ) -> Result<EntityType<'static>, ConceptWriteError> {
        OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let type_vertex = self
            .vertex_generator
            .create_entity_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let entity = EntityType::new(type_vertex);

        TypeWriter::storage_put_label(snapshot, entity.clone(), label);
        Ok(entity)
    }

    pub fn create_relation_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        label: &Label<'_>,
    ) -> Result<RelationType<'static>, ConceptWriteError> {
        OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let type_vertex = self
            .vertex_generator
            .create_relation_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let relation = RelationType::new(type_vertex);

        TypeWriter::storage_put_label(snapshot, relation.clone(), label);
        Ok(relation)
    }

    pub(crate) fn create_role_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        label: &Label<'_>,
        relation_type: RelationType<'static>,
        ordering: Ordering,
        cardinality: Option<AnnotationCardinality>,
    ) -> Result<RoleType<'static>, ConceptWriteError> {
        OperationTimeValidation::validate_new_role_name_uniqueness(
            snapshot,
            relation_type.clone().into_owned(),
            &label.clone().into_owned(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // Capabilities can have default values for annotations (e.g. @card(1..X)),
        // and they can contradict the absence of data for their object types.
        // It is dirty to delete role type right after creating it with this validation,
        // but it allows us to have more consistent and scalable validations.
        let type_vertex = self
            .vertex_generator
            .create_role_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let role_type = RoleType::new(type_vertex);
        let relates = Relates::new(relation_type, role_type.clone());

        TypeWriter::storage_put_label(snapshot, role_type.clone(), &label);
        TypeWriter::storage_put_type_vertex_property(snapshot, role_type.clone(), Some(ordering));
        TypeWriter::storage_put_edge(snapshot, relates.clone());

        let cardinality_error: Option<ConceptWriteError> = match cardinality {
            None => {
                if let Err(error) = OperationTimeValidation::validate_new_acquired_relates_compatible_with_instances(
                    snapshot,
                    self,
                    thing_manager,
                    relates.clone().into_owned(),
                    HashSet::from([Annotation::Cardinality(self.get_relates_default_cardinality(ordering))]),
                ) {
                    Some(ConceptWriteError::SchemaValidation { source: error })
                } else {
                    None
                }
            }
            Some(cardinality) => {
                if let Err(error) =
                    self.set_relates_annotation_cardinality(snapshot, thing_manager, relates.clone(), cardinality)
                {
                    Some(error)
                } else {
                    None
                }
            }
        };

        match cardinality_error {
            None => Ok(role_type),
            Some(error) => {
                TypeWriter::storage_unput_edge(snapshot, relates);
                TypeWriter::storage_unput_type_vertex_property::<Ordering>(snapshot, role_type.clone(), Some(ordering));
                TypeWriter::storage_unput_label(snapshot, role_type.clone(), &label);
                TypeWriter::storage_unput_vertex(snapshot, role_type);
                Err(error)
            }
        }
    }

    pub fn create_attribute_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        label: &Label<'_>,
    ) -> Result<AttributeType<'static>, ConceptWriteError> {
        OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let type_vertex = self
            .vertex_generator
            .create_attribute_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let attribute_type = AttributeType::new(type_vertex);

        TypeWriter::storage_put_label(snapshot, attribute_type.clone(), label);
        Ok(attribute_type)
    }

    fn delete_object_type_capabilities_unchecked(
        &self,
        snapshot: &mut impl WritableSnapshot,
        object_type: ObjectType<'static>,
    ) -> Result<(), ConceptWriteError> {
        for owns in TypeReader::get_capabilities_declared::<Owns<'static>>(snapshot, object_type.clone())? {
            self.unset_owns_unchecked(snapshot, owns)?;
        }

        for plays in TypeReader::get_capabilities_declared::<Plays<'static>>(snapshot, object_type.clone())? {
            self.unset_plays_unchecked(snapshot, plays)?;
        }

        Ok(())
    }

    fn validate_delete_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_no_subtypes_for_type_deletion(snapshot, type_.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_no_instances_to_delete(snapshot, thing_manager, type_)
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        Ok(())
    }

    pub(crate) fn delete_entity_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        entity_type: EntityType<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.validate_delete_type(snapshot, thing_manager, entity_type.clone())?;

        self.delete_object_type_capabilities_unchecked(snapshot, entity_type.clone().into_owned_object_type())?;
        self.delete_type(snapshot, entity_type)
    }

    pub(crate) fn delete_relation_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.validate_delete_type(snapshot, thing_manager, relation_type.clone())?;

        let declared_relates =
            TypeReader::get_capabilities_declared::<Relates<'static>>(snapshot, relation_type.clone())?;
        for relates in declared_relates.iter() {
            self.delete_role_type(snapshot, thing_manager, relates.role())?;
        }
        self.delete_object_type_capabilities_unchecked(snapshot, relation_type.clone().into_owned_object_type())?;
        self.delete_type(snapshot, relation_type)
    }

    pub(crate) fn delete_attribute_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.validate_delete_type(snapshot, thing_manager, attribute_type.clone())?;

        for owns in
            TypeReader::get_capabilities_for_interface_declared::<Owns<'static>>(snapshot, attribute_type.clone())?
        {
            self.unset_owns_unchecked(snapshot, owns)?;
        }

        self.unset_value_type_unchecked(snapshot, attribute_type.clone())?;
        self.delete_type(snapshot, attribute_type)
    }

    pub(crate) fn delete_role_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.validate_delete_type(snapshot, thing_manager, role_type.clone())?;

        let relates = TypeReader::get_role_type_relates_declared(snapshot, role_type.clone())?;

        OperationTimeValidation::validate_no_overriding_capabilities_for_capability_unset(snapshot, relates.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_no_instances_to_unset_relates(
            snapshot,
            self,
            thing_manager,
            relates.relation(),
            role_type.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.delete_relates_and_its_role_type_unchecked(snapshot, relates)
    }

    fn delete_relates_and_its_role_type_unchecked(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relates: Relates<'static>,
    ) -> Result<(), ConceptWriteError> {
        for plays in TypeReader::get_capabilities_for_interface_declared::<Plays<'static>>(snapshot, relates.role())? {
            self.unset_plays_unchecked(snapshot, plays)?;
        }

        self.unset_relates_unchecked(snapshot, relates.clone())?;
        TypeWriter::storage_delete_type_vertex_property::<Ordering>(snapshot, relates.role());
        self.delete_type(snapshot, relates.role())
    }

    fn delete_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        for annotation in TypeReader::get_type_annotations_declared(snapshot, type_.clone())? {
            self.unset_annotation(snapshot, type_.clone(), annotation.into().category())?;
        }
        self.unset_supertype(snapshot, type_.clone())?;
        TypeWriter::storage_delete_label(snapshot, type_.clone());
        TypeWriter::storage_delete_vertex(snapshot, type_);
        Ok(())
    }

    fn unset_owns_unchecked(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        TypeWriter::storage_delete_type_edge_property::<Ordering>(snapshot, owns.clone());
        self.unset_capability(snapshot, owns)
    }

    fn unset_plays_unchecked(
        &self,
        snapshot: &mut impl WritableSnapshot,
        plays: Plays<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.unset_capability(snapshot, plays)
    }

    fn unset_relates_unchecked(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relates: Relates<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.unset_capability(snapshot, relates)
    }

    fn unset_capability(
        &self,
        snapshot: &mut impl WritableSnapshot,
        capability: impl Capability<'static>,
    ) -> Result<(), ConceptWriteError> {
        for annotation in TypeReader::get_type_edge_annotations_declared(snapshot, capability.clone())? {
            self.unset_capability_annotation(snapshot, capability.clone(), annotation.clone().into().category())?;
        }
        self.unset_override(snapshot, capability.clone())?;
        TypeWriter::storage_delete_edge(snapshot, capability.clone());
        Ok(())
    }

    pub(crate) fn set_label<T: KindAPI<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: T,
        label: &Label<'_>,
    ) -> Result<(), ConceptWriteError> {
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, type_.clone()).is_ok());

        match T::ROOT_KIND {
            Kind::Entity | Kind::Attribute => {
                OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
                    .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
            }
            Kind::Relation => unreachable!("Use set_relation_type_label instead"),
            Kind::Role => unreachable!("Use set_name instead"),
        }

        TypeWriter::storage_delete_label(snapshot, type_.clone());
        TypeWriter::storage_put_label(snapshot, type_, &label);
        Ok(())
    }

    pub(crate) fn set_relation_type_label(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation_type: RelationType<'static>,
        label: &Label<'_>,
    ) -> Result<(), ConceptWriteError> {
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, relation_type.clone()).is_ok());

        OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_delete_label(snapshot, relation_type.clone());
        TypeWriter::storage_put_label(snapshot, relation_type.clone(), &label);

        let relates = self.get_relation_type_relates_declared(snapshot, relation_type)?;
        for relate in &relates {
            self.set_role_type_scope(snapshot, relate.role(), label.clone().name().as_str())?;
        }

        Ok(())
    }

    pub(crate) fn set_role_type_name(
        &self,
        snapshot: &mut impl WritableSnapshot,
        role_type: RoleType<'static>,
        name: &str,
    ) -> Result<(), ConceptWriteError> {
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, role_type.clone()).is_ok());

        let old_label = TypeReader::get_label(snapshot, role_type.clone())?.unwrap();
        debug_assert!(old_label.scope().is_some());

        let new_label = Label::build_scoped(name, old_label.scope().unwrap().as_str());
        let relation_type = TypeReader::get_role_type_relates_declared(snapshot, role_type.clone())?.relation();

        OperationTimeValidation::validate_new_role_name_uniqueness(
            snapshot,
            relation_type,
            &new_label.clone().into_owned(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_delete_label(snapshot, role_type.clone());
        TypeWriter::storage_put_label(snapshot, role_type, &new_label);
        Ok(())
    }

    fn set_role_type_scope(
        &self,
        snapshot: &mut impl WritableSnapshot,
        role_type: RoleType<'static>,
        scope: &str,
    ) -> Result<(), ConceptWriteError> {
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, role_type.clone()).is_ok());

        let old_label = TypeReader::get_label(snapshot, role_type.clone())?.unwrap();
        debug_assert!(old_label.scope().is_some());

        let new_label = Label::build_scoped(old_label.name().as_str(), scope);

        TypeWriter::storage_delete_label(snapshot, role_type.clone());
        TypeWriter::storage_put_label(snapshot, role_type, &new_label);
        Ok(())
    }

    pub(crate) fn set_value_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        value_type: ValueType,
    ) -> Result<(), ConceptWriteError> {
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, attribute_type.clone()).is_ok());

        OperationTimeValidation::validate_value_type_compatible_with_inherited_value_type(
            snapshot,
            attribute_type.clone(),
            value_type.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_subtypes_value_types_compatible_with_new_value_type(
            snapshot,
            attribute_type.clone(),
            value_type.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_attribute_type_value_type_compatible_with_annotations_transitive(
            snapshot,
            attribute_type.clone(),
            Some(value_type.clone()),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_value_type_compatible_with_all_owns_annotations_transitive(
            snapshot,
            attribute_type.clone(),
            Some(value_type.clone()),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let existing_value_type = TypeReader::get_value_type_without_source(snapshot, attribute_type.clone())?;
        match existing_value_type {
            Some(existing_value_type) if value_type != existing_value_type => {
                OperationTimeValidation::validate_no_instances_to_change_value_type(
                    snapshot,
                    thing_manager,
                    attribute_type.clone(),
                )
                .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
            }
            _ => {}
        }

        TypeWriter::storage_set_value_type(snapshot, attribute_type, value_type);
        Ok(())
    }

    pub(crate) fn unset_value_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_value_type_can_be_unset(snapshot, thing_manager, attribute_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.unset_value_type_unchecked(snapshot, attribute_type)
    }

    fn unset_value_type_unchecked(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        let value_type = attribute_type.get_value_type_declared(snapshot, self)?;
        if value_type.is_some() {
            TypeWriter::storage_unset_value_type(snapshot, attribute_type);
        }
        Ok(())
    }

    fn set_supertype<K: KindAPI<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        subtype: K,
        supertype: K,
    ) -> Result<(), ConceptWriteError> {
        debug_assert! {
            OperationTimeValidation::validate_type_exists(snapshot, subtype.clone()).is_ok() &&
                OperationTimeValidation::validate_type_exists(snapshot, supertype.clone()).is_ok()
        };

        OperationTimeValidation::validate_sub_does_not_create_cycle(snapshot, subtype.clone(), supertype.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_supertype_annotations_compatibility(
            snapshot,
            self,
            subtype.clone(),
            supertype.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.unset_supertype(snapshot, subtype.clone())?;
        TypeWriter::storage_put_supertype(snapshot, subtype.clone(), supertype.clone());
        Ok(())
    }

    fn unset_supertype<T: TypeAPI<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        subtype: T,
    ) -> Result<(), ConceptWriteError> {
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, subtype.clone()).is_ok());
        TypeWriter::storage_may_delete_supertype(snapshot, subtype.clone())?;
        Ok(())
    }

    fn unset_override<CAP: Capability<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        overriding_capability: CAP,
    ) -> Result<(), ConceptWriteError> {
        if TypeReader::get_capability_override(snapshot, overriding_capability.clone())?.is_some() {
            TypeWriter::storage_delete_type_edge_overridden(snapshot, overriding_capability);
        }
        Ok(())
    }

    pub(crate) fn set_attribute_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: AttributeType<'static>,
        supertype: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_value_type_is_compatible_with_new_supertypes_value_type_transitive(
            snapshot,
            thing_manager,
            subtype.clone(),
            Some(supertype.clone()),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_attribute_type_supertype_is_abstract(snapshot, self, supertype.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_interface_change_supertype_does_not_corrupt_capabilities_overrides_transitive::<Owns<'static>>(
            snapshot,
            subtype.clone(),
            Some(supertype.clone()),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_attribute_type_does_not_lose_instances_with_independent_annotation_with_new_supertype(
            snapshot,
            thing_manager,
            subtype.clone(),
            Some(supertype.clone()),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_updated_annotations_compatible_with_attribute_type_and_subtypes_instances_on_supertype_change(
            snapshot,
            self,
            thing_manager,
            subtype.clone(),
            supertype.clone(),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_supertype(snapshot, subtype, supertype)
    }

    pub(crate) fn unset_attribute_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_value_type_is_compatible_with_new_supertypes_value_type_transitive(
            snapshot,
            thing_manager,
            subtype.clone(),
            None, // supertype
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_interface_change_supertype_does_not_corrupt_capabilities_overrides_transitive::<Owns<'static>>(
            snapshot,
            subtype.clone(),
            None, // new_interface_supertype
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_attribute_type_does_not_lose_instances_with_independent_annotation_with_new_supertype(
            snapshot,
            thing_manager,
            subtype.clone(),
            None, // supertype
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.unset_supertype(snapshot, subtype)
    }

    pub(crate) fn set_object_type_supertype<T: ObjectTypeAPI<'static> + KindAPI<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: T,
        supertype: T,
    ) -> Result<(), ConceptWriteError> {
        let object_subtype = subtype.clone().into_owned_object_type();
        let object_supertype = supertype.clone().into_owned_object_type();

        OperationTimeValidation::validate_capabilities_are_not_overridden_in_the_new_supertype_transitive::<
            Owns<'static>,
        >(snapshot, object_subtype.clone(), object_supertype.clone())
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_capability_overrides_compatible_with_new_supertype_transitive::<Owns<'static>>(
            snapshot,
            object_subtype.clone(),
            Some(object_supertype.clone()),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_lost_owns_do_not_cause_lost_instances_while_changing_supertype(
            snapshot,
            self,
            thing_manager,
            object_subtype.clone(),
            Some(object_supertype.clone()),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_changed_annotations_compatible_with_owns_and_overriding_owns_instances_on_supertype_change(
            snapshot,
            self,
            thing_manager,
            object_subtype.clone(),
            object_supertype.clone(),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_capabilities_are_not_overridden_in_the_new_supertype_transitive::<
            Plays<'static>,
        >(snapshot, object_subtype.clone(), object_supertype.clone())
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_capability_overrides_compatible_with_new_supertype_transitive::<Plays<'static>>(
            snapshot,
            object_subtype.clone(),
            Some(object_supertype.clone()),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_lost_plays_do_not_cause_lost_instances_while_changing_supertype(
            snapshot,
            self,
            thing_manager,
            object_subtype.clone(),
            Some(object_supertype.clone()),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_changed_annotations_compatible_with_plays_and_overriding_plays_instances_on_supertype_change(
            snapshot,
            self,
            thing_manager,
            object_subtype,
            object_supertype,
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_supertype(snapshot, subtype, supertype)
    }

    pub(crate) fn unset_object_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: ObjectType<'static>,
    ) -> Result<(), ConceptWriteError> {
        let object_subtype = subtype.clone().into_owned_object_type();

        OperationTimeValidation::validate_capability_overrides_compatible_with_new_supertype_transitive::<Owns<'static>>(
            snapshot,
            object_subtype.clone(),
            None, // supertype
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_lost_owns_do_not_cause_lost_instances_while_changing_supertype(
            snapshot,
            self,
            thing_manager,
            object_subtype.clone(),
            None, // supertype
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_capability_overrides_compatible_with_new_supertype_transitive::<Plays<'static>>(
            snapshot,
            object_subtype.clone(),
            None, // supertype
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_lost_plays_do_not_cause_lost_instances_while_changing_supertype(
            snapshot,
            self,
            thing_manager,
            object_subtype.clone(),
            None, // supertype
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.unset_supertype(snapshot, subtype)
    }

    pub(crate) fn set_entity_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: EntityType<'static>,
        supertype: EntityType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_updated_annotations_compatible_with_entity_type_and_subtypes_instances_on_supertype_change(
            snapshot,
            self,
            thing_manager,
            subtype.clone(),
            supertype.clone(),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_object_type_supertype(snapshot, thing_manager, subtype, supertype)
    }

    pub(crate) fn unset_entity_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: EntityType<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.unset_object_type_supertype(snapshot, thing_manager, subtype.into_owned_object_type())
    }

    pub(crate) fn set_relation_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: RelationType<'static>,
        supertype: RelationType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_role_names_compatible_with_new_relation_supertype_transitive(
            snapshot,
            subtype.clone(),
            supertype.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_updated_annotations_compatible_with_relation_type_and_subtypes_instances_on_supertype_change(
            snapshot,
            self,
            thing_manager,
            subtype.clone(),
            supertype.clone(),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_capability_overrides_compatible_with_new_supertype_transitive::<
            Relates<'static>,
        >(snapshot, subtype.clone(), Some(supertype.clone()))
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_relation_type_does_not_acquire_cascade_annotation_to_lose_instances_with_new_supertype(
            snapshot,
            thing_manager,
            subtype.clone(),
            supertype.clone(),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_lost_relates_do_not_cause_lost_instances_while_changing_supertype(
            snapshot,
            self,
            thing_manager,
            subtype.clone(),
            Some(supertype.clone()),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_changed_annotations_compatible_with_relates_and_overriding_relates_instances_on_supertype_change(
            snapshot,
            self,
            thing_manager,
            subtype.clone(),
            supertype.clone(),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_object_type_supertype(snapshot, thing_manager, subtype, supertype)
    }

    pub(crate) fn unset_relation_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: RelationType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_capability_overrides_compatible_with_new_supertype_transitive::<
            Relates<'static>,
        >(
            snapshot,
            subtype.clone(),
            None, // supertype
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_lost_relates_do_not_cause_lost_instances_while_changing_supertype(
            snapshot,
            self,
            thing_manager,
            subtype.clone(),
            None, // supertype
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.unset_object_type_supertype(snapshot, thing_manager, subtype.into_owned_object_type())
    }

    pub(crate) fn set_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owner: ObjectType<'static>,
        attribute: AttributeType<'static>,
        ordering: Ordering,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_capability_abstractness::<Owns<'static>>(
            snapshot,
            self,
            owner.clone(),
            attribute.clone(),
            None,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_interface_not_overridden::<Owns<'static>>(
            snapshot,
            owner.clone(),
            attribute.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let owns = Owns::new(ObjectType::new(owner.clone().into_vertex()), attribute.clone());

        let initial_annotations =
            HashSet::from([Annotation::Cardinality(self.get_owns_default_cardinality(ordering.clone()))]);
        OperationTimeValidation::validate_new_acquired_owns_compatible_with_instances(
            snapshot,
            self,
            thing_manager,
            owns.clone().into_owned(),
            initial_annotations,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_put_edge(snapshot, owns.clone());
        TypeWriter::storage_put_type_edge_property(snapshot, owns, Some(ordering));
        Ok(())
    }

    pub(crate) fn unset_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owner: ObjectType<'static>,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_unset_owns_is_not_inherited(snapshot, owner.clone(), attribute_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let owns_declared = owner.get_owns_declared(snapshot, self)?;
        if let Some(owns) = owns_declared.iter().find(|owns| owns.attribute() == attribute_type) {
            OperationTimeValidation::validate_no_overriding_capabilities_for_capability_unset(snapshot, owns.clone())
                .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

            OperationTimeValidation::validate_no_instances_to_unset_owns(
                snapshot,
                self,
                thing_manager,
                owner.clone(),
                attribute_type.clone(),
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

            self.unset_owns_unchecked(snapshot, owns.clone())?;
        }
        Ok(())
    }

    pub(crate) fn set_owns_override(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
        overridden: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_owns_is_inherited(snapshot, owns.owner(), overridden.attribute())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_overridden_interface_type_is_supertype_or_self::<Owns<'static>>(
            snapshot,
            owns.clone(),
            overridden.attribute(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_owns_override_ordering_match(
            snapshot,
            owns.clone(),
            overridden.clone(),
            None,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_capability_override_annotations_compatibility_transitive(
            snapshot,
            self,
            owns.clone(),
            overridden.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_object_type_subtypes_do_not_override_new_overridden_capability(
            snapshot,
            owns.clone(),
            overridden.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_owns_value_type_compatible_with_overridden_owns_annotations_transitive(
            snapshot,
            owns.clone(),
            overridden.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_override(
            snapshot,
            self,
            owns.clone(),
            Some(overridden.clone()),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        if owns.attribute() != overridden.attribute() {
            OperationTimeValidation::validate_no_instances_to_override_owns(
                snapshot,
                self,
                thing_manager,
                owns.owner(),
                overridden.attribute(),
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        }

        OperationTimeValidation::validate_updated_annotations_compatible_with_owns_and_overriding_owns_instances_on_override(
            snapshot,
            self,
            thing_manager,
            owns.clone(),
            Some(overridden.clone()),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_set_type_edge_overridden(snapshot, owns, overridden);
        Ok(())
    }

    pub(crate) fn unset_owns_override(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        if let Some(overridden) = &*owns.get_override(snapshot, self)? {
            OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_override(
                snapshot,
                self,
                owns.clone(),
                None, // unset override
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

            OperationTimeValidation::validate_updated_annotations_compatible_with_owns_and_overriding_owns_instances_on_override(
                snapshot,
                self,
                thing_manager,
                owns.clone(),
                None, // unset override
            )
                .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

            let overridden_annotations = overridden
                .get_annotations(snapshot, self)?
                .keys()
                .cloned()
                .map(|annotation| annotation.try_into().unwrap())
                .collect();
            OperationTimeValidation::validate_new_acquired_owns_compatible_with_instances(
                snapshot,
                self,
                thing_manager,
                overridden.clone(),
                overridden_annotations,
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

            TypeWriter::storage_delete_type_edge_overridden(snapshot, owns);
        }
        Ok(())
    }

    pub(crate) fn set_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        player: ObjectType<'static>,
        role: RoleType<'static>,
    ) -> Result<Plays<'static>, ConceptWriteError> {
        OperationTimeValidation::validate_capability_abstractness::<Plays<'static>>(
            snapshot,
            self,
            player.clone(),
            role.clone(),
            None,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_interface_not_overridden::<Plays<'static>>(
            snapshot,
            player.clone(),
            role.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let plays = Plays::new(ObjectType::new(player.into_vertex()), role);

        let initial_annotations = HashSet::from([Annotation::Cardinality(self.get_plays_default_cardinality())]);
        OperationTimeValidation::validate_new_acquired_plays_compatible_with_instances(
            snapshot,
            self,
            thing_manager,
            plays.clone().into_owned(),
            initial_annotations,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_put_edge(snapshot, plays.clone());
        Ok(plays)
    }

    pub(crate) fn unset_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        player: ObjectType<'static>,
        role_type: RoleType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_unset_plays_is_not_inherited(snapshot, player.clone(), role_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let plays_declared = player.get_plays_declared(snapshot, self)?;
        if let Some(plays) = plays_declared.iter().find(|plays| plays.role() == role_type) {
            OperationTimeValidation::validate_no_overriding_capabilities_for_capability_unset(snapshot, plays.clone())
                .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

            OperationTimeValidation::validate_no_instances_to_unset_plays(
                snapshot,
                self,
                thing_manager,
                player.clone(),
                role_type.clone(),
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

            self.unset_plays_unchecked(snapshot, plays.clone())?;
        }
        Ok(())
    }

    pub(crate) fn set_plays_override(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        plays: Plays<'static>,
        overridden: Plays<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_plays_is_inherited(snapshot, plays.player(), overridden.role())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_overridden_interface_type_is_supertype_or_self::<Plays<'static>>(
            snapshot,
            plays.clone(),
            overridden.role(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_capability_override_annotations_compatibility_transitive(
            snapshot,
            self,
            plays.clone(),
            overridden.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_object_type_subtypes_do_not_override_new_overridden_capability(
            snapshot,
            plays.clone(),
            overridden.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_override(
            snapshot,
            self,
            plays.clone(),
            Some(overridden.clone()),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        if plays.role() != overridden.role() {
            OperationTimeValidation::validate_no_instances_to_override_plays(
                snapshot,
                self,
                thing_manager,
                plays.player(),
                overridden.role(),
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        }

        OperationTimeValidation::validate_updated_annotations_compatible_with_plays_and_overriding_plays_instances_on_override(
            snapshot,
            self,
            thing_manager,
            plays.clone(),
            Some(overridden.clone()),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_set_type_edge_overridden(snapshot, plays, overridden);
        Ok(())
    }

    pub(crate) fn unset_plays_override(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        plays: Plays<'static>,
    ) -> Result<(), ConceptWriteError> {
        if let Some(overridden) = &*plays.get_override(snapshot, self)? {
            OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_override(
                snapshot,
                self,
                plays.clone(),
                None, // unset override
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

            OperationTimeValidation::validate_updated_annotations_compatible_with_plays_and_overriding_plays_instances_on_override(
                snapshot,
                self,
                thing_manager,
                plays.clone(),
                None, // unset override
            )
                .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

            let overridden_annotations = overridden
                .get_annotations(snapshot, self)?
                .keys()
                .cloned()
                .map(|annotation| annotation.try_into().unwrap())
                .collect();
            OperationTimeValidation::validate_new_acquired_plays_compatible_with_instances(
                snapshot,
                self,
                thing_manager,
                overridden.clone(),
                overridden_annotations,
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

            TypeWriter::storage_delete_type_edge_overridden(snapshot, plays);
        }
        Ok(())
    }

    pub(crate) fn set_owns_ordering(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
        ordering: Ordering,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_owns_distinct_annotation_ordering(
            snapshot,
            owns.clone(),
            Some(ordering),
            None,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let owns_override_opt = TypeReader::get_capability_override(snapshot, owns.clone())?;
        if let Some(owns_override) = owns_override_opt {
            OperationTimeValidation::validate_owns_override_ordering_match(
                snapshot,
                owns.clone(),
                owns_override.clone(),
                Some(ordering),
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        }

        // TODO: It is an operation blocking call (can only be avoided by unset overrides, which feels too much)
        // unless we allow ordered sub unordered.
        // OperationTimeValidation::validate_overriding_owns_ordering_match(snapshot, owns.clone(), Some(ordering))
        //     .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_no_owns_instances_to_set_ordering(snapshot, thing_manager, owns.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_set_owns_ordering(snapshot, owns, ordering);
        Ok(())
    }

    pub(crate) fn set_role_ordering(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType<'static>,
        ordering: Ordering,
    ) -> Result<(), ConceptWriteError> {
        let relates = self.get_role_type_relates(snapshot, role_type.clone().into_owned())?;
        OperationTimeValidation::validate_relates_distinct_annotation_ordering(
            snapshot,
            relates.clone(),
            Some(ordering),
            None,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let relates_override_opt = TypeReader::get_capability_override(snapshot, relates.clone())?;
        if let Some(relates_override) = relates_override_opt {
            OperationTimeValidation::validate_role_supertype_ordering_match(
                snapshot,
                relates.role(),
                relates_override.role(),
                Some(ordering),
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        }

        OperationTimeValidation::validate_no_role_instances_to_set_ordering(snapshot, thing_manager, relates.role())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_put_type_vertex_property(snapshot, role_type, Some(ordering));
        Ok(())
    }

    pub(crate) fn set_role_type_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType<'static>,
    ) -> Result<(), ConceptWriteError> {
        let relates = TypeReader::get_role_type_relates_declared(snapshot, role_type.clone())?;

        OperationTimeValidation::validate_capability_abstractness::<Relates<'static>>(
            snapshot,
            self,
            relates.relation(),
            role_type.clone(),
            Some(true), // set_abstract
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_annotation_abstract(snapshot, thing_manager, role_type)
    }

    pub(crate) fn set_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Abstract(AnnotationAbstract);

        self.validate_set_annotation_general(snapshot, type_.clone(), annotation.clone())?;

        OperationTimeValidation::validate_type_supertype_abstractness_to_set_abstract_annotation(
            snapshot,
            self,
            type_.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_new_abstract_annotation_compatible_with_type_and_subtypes_instances(
            snapshot,
            self,
            thing_manager,
            type_.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_annotation(snapshot, type_, annotation)
    }

    pub(crate) fn unset_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Abstract;
        self.validate_unset_annotation_general(snapshot, type_.clone(), annotation_category.clone())?;

        OperationTimeValidation::validate_no_abstract_subtypes_to_unset_abstract_annotation(
            snapshot,
            self,
            type_.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.unset_annotation(snapshot, type_, annotation_category)
    }

    pub(crate) fn unset_relation_type_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_no_capabilities_with_abstract_interfaces_to_unset_abstractness::<
            Relates<'static>,
        >(snapshot, self, relation_type.clone())
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.unset_object_type_annotation_abstract(snapshot, relation_type)
    }

    pub(crate) fn unset_object_type_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl ObjectTypeAPI<'static> + KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_no_capabilities_with_abstract_interfaces_to_unset_abstractness::<Owns<'static>>(
            snapshot,
            self,
            type_.clone(),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_no_capabilities_with_abstract_interfaces_to_unset_abstractness::<
            Plays<'static>,
        >(snapshot, self, type_.clone())
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.unset_annotation_abstract(snapshot, type_)
    }

    pub(crate) fn unset_attribute_type_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_no_subtypes_for_type_abstractness_unset(snapshot, type_.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_value_type_compatible_with_abstractness(
            snapshot,
            type_.clone(),
            TypeReader::get_value_type_without_source(snapshot, type_.clone())?,
            Some(false),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.unset_annotation_abstract(snapshot, type_)
    }

    pub(crate) fn set_annotation_independent(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Independent(AnnotationIndependent);

        self.validate_set_annotation_general(snapshot, attribute_type.clone(), annotation.clone())?;

        // It won't validate anything, but placing this call here helps maintain the validation consistency
        OperationTimeValidation::validate_new_annotation_compatible_with_attribute_type_and_subtypes_instances(
            snapshot,
            self,
            thing_manager,
            attribute_type.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_annotation(snapshot, attribute_type, annotation)
    }

    pub(crate) fn unset_annotation_independent(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Independent;
        self.validate_unset_annotation_general(snapshot, type_.clone(), annotation_category.clone())?;
        self.unset_annotation(snapshot, type_, annotation_category)
    }

    pub(crate) fn set_relates_override(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relates: Relates<'static>,
        overridden: Relates<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_relates_is_inherited(snapshot, relates.relation(), overridden.role())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_role_supertype_ordering_match(
            snapshot,
            relates.role(),
            overridden.role(),
            None,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_capability_override_annotations_compatibility_transitive(
            snapshot,
            self,
            relates.clone(),
            overridden.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_object_type_subtypes_do_not_override_new_overridden_capability(
            snapshot,
            relates.clone(),
            overridden.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_interface_change_supertype_does_not_corrupt_capabilities_overrides_transitive::<Plays<'static>>(
            snapshot,
            relates.role(),
            Some(overridden.role()),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_override(
            snapshot,
            self,
            relates.clone(),
            Some(overridden.clone()),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        if relates.role() != overridden.role() {
            OperationTimeValidation::validate_no_instances_to_override_relates(
                snapshot,
                self,
                thing_manager,
                relates.relation(),
                overridden.role(),
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        }

        OperationTimeValidation::validate_updated_annotations_compatible_with_role_type_and_subtypes_instances_on_supertype_change(
            snapshot,
            self,
            thing_manager,
            relates.role(),
            overridden.role(),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_updated_annotations_compatible_with_relates_and_overriding_relates_instances_on_override(
            snapshot,
            self,
            thing_manager,
            relates.clone(),
            Some(overridden.clone()),
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_supertype(snapshot, relates.role(), overridden.role())?;
        TypeWriter::storage_set_type_edge_overridden(snapshot, relates, overridden);
        Ok(())
    }

    pub(crate) fn unset_relates_override(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relates: Relates<'static>,
    ) -> Result<(), ConceptWriteError> {
        let role_type = relates.role();
        if let Some(superrole_type) = role_type.get_supertype(snapshot, self)? {
            OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_override(
                snapshot,
                self,
                relates.clone(),
                None, // unset override
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

            OperationTimeValidation::validate_interface_change_supertype_does_not_corrupt_capabilities_overrides_transitive::<Plays<'static>>(
                snapshot,
                relates.role(),
                None, // new_interface_supertype
            )
                .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

            OperationTimeValidation::validate_updated_annotations_compatible_with_relates_and_overriding_relates_instances_on_override(
                snapshot,
                self,
                thing_manager,
                relates.clone(),
                None, // unset override
            )
                .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

            let overridden = superrole_type.get_relates(snapshot, self)?;
            let overridden_annotations = overridden
                .get_annotations(snapshot, self)?
                .keys()
                .cloned()
                .map(|annotation| annotation.try_into().unwrap())
                .collect();
            OperationTimeValidation::validate_new_acquired_relates_compatible_with_instances(
                snapshot,
                self,
                thing_manager,
                overridden.clone(),
                overridden_annotations,
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

            TypeWriter::storage_delete_type_edge_overridden(snapshot, relates);
            self.unset_supertype(snapshot, role_type)?;
        }
        Ok(())
    }

    pub(crate) fn set_owns_annotation_distinct(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Distinct(AnnotationDistinct);

        self.validate_set_capability_annotation_general(snapshot, owns.clone(), annotation.clone())?;

        OperationTimeValidation::validate_owns_distinct_annotation_ordering(snapshot, owns.clone(), None, Some(true))
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_new_annotation_compatible_with_owns_and_overriding_owns_instances(
            snapshot,
            self,
            thing_manager,
            owns.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_capability_annotation(snapshot, owns, annotation)
    }

    pub(crate) fn set_relates_annotation_distinct(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relates: Relates<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Distinct(AnnotationDistinct);

        self.validate_set_capability_annotation_general(snapshot, relates.clone(), annotation.clone())?;

        OperationTimeValidation::validate_relates_distinct_annotation_ordering(
            snapshot,
            relates.clone(),
            None,
            Some(true),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_new_annotation_compatible_with_relates_and_overriding_relates_instances(
            snapshot,
            self,
            thing_manager,
            relates.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_capability_annotation(snapshot, relates, annotation)
    }

    pub(crate) fn unset_capability_annotation_distinct<CAP: Capability<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        capability: CAP,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Distinct;
        self.validate_unset_capability_annotation_general(snapshot, capability.clone(), annotation_category.clone())?;
        self.unset_capability_annotation(snapshot, capability, annotation_category)
    }

    pub(crate) fn set_owns_annotation_unique(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Unique(AnnotationUnique);

        self.validate_set_capability_annotation_general(snapshot, owns.clone(), annotation.clone())?;

        OperationTimeValidation::validate_owns_value_type_compatible_with_unique_annotation_transitive(
            snapshot,
            owns.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_new_annotation_compatible_with_owns_and_overriding_owns_instances(
            snapshot,
            self,
            thing_manager,
            owns.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_capability_annotation(snapshot, owns, annotation)
    }

    pub(crate) fn unset_owns_annotation_unique(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Unique;
        self.validate_unset_capability_annotation_general(snapshot, owns.clone(), annotation_category.clone())?;
        self.unset_capability_annotation(snapshot, owns, annotation_category)
    }

    pub(crate) fn set_owns_annotation_key(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Key(AnnotationKey);

        self.validate_set_capability_annotation_general(snapshot, owns.clone(), annotation.clone())?;

        OperationTimeValidation::validate_owns_value_type_compatible_with_key_annotation_transitive(
            snapshot,
            owns.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.validate_updated_capability_cardinality(snapshot, owns.clone(), AnnotationKey::CARDINALITY, true)?;

        OperationTimeValidation::validate_new_annotation_compatible_with_owns_and_overriding_owns_instances(
            snapshot,
            self,
            thing_manager,
            owns.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_capability_annotation(snapshot, owns, annotation)
    }

    pub(crate) fn unset_owns_annotation_key(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Key;

        self.validate_unset_capability_annotation_general(snapshot, owns.clone(), annotation_category.clone())?;

        if capability_get_declared_annotation_by_category(snapshot, owns.clone(), annotation_category)?.is_some() {
            let updated_cardinality = self.get_cardinality_inherited_or_default(snapshot, owns.clone(), None)?;

            self.validate_updated_capability_cardinality(snapshot, owns.clone(), updated_cardinality, false)?;

            OperationTimeValidation::validate_new_annotation_compatible_with_owns_and_overriding_owns_instances(
                snapshot,
                self,
                thing_manager,
                owns.clone(),
                Annotation::Cardinality(updated_cardinality),
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        }

        self.unset_capability_annotation(snapshot, owns, annotation_category)
    }

    pub(crate) fn set_owns_annotation_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
        cardinality: AnnotationCardinality,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Cardinality(cardinality.clone());

        self.validate_set_capability_annotation_general(snapshot, owns.clone(), annotation.clone())?;
        self.validate_updated_capability_cardinality(snapshot, owns.clone(), cardinality, false)?;

        OperationTimeValidation::validate_new_annotation_compatible_with_owns_and_overriding_owns_instances(
            snapshot,
            self,
            thing_manager,
            owns.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_capability_annotation(snapshot, owns, annotation)
    }

    pub(crate) fn unset_owns_annotation_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Cardinality;

        self.validate_unset_capability_annotation_general(snapshot, owns.clone(), annotation_category.clone())?;

        if capability_get_declared_annotation_by_category(snapshot, owns.clone(), annotation_category)?.is_some() {
            let updated_cardinality = self.get_cardinality_inherited_or_default(snapshot, owns.clone(), None)?;

            if updated_cardinality != owns.get_cardinality(snapshot, self)? {
                self.validate_updated_capability_cardinality(snapshot, owns.clone(), updated_cardinality, false)?;

                OperationTimeValidation::validate_new_annotation_compatible_with_owns_and_overriding_owns_instances(
                    snapshot,
                    self,
                    thing_manager,
                    owns.clone(),
                    Annotation::Cardinality(updated_cardinality),
                )
                .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
            }
        }

        self.unset_capability_annotation(snapshot, owns, annotation_category)
    }

    pub(crate) fn set_plays_annotation_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        plays: Plays<'static>,
        cardinality: AnnotationCardinality,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Cardinality(cardinality.clone());

        self.validate_set_capability_annotation_general(snapshot, plays.clone(), annotation.clone())?;
        self.validate_updated_capability_cardinality(snapshot, plays.clone(), cardinality, false)?;

        OperationTimeValidation::validate_new_annotation_compatible_with_plays_and_overriding_plays_instances(
            snapshot,
            self,
            thing_manager,
            plays.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_capability_annotation(snapshot, plays, annotation)
    }

    pub(crate) fn unset_plays_annotation_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        plays: Plays<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Cardinality;

        self.validate_unset_capability_annotation_general(snapshot, plays.clone(), annotation_category.clone())?;

        if capability_get_declared_annotation_by_category(snapshot, plays.clone(), annotation_category)?.is_some() {
            let updated_cardinality = self.get_cardinality_inherited_or_default(snapshot, plays.clone(), None)?;

            self.validate_updated_capability_cardinality(snapshot, plays.clone(), updated_cardinality, false)?;

            OperationTimeValidation::validate_new_annotation_compatible_with_plays_and_overriding_plays_instances(
                snapshot,
                self,
                thing_manager,
                plays.clone(),
                Annotation::Cardinality(updated_cardinality),
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        }

        self.unset_capability_annotation(snapshot, plays, annotation_category)
    }

    pub(crate) fn set_relates_annotation_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relates: Relates<'static>,
        cardinality: AnnotationCardinality,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Cardinality(cardinality.clone());

        self.validate_set_capability_annotation_general(snapshot, relates.clone(), annotation.clone())?;
        self.validate_updated_capability_cardinality(snapshot, relates.clone(), cardinality, false)?;

        OperationTimeValidation::validate_new_annotation_compatible_with_relates_and_overriding_relates_instances(
            snapshot,
            self,
            thing_manager,
            relates.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_capability_annotation(snapshot, relates, annotation)
    }

    pub(crate) fn unset_relates_annotation_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relates: Relates<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Cardinality;

        self.validate_unset_capability_annotation_general(snapshot, relates.clone(), annotation_category.clone())?;

        if capability_get_declared_annotation_by_category(snapshot, relates.clone(), annotation_category)?.is_some() {
            let updated_cardinality = self.get_cardinality_inherited_or_default(snapshot, relates.clone(), None)?;

            self.validate_updated_capability_cardinality(snapshot, relates.clone(), updated_cardinality, false)?;

            OperationTimeValidation::validate_new_annotation_compatible_with_relates_and_overriding_relates_instances(
                snapshot,
                self,
                thing_manager,
                relates.clone(),
                Annotation::Cardinality(updated_cardinality),
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        }

        self.unset_capability_annotation(snapshot, relates, annotation_category)
    }

    pub(crate) fn set_annotation_regex(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        regex: AnnotationRegex,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Regex(regex.clone());

        self.validate_set_annotation_general(snapshot, attribute_type.clone(), annotation.clone())?;

        OperationTimeValidation::validate_annotation_regex_compatible_value_type(
            snapshot,
            attribute_type.clone(),
            TypeReader::get_value_type_without_source(snapshot, attribute_type.clone())?,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_regex_arguments(regex.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_annotation_set_only_for_interface_transitive::<Owns<'static>>(
            snapshot,
            attribute_type.clone(),
            AnnotationCategory::Regex,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_type_regex_narrows_inherited_regex(
            snapshot,
            attribute_type.clone(),
            None, // supertype: will be read from storage
            regex.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_subtypes_narrow_regex(snapshot, attribute_type.clone(), regex.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_new_annotation_compatible_with_attribute_type_and_subtypes_instances(
            snapshot,
            self,
            thing_manager,
            attribute_type.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_annotation(snapshot, attribute_type, annotation)
    }

    pub(crate) fn unset_annotation_regex(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Regex;
        self.validate_unset_annotation_general(snapshot, type_.clone(), annotation_category.clone())?;
        self.unset_annotation(snapshot, type_, annotation_category)
    }

    pub(crate) fn set_owns_annotation_regex(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
        regex: AnnotationRegex,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Regex(regex.clone());

        self.validate_set_capability_annotation_general(snapshot, owns.clone(), annotation.clone())?;

        OperationTimeValidation::validate_annotation_regex_compatible_value_type(
            snapshot,
            owns.attribute(),
            TypeReader::get_value_type_without_source(snapshot, owns.attribute())?,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_regex_arguments(regex.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_annotation_set_only_for_capability_transitive(
            snapshot,
            owns.clone(),
            AnnotationCategory::Regex,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_capability_regex_narrows_inherited_regex(
            snapshot,
            owns.clone(),
            None, // overridden_owns: will be read from storage
            regex.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_overriding_capabilities_narrow_regex(snapshot, owns.clone(), regex.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_new_annotation_compatible_with_owns_and_overriding_owns_instances(
            snapshot,
            self,
            thing_manager,
            owns.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_capability_annotation(snapshot, owns, annotation)
    }

    pub(crate) fn unset_owns_annotation_regex(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Regex;
        self.validate_unset_capability_annotation_general(snapshot, owns.clone(), annotation_category.clone())?;
        self.unset_capability_annotation(snapshot, owns, annotation_category)
    }

    pub(crate) fn set_annotation_cascade(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Cascade(AnnotationCascade);

        self.validate_set_annotation_general(snapshot, relation_type.clone(), annotation.clone())?;

        // It won't validate anything, but placing this call here helps maintain the validation consistency
        OperationTimeValidation::validate_new_annotation_compatible_with_relation_type_and_subtypes_instances(
            snapshot,
            self,
            thing_manager,
            relation_type.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_annotation(snapshot, relation_type, annotation)
    }

    pub(crate) fn unset_annotation_cascade(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Cascade;
        self.validate_unset_annotation_general(snapshot, type_.clone(), annotation_category.clone())?;
        self.unset_annotation(snapshot, type_, annotation_category)
    }

    pub(crate) fn set_annotation_range(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        range: AnnotationRange,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Range(range.clone());

        self.validate_set_annotation_general(snapshot, attribute_type.clone(), annotation.clone())?;

        let type_value_type = TypeReader::get_value_type_without_source(snapshot, attribute_type.clone())?;

        OperationTimeValidation::validate_annotation_range_compatible_value_type(
            snapshot,
            attribute_type.clone(),
            type_value_type.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_range_arguments(range.clone(), type_value_type)
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_annotation_set_only_for_interface_transitive::<Owns<'static>>(
            snapshot,
            attribute_type.clone(),
            AnnotationCategory::Range,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_type_range_narrows_inherited_range(
            snapshot,
            attribute_type.clone(),
            None, // supertype: will be read from storage
            range.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_subtypes_narrow_range(snapshot, attribute_type.clone(), range.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // TODO: Maybe for the future: check if compatible with existing VALUES annotation

        OperationTimeValidation::validate_new_annotation_compatible_with_attribute_type_and_subtypes_instances(
            snapshot,
            self,
            thing_manager,
            attribute_type.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_annotation(snapshot, attribute_type, annotation)
    }

    pub(crate) fn unset_annotation_range(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Range;
        self.validate_unset_annotation_general(snapshot, type_.clone(), annotation_category.clone())?;
        self.unset_annotation(snapshot, type_, annotation_category)
    }

    pub(crate) fn set_owns_annotation_range(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
        range: AnnotationRange,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Range(range.clone());

        self.validate_set_capability_annotation_general(snapshot, owns.clone(), annotation.clone())?;

        let attribute_value_type = TypeReader::get_value_type_without_source(snapshot, owns.attribute())?;

        OperationTimeValidation::validate_annotation_range_compatible_value_type(
            snapshot,
            owns.attribute(),
            attribute_value_type.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_range_arguments(range.clone(), attribute_value_type)
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_annotation_set_only_for_capability_transitive(
            snapshot,
            owns.clone(),
            AnnotationCategory::Range,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_capabilities_range_narrows_inherited_range(
            snapshot,
            owns.clone(),
            None, // overridden_owns: will be read from storage
            range.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_overriding_capabilities_narrow_range(snapshot, owns.clone(), range.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // TODO: Maybe for the future: check if compatible with existing VALUES annotation

        OperationTimeValidation::validate_new_annotation_compatible_with_owns_and_overriding_owns_instances(
            snapshot,
            self,
            thing_manager,
            owns.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_capability_annotation(snapshot, owns, annotation)
    }

    pub(crate) fn unset_owns_annotation_range(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Range;
        self.validate_unset_capability_annotation_general(snapshot, owns.clone(), annotation_category.clone())?;
        self.unset_capability_annotation(snapshot, owns, annotation_category)
    }

    pub(crate) fn set_annotation_values(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        values: AnnotationValues,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Values(values.clone());

        self.validate_set_annotation_general(snapshot, attribute_type.clone(), annotation.clone())?;

        let type_value_type = TypeReader::get_value_type_without_source(snapshot, attribute_type.clone())?;

        OperationTimeValidation::validate_annotation_values_compatible_value_type(
            snapshot,
            attribute_type.clone(),
            type_value_type.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_values_arguments(values.clone(), type_value_type)
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_annotation_set_only_for_interface_transitive::<Owns<'static>>(
            snapshot,
            attribute_type.clone(),
            AnnotationCategory::Values,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_type_values_narrows_inherited_values(
            snapshot,
            attribute_type.clone(),
            None, // supertype: will be read from storage
            values.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_subtypes_narrow_values(snapshot, attribute_type.clone(), values.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // TODO: Maybe for the future: check if compatible with existing RANGE annotation

        OperationTimeValidation::validate_new_annotation_compatible_with_attribute_type_and_subtypes_instances(
            snapshot,
            self,
            thing_manager,
            attribute_type.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_annotation(snapshot, attribute_type, annotation)
    }

    pub(crate) fn unset_annotation_values(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Values;
        self.validate_unset_annotation_general(snapshot, type_.clone(), annotation_category.clone())?;
        self.unset_annotation(snapshot, type_, annotation_category)
    }

    pub(crate) fn set_owns_annotation_values(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns<'static>,
        values: AnnotationValues,
    ) -> Result<(), ConceptWriteError> {
        let annotation = Annotation::Values(values.clone());

        self.validate_set_capability_annotation_general(snapshot, owns.clone(), annotation.clone())?;

        let attribute_value_type = TypeReader::get_value_type_without_source(snapshot, owns.attribute())?;

        OperationTimeValidation::validate_annotation_values_compatible_value_type(
            snapshot,
            owns.attribute(),
            attribute_value_type.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_values_arguments(values.clone(), attribute_value_type)
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_annotation_set_only_for_capability_transitive(
            snapshot,
            owns.clone(),
            AnnotationCategory::Values,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_capabilities_values_narrows_inherited_values(
            snapshot,
            owns.clone(),
            None, // overridden_owns: will be read from storage
            values.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_overriding_capabilities_narrow_values(snapshot, owns.clone(), values.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // TODO: Maybe for the future: check if compatible with existing RANGE annotation

        OperationTimeValidation::validate_new_annotation_compatible_with_owns_and_overriding_owns_instances(
            snapshot,
            self,
            thing_manager,
            owns.clone(),
            annotation.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_capability_annotation(snapshot, owns, annotation)
    }

    pub(crate) fn unset_owns_annotation_values(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        let annotation_category = AnnotationCategory::Values;
        self.validate_unset_capability_annotation_general(snapshot, owns.clone(), annotation_category.clone())?;
        self.unset_capability_annotation(snapshot, owns, annotation_category)
    }

    fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
        annotation: Annotation,
    ) -> Result<(), ConceptWriteError> {
        storage_save_annotation!(snapshot, type_, annotation, TypeWriter::storage_put_type_vertex_property);
        Ok(())
    }

    fn set_capability_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        capability: impl Capability<'static>,
        annotation: Annotation,
    ) -> Result<(), ConceptWriteError> {
        storage_save_annotation!(snapshot, capability, annotation, TypeWriter::storage_put_type_edge_property);
        Ok(())
    }

    fn unset_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
        annotation_category: AnnotationCategory,
    ) -> Result<(), ConceptWriteError> {
        storage_delete_annotation!(
            snapshot,
            type_,
            annotation_category,
            TypeReader::get_type_annotations_declared,
            storage_delete_type_vertex_property
        );
        Ok(())
    }

    fn unset_capability_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        capability: impl Capability<'static>,
        annotation_category: AnnotationCategory,
    ) -> Result<(), ConceptWriteError> {
        storage_delete_annotation!(
            snapshot,
            capability,
            annotation_category,
            TypeReader::get_type_edge_annotations_declared,
            storage_delete_type_edge_property
        );
        Ok(())
    }

    fn validate_set_annotation_general(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
        annotation: Annotation,
    ) -> Result<(), ConceptWriteError> {
        let category = annotation.category();

        OperationTimeValidation::validate_declared_annotation_is_compatible_with_declared_annotations(
            snapshot,
            type_.clone(),
            category.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_declared_annotation_is_compatible_with_inherited_annotations(
            snapshot,
            type_.clone(),
            category.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_inherited_annotation_is_compatible_with_declared_annotations_of_subtypes(
            snapshot, category, type_,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        Ok(())
    }

    fn validate_updated_capability_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        capability: impl Capability<'static>,
        cardinality: AnnotationCardinality,
        is_key: bool,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_cardinality_arguments(cardinality)
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        if let Some(override_edge) = TypeReader::get_capability_override(snapshot, capability.clone())? {
            OperationTimeValidation::validate_cardinality_narrows_inherited_cardinality(
                snapshot,
                self,
                capability.clone(),
                override_edge,
                cardinality,
                is_key,
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        }

        OperationTimeValidation::validate_overriding_capabilities_narrow_cardinality(
            snapshot,
            capability.clone(),
            cardinality,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_updated_cardinality_against_inheritance_line(
            snapshot,
            self,
            capability.clone(),
            cardinality,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        Ok(())
    }

    fn validate_set_capability_annotation_general(
        &self,
        snapshot: &mut impl WritableSnapshot,
        capability: impl Capability<'static>,
        annotation: Annotation,
    ) -> Result<(), ConceptWriteError> {
        let category = annotation.category();

        OperationTimeValidation::validate_declared_capability_annotation_is_compatible_with_declared_annotations(
            snapshot,
            capability.clone(),
            category.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_declared_capability_annotation_is_compatible_with_inherited_annotations(
            snapshot,
            capability.clone(),
            category.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_inherited_annotation_is_compatible_with_declared_annotations_of_overriding_capabilities(
            snapshot,
            category,
            capability,
        )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        Ok(())
    }

    fn validate_unset_annotation_general(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
        annotation_category: AnnotationCategory,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_unset_annotation_is_not_inherited(
            snapshot,
            type_.clone(),
            annotation_category,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        Ok(())
    }

    fn validate_unset_capability_annotation_general(
        &self,
        snapshot: &mut impl WritableSnapshot,
        capability: impl Capability<'static>,
        annotation_category: AnnotationCategory,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_unset_edge_annotation_is_not_inherited(
            snapshot,
            capability.clone(),
            annotation_category,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        Ok(())
    }
}
