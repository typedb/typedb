/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use encoding::{
    error::EncodingError,
    graph::{
        definition::{
            definition_key::DefinitionKey, definition_key_generator::DefinitionKeyGenerator, r#struct::StructDefinition,
        },
        type_::{vertex::TypeVertexEncoding, vertex_generator::TypeVertexGenerator, Kind},
    },
    value::{label::Label, value_type::ValueType},
};
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
        constraint::{
            get_abstract_constraint, get_cardinality_constraint, get_cardinality_constraints, get_distinct_constraints,
            get_independent_constraints, get_owns_default_constraints, get_plays_default_constraints,
            get_range_constraints, get_regex_constraints, get_relates_default_constraints, get_unique_constraint,
            get_values_constraints, CapabilityConstraint, Constraint, TypeConstraint,
        },
        entity_type::{EntityType, EntityTypeAnnotation},
        object_type::ObjectType,
        owns::{Owns, OwnsAnnotation},
        plays::{Plays, PlaysAnnotation},
        relates::{Relates, RelatesAnnotation},
        relation_type::{RelationType, RelationTypeAnnotation},
        role_type::{RoleType, RoleTypeAnnotation},
        type_manager::type_reader::TypeReader,
        Capability, KindAPI, ObjectTypeAPI, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
    },
};

pub mod type_cache;
pub mod type_reader;
mod type_writer;
pub mod validation;

// TODO: this should be parametrised into the database options? Would be great to have it be changable at runtime!
pub(crate) const RELATION_INDEX_THRESHOLD: u64 = 8;

#[derive(Debug)]
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
            ) -> Result<Option<$output_type>, Box<ConceptReadError>> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(label))
                } else {
                    TypeReader::get_labelled_type::<$output_type>(snapshot, label)
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
            pub fn $method_name(&self, snapshot: &impl ReadableSnapshot) -> Result<Vec<$type_>, Box<ConceptReadError>> {
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
                &self, snapshot: &impl ReadableSnapshot, type_: $type_
            ) -> Result<Option<$type_>, Box<ConceptReadError>> {
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
                &self, snapshot: &impl ReadableSnapshot, type_: $type_
            ) -> Result<MaybeOwns<'_, Vec<$type_>>, Box<ConceptReadError>> {
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
                &self, snapshot: &impl ReadableSnapshot, type_: $type_
            ) -> Result<MaybeOwns<'_, HashSet<$type_>>, Box<ConceptReadError>> {
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
            // WARN: subtypes currently do NOT include themselves
            pub(crate) fn $method_name(
                &self, snapshot: &impl ReadableSnapshot, type_: $type_
            ) -> Result<MaybeOwns<'_, Vec<$type_>>, Box<ConceptReadError>> {
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
                &self, snapshot: &impl ReadableSnapshot, type_: $type_
            ) -> Result<MaybeOwns<'_, Label<'static>>, Box<ConceptReadError>> {
                if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::Borrowed(cache.$cache_method(type_)))
                } else {
                    Ok(MaybeOwns::Owned(TypeReader::get_label(snapshot, type_)?.ok_or(ConceptReadError::CorruptMissingLabelOfType)?))
                }
            }
        )*
    }
}

macro_rules! get_type_label_arc_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &impl ReadableSnapshot, type_: $type_
            ) -> Result<Arc<Label<'static>>, Box<ConceptReadError>> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(type_))
                } else {
                    Ok(Arc::new(TypeReader::get_label(snapshot, type_)?.ok_or(ConceptReadError::CorruptMissingLabelOfType)?))
                }
            }
        )*
    }
}

macro_rules! get_annotations_declared_methods {
    ($(
        fn $method_name:ident($type_:ident) -> $annotation_type:ident = $reader_method:ident | $cache_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &impl ReadableSnapshot, type_: $type_
            ) -> Result<MaybeOwns<'_, HashSet<$annotation_type>>, Box<ConceptReadError>> {
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

macro_rules! get_annotation_declared_by_category_methods {
    ($(
        $(#[$meta:meta])* fn $method_name:ident($type_:ident) -> $annotation_type:ty;
    )*) => {
        $(
            $(#[$meta])*
            pub(crate) fn $method_name(
                &self, snapshot: &impl ReadableSnapshot, type_: $type_, annotation_category: AnnotationCategory,
            ) -> Result<Option<$annotation_type>, Box<ConceptReadError>> {
                Ok(type_.get_annotations_declared(snapshot, self)?.into_iter().find(|&type_annotation|
                {
                    Annotation::from(type_annotation.clone()).category() == annotation_category
                }).cloned())
            }
        )*
    }
}

macro_rules! get_constraints_methods {
    ($(
        fn $method_name:ident() -> $constraint_type:ident<$type_:ident> = $reader_method:ident | $cache_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &impl ReadableSnapshot, type_: $type_
            ) -> Result<MaybeOwns<'_, HashSet<$constraint_type<$type_>>>, Box<ConceptReadError>> {
                 if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::Borrowed(cache.$cache_method(type_)))
                } else {
                    let constraints = TypeReader::$reader_method(snapshot, type_)?;
                    Ok(MaybeOwns::Owned(constraints))
                }
            }
        )*
    }
}

macro_rules! get_type_capability_constraints_methods {
    ($(
        fn $method_name:ident($type_decl:ident => |$type_:ident| $reader_convert:expr) -> $capability:ident = $reader_method:ident | $cache_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self,
                snapshot: &impl ReadableSnapshot,
                $type_: $type_decl,
                interface_type: <$capability as Capability<'static>>::InterfaceType,
            ) -> Result<MaybeOwns<'_, HashSet<CapabilityConstraint<$capability>>>, Box<ConceptReadError>> {
                 if let Some(cache) = &self.type_cache {
                     Ok(match cache.$cache_method($type_).get(&interface_type) {
                         Some(cached) => MaybeOwns::Borrowed(cached),
                         None => MaybeOwns::Owned(HashSet::new()),
                     })
                } else {
                     let $type_ = $reader_convert;
                    let constraints = TypeReader::$reader_method(snapshot, $type_, interface_type)?;
                    Ok(MaybeOwns::Owned(constraints))
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

macro_rules! get_filtered_constraints_methods {
    ($(
        fn $method_name:ident() -> $constraint_type:ident<$type_:ident> = $get_constraints_method:ident + $filtering_method:path;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &impl ReadableSnapshot, type_: $type_
            ) -> Result<HashSet<$constraint_type<$type_>>, Box<ConceptReadError>> {
                Ok($filtering_method(type_.$get_constraints_method(snapshot, self)?.into_iter().cloned()))
            }
        )*
    }
}

macro_rules! get_type_capability_filtered_constraints_methods {
    ($(
        fn $method_name:ident() -> $capability:ident = $get_constraints_method:ident + $filtering_method:path;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self,
                snapshot: &impl ReadableSnapshot,
                object_type: <$capability as Capability<'static>>::ObjectType,
                interface_type: <$capability as Capability<'static>>::InterfaceType,
            ) -> Result<HashSet<CapabilityConstraint<$capability>>, Box<ConceptReadError>> {
                Ok($filtering_method(object_type.$get_constraints_method(snapshot, self, interface_type)?.into_iter().cloned()))
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

    pub fn definition_key_generator(&self) -> Arc<DefinitionKeyGenerator> {
        self.definition_key_generator.clone()
    }

    pub fn get_struct_definition_key(
        &self,
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<Option<DefinitionKey<'static>>, Box<ConceptReadError>> {
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
    ) -> Result<MaybeOwns<'_, StructDefinition>, Box<ConceptReadError>> {
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
    ) -> Result<Vec<StructFieldIDUInt>, Box<ConceptReadError>> {
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
                            Err(Box::new(ConceptReadError::Encoding {
                                source: EncodingError::IndexingIntoNonStructField {
                                    struct_name: definition.name,
                                    field_path: fields_path.iter().map(|&str| str.to_owned()).collect(),
                                },
                            }))
                        };
                    }
                }
            } else {
                return Err(Box::new(ConceptReadError::Encoding {
                    source: EncodingError::StructFieldUnresolvable {
                        struct_name: definition.name,
                        field_path: fields_path.iter().map(|&str| str.to_owned()).collect(),
                    },
                }));
            }
        }

        Err(Box::new(ConceptReadError::Encoding {
            source: EncodingError::StructPathIncomplete {
                struct_name: definition.name,
                field_path: fields_path.iter().map(|&str| str.to_owned()).collect(),
            },
        }))
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

    get_type_label_arc_methods! {
        fn get_entity_type_label_arc() -> EntityType = get_label_owned;
        fn get_relation_type_label_arc() -> RelationType = get_label_owned;
        fn get_role_type_label_arc() -> RoleType = get_label_owned;
        fn get_attribute_type_label_arc() -> AttributeType = get_label_owned;
    }

    pub fn get_roles_by_name(
        &self,
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<Option<Cow<'_, [RoleType]>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_roles_by_name(name).map(Cow::Borrowed))
        } else {
            let roles = TypeReader::get_roles_by_name(snapshot, name.to_owned())?;
            if roles.is_empty() {
                Ok(None)
            } else {
                Ok(Some(Cow::Owned(roles)))
            }
        }
    }

    pub(crate) fn get_entity_type_owns_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType,
    ) -> Result<MaybeOwns<'_, HashSet<Owns>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_declared(entity_type)))
        } else {
            let owns = TypeReader::get_capabilities_declared(snapshot, entity_type)?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_relation_type_owns_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType,
    ) -> Result<MaybeOwns<'_, HashSet<Owns>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_declared(relation_type)))
        } else {
            let owns = TypeReader::get_capabilities_declared(snapshot, relation_type)?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_attribute_type_owns(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
    ) -> Result<MaybeOwns<'_, HashSet<Owns>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_attribute_type_owns(attribute_type)))
        } else {
            let plays = TypeReader::get_capabilities_for_interface::<Owns>(snapshot, attribute_type)?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_attribute_type_owner_types(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
    ) -> Result<MaybeOwns<'_, HashMap<ObjectType, Owns>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_attribute_type_owner_types(attribute_type)))
        } else {
            let owns = TypeReader::get_object_types_with_capabilities_for_interface::<Owns>(snapshot, attribute_type)?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_relation_type_relates_root(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType,
    ) -> Result<MaybeOwns<'_, HashSet<Relates>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_relation_type_relates_root(relation_type)))
        } else {
            let relates = TypeReader::get_relation_type_relates_root(snapshot, relation_type)?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_relation_type_relates_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType,
    ) -> Result<MaybeOwns<'_, HashSet<Relates>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_relation_type_relates_declared(relation_type)))
        } else {
            let relates = TypeReader::get_capabilities_declared::<Relates>(snapshot, relation_type)?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_relation_type_relates(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType,
    ) -> Result<MaybeOwns<'_, HashSet<Relates>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_relation_type_relates(relation_type)))
        } else {
            let relates = TypeReader::get_capabilities::<Relates>(snapshot, relation_type, false)?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_relation_type_relates_with_specialised(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType,
    ) -> Result<MaybeOwns<'_, HashSet<Relates>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_relation_type_relates_with_specialised(relation_type)))
        } else {
            let relates = TypeReader::get_capabilities::<Relates>(snapshot, relation_type, true)?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_role_type_plays(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType,
    ) -> Result<MaybeOwns<'_, HashSet<Plays>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_role_type_plays(role_type)))
        } else {
            let plays = TypeReader::get_capabilities_for_interface::<Plays>(snapshot, role_type)?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_role_type_player_types(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType,
    ) -> Result<MaybeOwns<'_, HashMap<ObjectType, Plays>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_role_type_player_types(role_type)))
        } else {
            let plays = TypeReader::get_object_types_with_capabilities_for_interface::<Plays>(snapshot, role_type)?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_role_type_ordering(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType,
    ) -> Result<Ordering, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_role_type_ordering(role_type))
        } else {
            Ok(TypeReader::get_type_ordering(snapshot, role_type)?)
        }
    }

    pub(crate) fn get_role_type_relates_root(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType,
    ) -> Result<Relates, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_role_type_relates_root(role_type).clone())
        } else {
            let relates = TypeReader::get_role_type_relates_root(snapshot, role_type)?;
            Ok(relates)
        }
    }

    pub(crate) fn get_role_type_relates(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType,
    ) -> Result<MaybeOwns<'_, HashSet<Relates>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_role_type_relates(role_type)))
        } else {
            let relates = TypeReader::get_capabilities_for_interface::<Relates>(snapshot, role_type)?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_role_type_relation_types(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType,
    ) -> Result<MaybeOwns<'_, HashMap<RelationType, Relates>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_role_type_relation_types(role_type)))
        } else {
            let relates = TypeReader::get_object_types_with_capabilities_for_interface::<Relates>(snapshot, role_type)?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_entity_type_owns(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType,
    ) -> Result<MaybeOwns<'_, HashSet<Owns>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns(entity_type)))
        } else {
            let owns = TypeReader::get_capabilities::<Owns>(snapshot, entity_type.into_owned_object_type(), false)?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_entity_type_owns_with_specialised(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType,
    ) -> Result<MaybeOwns<'_, HashSet<Owns>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_with_specialised(entity_type)))
        } else {
            let owns = TypeReader::get_capabilities::<Owns>(snapshot, entity_type.into_owned_object_type(), true)?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_relation_type_owns(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType,
    ) -> Result<MaybeOwns<'_, HashSet<Owns>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns(relation_type)))
        } else {
            let owns = TypeReader::get_capabilities::<Owns>(snapshot, relation_type.into_owned_object_type(), false)?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_relation_type_owns_with_specialised(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType,
    ) -> Result<MaybeOwns<'_, HashSet<Owns>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_with_specialised(relation_type)))
        } else {
            let owns = TypeReader::get_capabilities::<Owns>(snapshot, relation_type.into_owned_object_type(), true)?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn relation_index_available(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType,
    ) -> Result<bool, Box<ConceptReadError>> {
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
        entity_type: EntityType,
    ) -> Result<MaybeOwns<'this, HashSet<Plays>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_declared(entity_type)))
        } else {
            let plays = TypeReader::get_capabilities_declared(snapshot, entity_type)?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_entity_type_plays(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType,
    ) -> Result<MaybeOwns<'_, HashSet<Plays>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays(entity_type)))
        } else {
            let plays = TypeReader::get_capabilities::<Plays>(snapshot, entity_type.into_owned_object_type(), false)?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_entity_type_plays_with_specialised(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType,
    ) -> Result<MaybeOwns<'_, HashSet<Plays>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_with_specialised(entity_type)))
        } else {
            let plays = TypeReader::get_capabilities::<Plays>(snapshot, entity_type.into_owned_object_type(), true)?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_relation_type_plays_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType,
    ) -> Result<MaybeOwns<'this, HashSet<Plays>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_declared(relation_type)))
        } else {
            let plays = TypeReader::get_capabilities_declared(snapshot, relation_type)?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_relation_type_plays(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType,
    ) -> Result<MaybeOwns<'_, HashSet<Plays>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays(relation_type)))
        } else {
            let plays = TypeReader::get_capabilities::<Plays>(snapshot, relation_type.into_owned_object_type(), false)?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_relation_type_plays_with_specialised(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType,
    ) -> Result<MaybeOwns<'_, HashSet<Plays>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_with_specialised(relation_type)))
        } else {
            let plays = TypeReader::get_capabilities::<Plays>(snapshot, relation_type.into_owned_object_type(), true)?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_attribute_type_value_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
    ) -> Result<Option<(ValueType, AttributeType)>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_attribute_type_value_type(attribute_type).clone())
        } else {
            Ok(TypeReader::get_value_type(snapshot, attribute_type)?)
        }
    }

    pub(crate) fn get_attribute_type_value_type_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType,
    ) -> Result<Option<ValueType>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_attribute_type_value_type_declared(attribute_type).clone())
        } else {
            Ok(TypeReader::get_value_type_declared(snapshot, attribute_type)?)
        }
    }

    pub(crate) fn get_owns_ordering(
        &self,
        snapshot: &impl ReadableSnapshot,
        owns: Owns,
    ) -> Result<Ordering, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_owns_ordering(owns))
        } else {
            Ok(TypeReader::get_capability_ordering(snapshot, owns)?)
        }
    }

    pub(crate) fn get_relates_is_specialising(
        &self,
        snapshot: &impl ReadableSnapshot,
        relates: Relates,
    ) -> Result<bool, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_relates_is_specialising(relates))
        } else {
            Ok(TypeReader::is_relates_specialising(snapshot, relates)?)
        }
    }

    get_annotations_declared_methods! {
        fn get_entity_type_annotations_declared(EntityType) -> EntityTypeAnnotation = get_type_annotations_declared | get_annotations_declared;
        fn get_relation_type_annotations_declared(RelationType) -> RelationTypeAnnotation = get_type_annotations_declared | get_annotations_declared;
        fn get_role_type_annotations_declared(RoleType) -> RoleTypeAnnotation = get_type_annotations_declared | get_annotations_declared;
        fn get_attribute_type_annotations_declared(AttributeType) -> AttributeTypeAnnotation = get_type_annotations_declared | get_annotations_declared;
        fn get_owns_annotations_declared(Owns) -> OwnsAnnotation = get_capability_annotations_declared | get_owns_annotations_declared;
        fn get_plays_annotations_declared(Plays) -> PlaysAnnotation = get_capability_annotations_declared | get_plays_annotations_declared;
        fn get_relates_annotations_declared(Relates) -> RelatesAnnotation = get_capability_annotations_declared | get_relates_annotations_declared;
    }

    get_annotation_declared_by_category_methods! {
        fn get_entity_type_annotation_declared_by_category(EntityType) -> EntityTypeAnnotation;
        fn get_relation_type_annotation_declared_by_category(RelationType) -> RelationTypeAnnotation;
        fn get_attribute_type_annotation_declared_by_category(AttributeType) -> AttributeTypeAnnotation;
        #[expect(unreachable_code, reason = "RoleTypeAnnotation is uninhabited")]
        fn get_role_type_annotation_declared_by_category(RoleType) -> RoleTypeAnnotation;
        fn get_owns_annotation_declared_by_category(Owns) -> OwnsAnnotation;
        fn get_plays_annotation_declared_by_category(Plays) -> PlaysAnnotation;
        fn get_relates_annotation_declared_by_category(Relates) -> RelatesAnnotation;
    }

    get_constraints_methods! {
        fn get_entity_type_constraints() -> TypeConstraint<EntityType> = get_type_constraints | get_constraints;
        fn get_relation_type_constraints() -> TypeConstraint<RelationType> = get_type_constraints | get_constraints;
        fn get_role_type_constraints() -> TypeConstraint<RoleType> = get_type_constraints | get_constraints;
        fn get_attribute_type_constraints() -> TypeConstraint<AttributeType> = get_type_constraints | get_constraints;
        fn get_owns_constraints() -> CapabilityConstraint<Owns> = get_capability_constraints | get_owns_constraints;
        fn get_plays_constraints() -> CapabilityConstraint<Plays> = get_capability_constraints | get_plays_constraints;
        fn get_relates_constraints() -> CapabilityConstraint<Relates> = get_capability_constraints | get_relates_constraints;
    }

    get_type_capability_constraints_methods! {
        fn get_entity_type_owned_attribute_type_constraints(EntityType => |type_| { type_.into_owned_object_type() }) -> Owns = get_type_capability_constraints | get_owned_attribute_type_constraints;
        fn get_relation_type_owned_attribute_type_constraints(RelationType => |type_| { type_.into_owned_object_type() }) -> Owns = get_type_capability_constraints | get_owned_attribute_type_constraints;
        fn get_entity_type_played_role_type_constraints(EntityType => |type_| { type_.into_owned_object_type() }) -> Plays = get_type_capability_constraints | get_played_role_type_constraints;
        fn get_relation_type_played_role_type_constraints(RelationType => |type_| { type_.into_owned_object_type() }) -> Plays = get_type_capability_constraints | get_played_role_type_constraints;
        fn get_relation_type_related_role_type_constraints(RelationType => |type_| { type_ }) -> Relates = get_type_capability_constraints | get_relation_type_related_role_type_constraints;
    }

    get_filtered_constraints_methods! {
        fn get_attribute_type_independent_constraints() -> TypeConstraint<AttributeType> = get_constraints + get_independent_constraints;
        fn get_attribute_type_regex_constraints() -> TypeConstraint<AttributeType> = get_constraints + get_regex_constraints;
        fn get_attribute_type_range_constraints() -> TypeConstraint<AttributeType> = get_constraints + get_range_constraints;
        fn get_attribute_type_values_constraints() -> TypeConstraint<AttributeType> = get_constraints + get_values_constraints;
        fn get_owns_cardinality_constraints() -> CapabilityConstraint<Owns> = get_constraints + get_cardinality_constraints;
        fn get_plays_cardinality_constraints() -> CapabilityConstraint<Plays> = get_constraints + get_cardinality_constraints;
        fn get_relates_cardinality_constraints() -> CapabilityConstraint<Relates> = get_constraints + get_cardinality_constraints;
        fn get_relates_distinct_constraints() -> CapabilityConstraint<Relates> = get_constraints + get_distinct_constraints;
        fn get_owns_distinct_constraints() -> CapabilityConstraint<Owns> = get_constraints + get_distinct_constraints;
        fn get_owns_regex_constraints() -> CapabilityConstraint<Owns> = get_constraints + get_regex_constraints;
        fn get_owns_range_constraints() -> CapabilityConstraint<Owns> = get_constraints + get_range_constraints;
        fn get_owns_values_constraints() -> CapabilityConstraint<Owns> = get_constraints + get_values_constraints;
    }

    get_type_capability_filtered_constraints_methods! {
        fn get_type_owns_cardinality_constraints() -> Owns = get_owned_attribute_type_constraints + get_cardinality_constraints;
        fn get_type_plays_cardinality_constraints() -> Plays = get_played_role_type_constraints + get_cardinality_constraints;
        fn get_type_relates_cardinality_constraints() -> Relates = get_related_role_type_constraints + get_cardinality_constraints;
        fn get_type_owns_distinct_constraints() -> Owns = get_owned_attribute_type_constraints + get_distinct_constraints;
        fn get_type_relates_distinct_constraints() -> Relates = get_related_role_type_constraints + get_distinct_constraints;
        fn get_type_owns_regex_constraints() -> Owns = get_owned_attribute_type_constraints + get_regex_constraints;
        fn get_type_owns_range_constraints() -> Owns = get_owned_attribute_type_constraints + get_range_constraints;
        fn get_type_owns_values_constraints() -> Owns = get_owned_attribute_type_constraints + get_values_constraints;
    }

    pub(crate) fn get_type_abstract_constraint<T: KindAPI<'static>>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<Option<TypeConstraint<T>>, Box<ConceptReadError>> {
        let constraints = type_.get_constraints(snapshot, self)?;
        Ok(get_abstract_constraint(type_.clone(), constraints.iter()))
    }

    pub(crate) fn get_capability_abstract_constraint<CAP: Capability<'static>>(
        &self,
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
    ) -> Result<Option<CapabilityConstraint<CAP>>, Box<ConceptReadError>> {
        let constraints = capability.get_constraints(snapshot, self)?;
        Ok(get_abstract_constraint(capability.clone(), constraints.iter()))
    }

    pub(crate) fn get_type_owns_abstract_constraint(
        &self,
        snapshot: &impl ReadableSnapshot,
        object_type: ObjectType,
        attribute_type: AttributeType,
    ) -> Result<Option<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        let owns_declared = object_type.get_owns_attribute_declared(snapshot, self, attribute_type)?;
        Ok(if let Some(owns_declared) = owns_declared {
            let constraints = object_type.get_owned_attribute_type_constraints(snapshot, self, attribute_type)?;
            get_abstract_constraint(owns_declared, constraints.iter())
        } else {
            None
        })
    }

    pub(crate) fn get_type_plays_abstract_constraint(
        &self,
        snapshot: &impl ReadableSnapshot,
        object_type: ObjectType,
        role_type: RoleType,
    ) -> Result<Option<CapabilityConstraint<Plays>>, Box<ConceptReadError>> {
        let plays_declared = object_type.get_plays_role_declared(snapshot, self, role_type)?;
        Ok(if let Some(plays_declared) = plays_declared {
            let constraints = object_type.get_played_role_type_constraints(snapshot, self, role_type)?;
            get_abstract_constraint(plays_declared, constraints.iter())
        } else {
            None
        })
    }

    pub(crate) fn get_type_relates_abstract_constraint(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType,
        role_type: RoleType,
    ) -> Result<Option<CapabilityConstraint<Relates>>, Box<ConceptReadError>> {
        let relates_declared = relation_type.get_relates_role_declared(snapshot, self, role_type)?;
        Ok(if let Some(relates_declared) = relates_declared {
            let constraints = relation_type.get_related_role_type_constraints(snapshot, self, role_type)?;
            get_abstract_constraint(relates_declared, constraints.iter())
        } else {
            None
        })
    }

    pub(crate) fn get_unique_constraint(
        &self,
        snapshot: &impl ReadableSnapshot,
        owns: Owns,
    ) -> Result<Option<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        Ok(get_unique_constraint(owns.get_constraints(snapshot, self)?.iter()))
    }

    pub(crate) fn get_type_owns_unique_constraint(
        &self,
        snapshot: &impl ReadableSnapshot,
        object_type: ObjectType,
        attribute_type: AttributeType,
    ) -> Result<Option<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        Ok(get_unique_constraint(
            object_type.get_owned_attribute_type_constraints(snapshot, self, attribute_type)?.iter(),
        ))
    }

    pub(crate) fn get_capability_cardinality<CAP: Capability<'static>>(
        &self,
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
    ) -> Result<AnnotationCardinality, Box<ConceptReadError>> {
        self.get_capability_cardinality_constraint(snapshot, capability)?
            .ok_or_else(|| Box::new(ConceptReadError::CorruptMissingMandatoryCardinalityForNonSpecialisingCapability))?
            .description()
            .unwrap_cardinality()
            .map_err(|source| Box::new(ConceptReadError::Constraint { source }))
    }

    pub(crate) fn get_relates_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        relates: Relates,
    ) -> Result<AnnotationCardinality, Box<ConceptReadError>> {
        match relates.is_specialising(snapshot, self)? {
            true => {
                debug_assert!(self.get_capability_cardinality_constraint(snapshot, relates.clone())?.is_none());
                self.get_capability_cardinality(snapshot, relates.role().get_relates_root(snapshot, self)?)
            }
            false => self.get_capability_cardinality(snapshot, relates),
        }
    }

    fn get_capability_cardinality_constraint<CAP: Capability<'static>>(
        &self,
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
    ) -> Result<Option<CapabilityConstraint<CAP>>, Box<ConceptReadError>> {
        let constraints = capability.get_constraints(snapshot, self)?;
        Ok(get_cardinality_constraint(capability.clone(), constraints.iter()))
    }

    pub(crate) fn get_is_key(
        &self,
        snapshot: &impl ReadableSnapshot,
        owns: Owns,
    ) -> Result<bool, Box<ConceptReadError>> {
        match owns.get_ordering(snapshot, self) {
            Ok(_) => {
                Ok(if let Some(constraint) = get_unique_constraint(owns.get_constraints(snapshot, self)?.iter()) {
                    constraint
                        .source()
                        .get_annotations_declared(snapshot, self)?
                        .contains(&OwnsAnnotation::Key(AnnotationKey))
                } else {
                    false
                })
            }
            Err(err) => {
                match *err {
                    // Can be called from validation functions before storing ordering
                    ConceptReadError::OrderingValueMissing => Ok(false),
                    _ => Err(err),
                }
            }
        }
    }

    pub(crate) fn get_independent_attribute_types(
        &self,
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Arc<HashSet<AttributeType>>, Box<ConceptReadError>> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_independent_attribute_types())
        } else {
            let mut independent = HashSet::new();
            for type_ in self.get_attribute_types(snapshot)?.into_iter() {
                if type_.is_independent(snapshot, self)? {
                    independent.insert(type_);
                }
            }
            Ok(Arc::new(independent))
        }
    }
}

impl TypeManager {
    pub fn validate(&self, snapshot: &impl WritableSnapshot) -> Result<(), Vec<ConceptWriteError>> {
        let type_errors = CommitTimeValidation::validate(snapshot, self);
        match type_errors {
            Ok(errors) => {
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errors
                        .into_iter()
                        .map(|error| ConceptWriteError::SchemaValidation { typedb_source: error })
                        .collect())
                }
            }
            Err(error) => Err(vec![ConceptWriteError::ConceptRead { source: error }]),
        }
    }

    pub fn create_struct(
        &self,
        snapshot: &mut impl WritableSnapshot,
        name: String,
    ) -> Result<DefinitionKey<'static>, Box<ConceptWriteError>> {
        OperationTimeValidation::validate_struct_name_uniqueness(snapshot, &name)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

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
    ) -> Result<(), Box<ConceptWriteError>> {
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
        _thing_manager: &ThingManager,
        definition_key: DefinitionKey<'static>,
        field_name: &str,
    ) -> Result<(), Box<ConceptWriteError>> {
        // TODO: Somehow check instances?

        let mut struct_definition = TypeReader::get_struct_definition(snapshot, definition_key.clone())?;
        struct_definition.delete_field(field_name).map_err(|source| ConceptWriteError::Encoding { source })?;

        TypeWriter::storage_insert_struct(snapshot, definition_key.clone(), struct_definition);
        Ok(())
    }

    pub fn delete_struct(
        &self,
        snapshot: &mut impl WritableSnapshot,
        _thing_manager: &ThingManager,
        definition_key: &DefinitionKey<'static>,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_deleted_struct_is_not_used_in_schema(snapshot, definition_key)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        // TODO: Somehow check instances?

        TypeWriter::storage_delete_struct(snapshot, definition_key);
        Ok(())
    }

    pub fn create_entity_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        label: &Label<'_>,
    ) -> Result<EntityType, Box<ConceptWriteError>> {
        OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        let type_vertex = self
            .vertex_generator
            .create_entity_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let entity = EntityType::new(type_vertex);

        TypeWriter::storage_put_label(snapshot, entity, label);
        Ok(entity)
    }

    pub fn create_relation_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        label: &Label<'_>,
    ) -> Result<RelationType, Box<ConceptWriteError>> {
        OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        let type_vertex = self
            .vertex_generator
            .create_relation_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let relation = RelationType::new(type_vertex);

        TypeWriter::storage_put_label(snapshot, relation, label);
        Ok(relation)
    }

    pub(crate) fn create_role_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        label: &Label<'_>,
        relation_type: RelationType,
        ordering: Ordering,
    ) -> Result<RoleType, Box<ConceptWriteError>> {
        OperationTimeValidation::validate_new_role_name_uniqueness(
            snapshot,
            self,
            relation_type.into_owned(),
            &label.clone().into_owned(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        // Capabilities can have default values for annotations (e.g. @card(1..X)),
        // and they can contradict the absence of data for their object types.
        // It is dirty to delete role type right after creating it with this validation,
        // but it allows us to have more consistent and scalable validations.
        let type_vertex = self
            .vertex_generator
            .create_role_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let role_type = RoleType::new(type_vertex);

        TypeWriter::storage_put_label(snapshot, role_type, label);
        TypeWriter::storage_put_type_vertex_property(snapshot, role_type, Some(ordering));

        if let Err(relates_err) = self.set_relates(snapshot, thing_manager, relation_type, role_type, false) {
            TypeWriter::storage_unput_type_vertex_property(snapshot, role_type, Some(ordering));
            TypeWriter::storage_unput_label(snapshot, role_type, label);
            TypeWriter::storage_unput_vertex(snapshot, role_type);
            Err(relates_err)
        } else {
            Ok(role_type)
        }
    }

    fn set_relates(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType,
        role_type: RoleType,
        is_specialising: bool,
    ) -> Result<Relates, Box<ConceptWriteError>> {
        let relates = Relates::new(relation_type, role_type);
        let exists = relation_type.get_relates(snapshot, self)?.contains(&relates);

        if !exists && !is_specialising {
            OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_capabilities(
                snapshot,
                self,
                relates.clone(),
                true, // new relates is set
            )
            .map_err(|typedb_source| Box::new(ConceptWriteError::SchemaValidation { typedb_source }))?;

            let ordering = relates.role().get_ordering(snapshot, self)?;

            OperationTimeValidation::validate_new_acquired_relates_compatible_with_instances(
                snapshot,
                self,
                thing_manager,
                relates.clone().into_owned(),
                get_relates_default_constraints(relates.clone(), ordering, false),
            )
            .map_err(|typedb_source| Box::new(ConceptWriteError::SchemaValidation { typedb_source }))?;
        }

        TypeWriter::storage_put_edge(snapshot, relates.clone());
        Ok(relates)
    }

    pub fn create_attribute_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        label: &Label<'_>,
    ) -> Result<AttributeType, Box<ConceptWriteError>> {
        OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        let type_vertex = self
            .vertex_generator
            .create_attribute_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let attribute_type = AttributeType::new(type_vertex);

        TypeWriter::storage_put_label(snapshot, attribute_type, label);
        Ok(attribute_type)
    }

    fn delete_object_type_capabilities_unchecked(
        &self,
        snapshot: &mut impl WritableSnapshot,
        object_type: ObjectType,
    ) -> Result<(), Box<ConceptWriteError>> {
        for owns in TypeReader::get_capabilities_declared::<Owns>(snapshot, object_type)? {
            self.unset_owns_unchecked(snapshot, owns)?;
        }

        for plays in TypeReader::get_capabilities_declared::<Plays>(snapshot, object_type)? {
            self.unset_plays_unchecked(snapshot, plays)?;
        }

        Ok(())
    }

    fn validate_delete_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        type_: impl KindAPI<'static>,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_no_subtypes_for_type_deletion(snapshot, self, type_.clone())
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_no_instances_to_delete(snapshot, self, thing_manager, type_)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        Ok(())
    }

    pub(crate) fn delete_entity_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        entity_type: EntityType,
    ) -> Result<(), Box<ConceptWriteError>> {
        self.validate_delete_type(snapshot, thing_manager, entity_type)?;

        self.delete_object_type_capabilities_unchecked(snapshot, entity_type.into_owned_object_type())?;
        self.delete_type(snapshot, entity_type)
    }

    pub(crate) fn delete_relation_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType,
    ) -> Result<(), Box<ConceptWriteError>> {
        self.validate_delete_type(snapshot, thing_manager, relation_type)?;

        let declared_relates = TypeReader::get_capabilities_declared::<Relates>(snapshot, relation_type)?;
        for relates in declared_relates.iter() {
            self.delete_role_type(snapshot, thing_manager, relates.role())?;
        }
        self.delete_object_type_capabilities_unchecked(snapshot, relation_type.into_owned_object_type())?;
        self.delete_type(snapshot, relation_type)
    }

    pub(crate) fn delete_attribute_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>> {
        self.validate_delete_type(snapshot, thing_manager, attribute_type)?;

        for owns in TypeReader::get_capabilities_for_interface::<Owns>(snapshot, attribute_type)? {
            self.unset_owns_unchecked(snapshot, owns)?;
        }

        self.unset_value_type_unchecked(snapshot, attribute_type)?;
        self.delete_type(snapshot, attribute_type)
    }

    pub(crate) fn delete_role_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        self.validate_delete_type(snapshot, thing_manager, role_type)?;

        let relates = role_type.get_relates_root(snapshot, self)?;

        OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_capabilities(
            snapshot,
            self,
            relates.clone(),
            false,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        // Should be the same as in validate_delete_type, but leaving for consistency
        OperationTimeValidation::validate_no_corrupted_instances_to_unset_relates(
            snapshot,
            self,
            thing_manager,
            relates.relation(),
            role_type,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.delete_relates_and_its_role_type_unchecked(snapshot, relates)
    }

    fn delete_relates_and_its_role_type_unchecked(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relates: Relates,
    ) -> Result<(), Box<ConceptWriteError>> {
        for plays in TypeReader::get_capabilities_for_interface::<Plays>(snapshot, relates.role())? {
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
    ) -> Result<(), Box<ConceptWriteError>> {
        for annotation in TypeReader::get_type_annotations_declared(snapshot, type_.clone())? {
            self.unset_type_annotation(snapshot, type_.clone(), annotation.into().category())?;
        }
        self.unset_supertype(snapshot, type_.clone())?;
        TypeWriter::storage_delete_label(snapshot, type_.clone());
        TypeWriter::storage_delete_vertex(snapshot, type_);
        Ok(())
    }

    fn unset_owns_unchecked(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns,
    ) -> Result<(), Box<ConceptWriteError>> {
        TypeWriter::storage_delete_type_edge_property::<Ordering>(snapshot, owns);
        self.unset_capability(snapshot, owns)
    }

    fn unset_plays_unchecked(
        &self,
        snapshot: &mut impl WritableSnapshot,
        plays: Plays,
    ) -> Result<(), Box<ConceptWriteError>> {
        self.unset_capability(snapshot, plays)
    }

    fn unset_relates_unchecked(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relates: Relates,
    ) -> Result<(), Box<ConceptWriteError>> {
        self.unset_capability(snapshot, relates)
    }

    fn unset_capability(
        &self,
        snapshot: &mut impl WritableSnapshot,
        capability: impl Capability<'static>,
    ) -> Result<(), Box<ConceptWriteError>> {
        for annotation in TypeReader::get_capability_annotations_declared(snapshot, capability.clone())? {
            self.unset_capability_annotation(snapshot, capability.clone(), annotation.clone().into().category())?;
        }
        TypeWriter::storage_delete_edge(snapshot, capability.clone());
        Ok(())
    }

    pub(crate) fn set_label<T: KindAPI<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: T,
        label: &Label<'_>,
    ) -> Result<(), Box<ConceptWriteError>> {
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, type_.clone()).is_ok());

        match T::KIND {
            Kind::Entity | Kind::Attribute => {
                OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
                    .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
            }
            Kind::Relation => unreachable!("Use set_relation_type_label instead"),
            Kind::Role => unreachable!("Use set_name instead"),
        }

        TypeWriter::storage_delete_label(snapshot, type_.clone());
        TypeWriter::storage_put_label(snapshot, type_, label);
        Ok(())
    }

    pub(crate) fn set_relation_type_label(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation_type: RelationType,
        label: &Label<'_>,
    ) -> Result<(), Box<ConceptWriteError>> {
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, relation_type).is_ok());

        OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        TypeWriter::storage_delete_label(snapshot, relation_type);
        TypeWriter::storage_put_label(snapshot, relation_type, label);

        let relates = self.get_relation_type_relates_declared(snapshot, relation_type)?;
        for relate in &relates {
            self.set_role_type_scope(snapshot, relate.role(), label.clone().name().as_str())?;
        }

        Ok(())
    }

    pub(crate) fn set_role_type_name(
        &self,
        snapshot: &mut impl WritableSnapshot,
        role_type: RoleType,
        name: &str,
    ) -> Result<(), Box<ConceptWriteError>> {
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, role_type).is_ok());

        let old_label = TypeReader::get_label(snapshot, role_type)?.unwrap();
        debug_assert!(old_label.scope().is_some());

        let new_label = Label::build_scoped(name, old_label.scope().unwrap().as_str());
        let relation_type = role_type.get_relates_root(snapshot, self)?.relation();

        OperationTimeValidation::validate_new_role_name_uniqueness(
            snapshot,
            self,
            relation_type,
            &new_label.clone().into_owned(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        TypeWriter::storage_delete_label(snapshot, role_type);
        TypeWriter::storage_put_label(snapshot, role_type, &new_label);
        Ok(())
    }

    fn set_role_type_scope(
        &self,
        snapshot: &mut impl WritableSnapshot,
        role_type: RoleType,
        scope: &str,
    ) -> Result<(), Box<ConceptWriteError>> {
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, role_type).is_ok());

        let old_label = TypeReader::get_label(snapshot, role_type)?.unwrap();
        debug_assert!(old_label.scope().is_some());

        let new_label = Label::build_scoped(old_label.name().as_str(), scope);

        TypeWriter::storage_delete_label(snapshot, role_type);
        TypeWriter::storage_put_label(snapshot, role_type, &new_label);
        Ok(())
    }

    pub(crate) fn set_value_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        value_type: ValueType,
    ) -> Result<(), Box<ConceptWriteError>> {
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, attribute_type).is_ok());

        OperationTimeValidation::validate_value_type_compatible_with_inherited_value_type(
            snapshot,
            self,
            attribute_type,
            value_type.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_subtypes_value_types_compatible_with_new_value_type(
            snapshot,
            self,
            attribute_type,
            value_type.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_attribute_type_value_type_compatible_with_annotations_transitive(
            snapshot,
            self,
            attribute_type,
            Some(value_type.clone()),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_value_type_compatible_with_all_owns_annotations_transitive(
            snapshot,
            self,
            attribute_type,
            Some(value_type.clone()),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        match attribute_type.get_value_type_without_source(snapshot, self)? {
            Some(existing_value_type) if value_type != existing_value_type => {
                OperationTimeValidation::validate_no_instances_to_change_value_type(
                    snapshot,
                    self,
                    thing_manager,
                    attribute_type,
                )
                .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
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
        attribute_type: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_value_type_can_be_unset(snapshot, self, thing_manager, attribute_type)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.unset_value_type_unchecked(snapshot, attribute_type)
    }

    fn unset_value_type_unchecked(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute_type: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>> {
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
    ) -> Result<(), Box<ConceptWriteError>> {
        debug_assert! {
            OperationTimeValidation::validate_type_exists(snapshot, subtype.clone()).is_ok() &&
                OperationTimeValidation::validate_type_exists(snapshot, supertype.clone()).is_ok()
        };

        OperationTimeValidation::validate_sub_does_not_create_cycle(snapshot, self, subtype.clone(), supertype.clone())
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_type_supertype_abstractness_to_change_supertype(
            snapshot,
            self,
            subtype.clone(),
            supertype.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_type_declared_constraints_narrowing_of_supertype_constraints(
            snapshot,
            self,
            subtype.clone(),
            supertype.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.unset_supertype(snapshot, subtype.clone())?;
        TypeWriter::storage_put_supertype(snapshot, subtype.clone(), supertype.clone());
        Ok(())
    }

    fn unset_supertype<T: TypeAPI<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        subtype: T,
    ) -> Result<(), Box<ConceptWriteError>> {
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, subtype.clone()).is_ok());
        TypeWriter::storage_may_delete_supertype(snapshot, subtype.clone())?;
        Ok(())
    }

    pub(crate) fn set_attribute_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: AttributeType,
        supertype: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_value_type_is_compatible_with_new_supertypes_value_type_transitive(
            snapshot,
            self,
            thing_manager,
            subtype,
            Some(supertype),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_attribute_type_supertype_is_abstract(snapshot, self, supertype)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_new_attribute_type_supertype_owns_ordering(
            snapshot, self, subtype, supertype,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_attribute_type_does_not_lose_instances_with_independent_constraint_with_new_supertype(
            snapshot,
            self,
            thing_manager,
            subtype,
            Some(supertype),
        )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_updated_constraints_compatible_with_attribute_type_and_sub_instances_on_supertype_change(
            snapshot,
            self,
            thing_manager,
            subtype,
            supertype,
        )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_affected_constraints_compatible_with_owns_instances_on_attribute_supertype_change(
            snapshot,
            self,
            thing_manager,
            subtype,
            supertype,
        )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_supertype(snapshot, subtype, supertype)
    }

    pub(crate) fn unset_attribute_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_value_type_is_compatible_with_new_supertypes_value_type_transitive(
            snapshot,
            self,
            thing_manager,
            subtype,
            None, // supertype
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_attribute_type_does_not_lose_instances_with_independent_constraint_with_new_supertype(
            snapshot,
            self,
            thing_manager,
            subtype,
            None, // supertype
        )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_interface_type_supertype::<
            Owns,
        >(
            snapshot,
            self,
            subtype,
            None, // supertype is unset
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_affected_constraints_compatible_with_owns_instances_on_attribute_supertype_unset(
            snapshot,
            self,
            thing_manager,
            subtype,
        )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.unset_supertype(snapshot, subtype)
    }

    pub(crate) fn set_object_type_supertype<T: ObjectTypeAPI<'static> + KindAPI<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: T,
        supertype: T,
    ) -> Result<(), Box<ConceptWriteError>> {
        let object_subtype = subtype.clone().into_owned_object_type();
        let object_supertype = supertype.clone().into_owned_object_type();

        OperationTimeValidation::validate_lost_owns_do_not_cause_lost_instances_while_changing_supertype(
            snapshot,
            self,
            thing_manager,
            object_subtype,
            Some(object_supertype),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_updated_constraints_compatible_with_owns_instances_on_object_supertype_change(
            snapshot,
            self,
            thing_manager,
            object_subtype,
            object_supertype,
        )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_lost_plays_do_not_cause_lost_instances_while_changing_supertype(
            snapshot,
            self,
            thing_manager,
            object_subtype,
            Some(object_supertype),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_updated_constraints_compatible_with_plays_instances_on_object_supertype_change(
            snapshot,
            self,
            thing_manager,
            object_subtype,
            object_supertype,
        )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_supertype(snapshot, subtype, supertype)
    }

    pub(crate) fn unset_object_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: ObjectType,
    ) -> Result<(), Box<ConceptWriteError>> {
        let object_subtype = subtype.into_owned_object_type();

        OperationTimeValidation::validate_lost_owns_do_not_cause_lost_instances_while_changing_supertype(
            snapshot,
            self,
            thing_manager,
            object_subtype,
            None, // supertype
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_lost_plays_do_not_cause_lost_instances_while_changing_supertype(
            snapshot,
            self,
            thing_manager,
            object_subtype,
            None, // supertype
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.unset_supertype(snapshot, subtype)
    }

    pub(crate) fn set_entity_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: EntityType,
        supertype: EntityType,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_updated_constraints_compatible_with_entity_type_and_sub_instances_on_supertype_change(
            snapshot,
            self,
            thing_manager,
            subtype,
            supertype,
        )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_object_type_supertype(snapshot, thing_manager, subtype, supertype)
    }

    pub(crate) fn unset_entity_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: EntityType,
    ) -> Result<(), Box<ConceptWriteError>> {
        self.unset_object_type_supertype(snapshot, thing_manager, subtype.into_owned_object_type())
    }

    pub(crate) fn set_relation_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: RelationType,
        supertype: RelationType,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_role_names_compatible_with_new_relation_supertype_transitive(
            snapshot, self, subtype, supertype,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_updated_constraints_compatible_with_relation_type_and_sub_instances_on_supertype_change(
            snapshot,
            self,
            thing_manager,
            subtype,
            supertype,
        )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_relates_specialises_compatible_with_new_supertype_transitive(
            snapshot,
            self,
            subtype,
            Some(supertype),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        // TODO: Cascade constraint does not exist. Revisit it after cascade returns.
        // OperationTimeValidation::validate_relation_type_does_not_acquire_cascade_constraint_to_lose_instances_with_new_supertype(
        //     snapshot,
        //     self,
        //     thing_manager,
        //     subtype.clone(),
        //     supertype.clone(),
        // )
        //     .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_lost_relates_do_not_cause_lost_instances_while_changing_supertype(
            snapshot,
            self,
            thing_manager,
            subtype,
            Some(supertype),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_updated_constraints_compatible_with_relates_instances_on_relation_supertype_change(
            snapshot,
            self,
            thing_manager,
            subtype,
            supertype,
        )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_object_type_supertype(snapshot, thing_manager, subtype, supertype)
    }

    pub(crate) fn unset_relation_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: RelationType,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_relates_specialises_compatible_with_new_supertype_transitive(
            snapshot, self, subtype, None, // supertype
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_lost_relates_do_not_cause_lost_instances_while_changing_supertype(
            snapshot,
            self,
            thing_manager,
            subtype,
            None, // supertype
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.unset_object_type_supertype(snapshot, thing_manager, subtype.into_owned_object_type())
    }

    pub(crate) fn set_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owner: ObjectType,
        attribute: AttributeType,
        ordering: Ordering,
    ) -> Result<(), Box<ConceptWriteError>> {
        // TODO: When we introduce abstract owns, take care of extra validation of having
        // abstract - concrete - abstract - concrete - abstract for owner - subowner - subsubowner - ...
        let owns = Owns::new(ObjectType::new(owner.into_vertex()), attribute);
        let exists = owner.get_owns(snapshot, self)?.contains(&owns);

        if exists {
            OperationTimeValidation::validate_set_owns_does_not_conflict_with_same_existing_owns_ordering(
                snapshot, self, owns, ordering,
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
        } else {
            OperationTimeValidation::validate_updated_owns_does_not_conflict_with_any_sibling_owns_ordering(
                snapshot, self, owns, ordering,
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

            OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_capabilities(
                snapshot, self, owns, true,
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

            OperationTimeValidation::validate_new_acquired_owns_compatible_with_instances(
                snapshot,
                self,
                thing_manager,
                owns.into_owned(),
                get_owns_default_constraints(owns, ordering),
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
        }

        TypeWriter::storage_put_edge(snapshot, owns);
        TypeWriter::storage_put_type_edge_property(snapshot, owns, Some(ordering));
        Ok(())
    }

    pub(crate) fn unset_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owner: ObjectType,
        attribute_type: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_unset_owns_is_not_inherited(snapshot, self, owner, attribute_type)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        if let Some(owns) = owner.get_owns_attribute_declared(snapshot, self, attribute_type)? {
            OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_capabilities(
                snapshot, self, owns, false,
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

            OperationTimeValidation::validate_no_corrupted_instances_to_unset_owns(
                snapshot,
                self,
                thing_manager,
                owner,
                attribute_type,
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

            self.unset_owns_unchecked(snapshot, owns)?;
        }
        Ok(())
    }

    pub(crate) fn set_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        player: ObjectType,
        role: RoleType,
    ) -> Result<Plays, Box<ConceptWriteError>> {
        // TODO: When we introduce abstract plays, take care of extra validation of having
        // abstract - concrete - abstract - concrete - abstract for owner - subowner - subsubowner - ...
        let plays = Plays::new(ObjectType::new(player.into_vertex()), role);
        let exists = player.get_plays(snapshot, self)?.contains(&plays);

        if !exists {
            OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_capabilities(
                snapshot,
                self,
                plays.clone(),
                true,
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

            OperationTimeValidation::validate_new_acquired_plays_compatible_with_instances(
                snapshot,
                self,
                thing_manager,
                plays.clone().into_owned(),
                get_plays_default_constraints(plays.clone()),
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
        }

        TypeWriter::storage_put_edge(snapshot, plays.clone());
        Ok(plays)
    }

    pub(crate) fn unset_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        player: ObjectType,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_unset_plays_is_not_inherited(snapshot, self, player, role_type)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        if let Some(plays) = player.get_plays_role_declared(snapshot, self, role_type)? {
            OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_capabilities(
                snapshot,
                self,
                plays.clone(),
                false,
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

            OperationTimeValidation::validate_no_corrupted_instances_to_unset_plays(
                snapshot,
                self,
                thing_manager,
                player,
                role_type,
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

            self.unset_plays_unchecked(snapshot, plays.clone())?;
        }
        Ok(())
    }

    pub(crate) fn set_owns_ordering(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns,
        ordering: Ordering,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_updated_owns_does_not_conflict_with_any_sibling_owns_ordering(
            snapshot, self, owns, ordering,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_owns_distinct_constraint_ordering(snapshot, self, owns, Some(ordering), None)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_no_owns_instances_to_set_ordering(snapshot, self, thing_manager, owns)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        TypeWriter::storage_set_owns_ordering(snapshot, owns, ordering);
        Ok(())
    }

    pub(crate) fn set_role_ordering(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
        ordering: Ordering,
    ) -> Result<(), Box<ConceptWriteError>> {
        if let Some(role_supertype) = role_type.get_supertype(snapshot, self)? {
            OperationTimeValidation::validate_role_supertype_ordering_match(
                snapshot,
                self,
                role_type,
                role_supertype,
                Some(ordering),
                None, // read supertype ordering from storage
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
        }

        for role_subtype in role_type.get_subtypes(snapshot, self)?.into_iter() {
            OperationTimeValidation::validate_role_supertype_ordering_match(
                snapshot,
                self,
                *role_subtype,
                role_type,
                None, // read subtype ordering from storage
                Some(ordering),
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
        }

        for relates in self.get_role_type_relates(snapshot, role_type.into_owned())?.into_iter() {
            OperationTimeValidation::validate_relates_distinct_constraint_ordering(
                snapshot,
                self,
                relates.clone(),
                Some(ordering),
                None,
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
        }

        OperationTimeValidation::validate_no_role_instances_to_set_ordering(snapshot, self, thing_manager, role_type)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        TypeWriter::storage_put_type_vertex_property(snapshot, role_type, Some(ordering));
        Ok(())
    }

    pub(crate) fn set_entity_type_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        entity_type: EntityType,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Abstract(AnnotationAbstract);

        self.validate_set_type_annotation_general(snapshot, entity_type, annotation.clone())?;

        OperationTimeValidation::validate_type_supertype_abstractness_to_set_abstract_annotation(
            snapshot,
            self,
            entity_type,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_entity_type_and_sub_instances(
            snapshot,
            self,
            thing_manager,
            entity_type,
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_type_annotation(snapshot, entity_type, annotation)
    }

    pub(crate) fn set_relation_type_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Abstract(AnnotationAbstract);

        self.validate_set_type_annotation_general(snapshot, relation_type, annotation.clone())?;

        OperationTimeValidation::validate_type_supertype_abstractness_to_set_abstract_annotation(
            snapshot,
            self,
            relation_type,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_relation_type_and_sub_instances(
            snapshot,
            self,
            thing_manager,
            relation_type,
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_type_annotation(snapshot, relation_type, annotation)
    }

    pub(crate) fn set_attribute_type_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Abstract(AnnotationAbstract);

        self.validate_set_type_annotation_general(snapshot, attribute_type, annotation.clone())?;

        OperationTimeValidation::validate_type_supertype_abstractness_to_set_abstract_annotation(
            snapshot,
            self,
            attribute_type,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_attribute_type_and_sub_instances(
            snapshot,
            self,
            thing_manager,
            attribute_type,
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_type_annotation(snapshot, attribute_type, annotation)
    }

    pub(crate) fn unset_entity_type_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        entity_type: EntityType,
    ) -> Result<(), Box<ConceptWriteError>> {
        self.unset_type_annotation_abstract(snapshot, entity_type)
    }

    pub(crate) fn unset_relation_type_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation_type: RelationType,
    ) -> Result<(), Box<ConceptWriteError>> {
        self.unset_type_annotation_abstract(snapshot, relation_type)
    }

    pub(crate) fn unset_attribute_type_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute_type: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_no_attribute_subtypes_to_unset_abstractness(snapshot, self, attribute_type)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_value_type_compatible_with_abstractness(
            snapshot,
            self,
            attribute_type,
            attribute_type.get_value_type_without_source(snapshot, self)?,
            Some(false),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.unset_type_annotation_abstract(snapshot, attribute_type)
    }

    fn unset_type_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Abstract;

        OperationTimeValidation::validate_no_abstract_subtypes_to_unset_abstract_annotation(
            snapshot,
            self,
            type_.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.unset_type_annotation(snapshot, type_, annotation_category)
    }

    pub(crate) fn set_relates_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relates: Relates,
        is_manual: bool,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Abstract(AnnotationAbstract);

        if is_manual {
            OperationTimeValidation::validate_relates_is_not_specialising_to_manage_annotations(
                snapshot,
                self,
                relates.clone(),
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
        }

        self.validate_set_capability_annotation_general(snapshot, relates.clone(), annotation.clone())?;

        if !relates.is_specialising(snapshot, self)? {
            OperationTimeValidation::validate_type_supertype_abstractness_to_set_abstract_annotation(
                snapshot,
                self,
                relates.role(),
            )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
        }

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_relates_instances(
            snapshot,
            self,
            thing_manager,
            relates.clone(),
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_capability_annotation(snapshot, relates, annotation)
    }

    pub(crate) fn unset_relates_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relates: Relates,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Abstract;

        OperationTimeValidation::validate_relates_is_not_specialising_to_manage_annotations(
            snapshot,
            self,
            relates.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_no_abstract_subtypes_to_unset_abstract_annotation(
            snapshot,
            self,
            relates.role(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_relates_is_not_specialising_to_unset_abstract_annotation(
            snapshot,
            self,
            relates.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.unset_capability_annotation(snapshot, relates, annotation_category)
    }

    pub(crate) fn set_annotation_independent(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Independent(AnnotationIndependent);

        self.validate_set_type_annotation_general(snapshot, attribute_type, annotation.clone())?;

        // It won't validate anything, but placing this call here helps maintain the validation consistency
        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_attribute_type_and_sub_instances(
            snapshot,
            self,
            thing_manager,
            attribute_type,
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_type_annotation(snapshot, attribute_type, annotation)
    }

    pub(crate) fn unset_annotation_independent(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Independent;
        self.unset_type_annotation(snapshot, type_, annotation_category)
    }

    pub(crate) fn set_relates_specialise(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relates: Relates,
        specialised: Relates,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_relates_is_inherited(snapshot, self, relates.relation(), specialised.role())
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_specialised_relates_is_not_specialising(
            snapshot,
            self,
            relates.relation(),
            specialised.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        let old_supertype = relates.role().get_supertype(snapshot, self)?;
        self.set_role_type_supertype(snapshot, thing_manager, relates.role(), specialised.role())?;

        let specialising_relates =
            self.set_relates(snapshot, thing_manager, relates.relation(), specialised.role(), true)?;
        self.set_relates_annotation_abstract(snapshot, thing_manager, specialising_relates, false)?;

        if let Some(old_supertype) = old_supertype {
            self.unset_specialising_relates_when_unset_specialise_if_no_subtypes(
                snapshot,
                relates.relation(),
                old_supertype,
            )?;
        }

        Ok(())
    }

    fn unset_specialising_relates_when_unset_specialise_if_no_subtypes(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation_type: RelationType,
        specialised_role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        if specialised_role_type.get_subtypes(snapshot, self)?.len() == 0 {
            let specialising_relates = relation_type
                .get_relates_role_name_declared(
                    snapshot,
                    self,
                    specialised_role_type.get_label(snapshot, self)?.name.as_str(),
                )?
                .ok_or_else(|| Box::new(ConceptReadError::CorruptMissingMandatorySpecialisingRelatesForRole))?;
            self.unset_relates_unchecked(snapshot, specialising_relates)?;
        }
        Ok(())
    }

    fn set_role_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        subtype: RoleType,
        supertype: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_role_supertype_ordering_match(
            snapshot, self, subtype, supertype, None, // read ordering from storage
            None, // read ordering from storage
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_updated_constraints_compatible_with_role_type_and_sub_instances_on_supertype_change(
            snapshot,
            self,
            thing_manager,
            subtype,
            supertype,
        )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_affected_constraints_compatible_with_relates_instances_on_role_supertype_change(
            snapshot,
            self,
            thing_manager,
            subtype,
            supertype,
        )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_affected_constraints_compatible_with_plays_instances_on_role_supertype_change(
            snapshot,
            self,
            thing_manager,
            subtype,
            supertype,
        )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_supertype(snapshot, subtype, supertype)
    }

    pub(crate) fn unset_relates_specialise(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relates: Relates,
    ) -> Result<(), Box<ConceptWriteError>> {
        let role_type = relates.role();
        let relation_type = relates.relation();
        if let Some(supertype) = role_type.get_supertype(snapshot, self)? {
            self.unset_role_type_supertype(snapshot, thing_manager, role_type)?;
            self.unset_specialising_relates_when_unset_specialise_if_no_subtypes(snapshot, relation_type, supertype)?;
        }
        Ok(())
    }

    fn unset_role_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_interface_type_supertype::<
            Relates,
        >(
            snapshot, self, role_type, None, // supertype is unset
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_interface_type_supertype::<
            Plays,
        >(
            snapshot,
            self,
            role_type,
            None, // supertype is unset
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_affected_constraints_compatible_with_relates_instances_on_role_supertype_unset(
            snapshot,
            self,
            thing_manager,
            role_type,
        )
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_affected_constraints_compatible_with_plays_instances_on_role_supertype_unset(
            snapshot,
            self,
            thing_manager,
            role_type,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.unset_supertype(snapshot, role_type)
    }

    pub(crate) fn set_owns_annotation_distinct(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Distinct(AnnotationDistinct);

        self.validate_set_capability_annotation_general(snapshot, owns, annotation.clone())?;

        OperationTimeValidation::validate_owns_distinct_constraint_ordering(snapshot, self, owns, None, Some(true))
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_owns_instances(
            snapshot,
            self,
            thing_manager,
            owns,
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_capability_annotation(snapshot, owns, annotation)
    }

    pub(crate) fn set_relates_annotation_distinct(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relates: Relates,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Distinct(AnnotationDistinct);

        OperationTimeValidation::validate_relates_is_not_specialising_to_manage_annotations(
            snapshot,
            self,
            relates.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.validate_set_capability_annotation_general(snapshot, relates.clone(), annotation.clone())?;

        OperationTimeValidation::validate_relates_distinct_constraint_ordering(
            snapshot,
            self,
            relates.clone(),
            None,
            Some(true),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_relates_instances(
            snapshot,
            self,
            thing_manager,
            relates.clone(),
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_capability_annotation(snapshot, relates, annotation)
    }

    pub(crate) fn unset_owns_annotation_distinct(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Distinct;
        self.unset_capability_annotation(snapshot, owns, annotation_category)
    }

    pub(crate) fn unset_relates_annotation_distinct(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relates: Relates,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Distinct;

        OperationTimeValidation::validate_relates_is_not_specialising_to_manage_annotations(
            snapshot,
            self,
            relates.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.unset_capability_annotation(snapshot, relates, annotation_category)
    }

    pub(crate) fn set_owns_annotation_unique(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Unique(AnnotationUnique);

        self.validate_set_capability_annotation_general(snapshot, owns, annotation.clone())?;

        OperationTimeValidation::validate_owns_value_type_compatible_with_unique_annotation(
            snapshot,
            self,
            owns,
            owns.attribute().get_value_type_without_source(snapshot, self)?,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_owns_instances(
            snapshot,
            self,
            thing_manager,
            owns,
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_capability_annotation(snapshot, owns, annotation)
    }

    pub(crate) fn unset_owns_annotation_unique(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Unique;
        self.unset_capability_annotation(snapshot, owns, annotation_category)
    }

    pub(crate) fn set_owns_annotation_key(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Key(AnnotationKey);

        self.validate_set_capability_annotation_general(snapshot, owns, annotation.clone())?;

        OperationTimeValidation::validate_owns_value_type_compatible_with_key_annotation(
            snapshot,
            self,
            owns,
            owns.attribute().get_value_type_without_source(snapshot, self)?,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.validate_updated_capability_cardinality_against_schema(snapshot, owns, AnnotationKey::CARDINALITY)?;

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_owns_instances(
            snapshot,
            self,
            thing_manager,
            owns,
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_capability_annotation(snapshot, owns, annotation)
    }

    pub(crate) fn unset_owns_annotation_key(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Key;

        if self.get_owns_annotation_declared_by_category(snapshot, owns, annotation_category)?.is_some() {
            let updated_cardinality = Owns::get_default_cardinality(owns.get_ordering(snapshot, self)?);

            if updated_cardinality != owns.get_cardinality(snapshot, self)? {
                self.validate_updated_capability_cardinality_against_schema(snapshot, owns, updated_cardinality)?;

                OperationTimeValidation::validate_new_annotation_constraints_compatible_with_owns_instances(
                    snapshot,
                    self,
                    thing_manager,
                    owns,
                    Annotation::Cardinality(updated_cardinality),
                )
                .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
            }
        }

        self.unset_capability_annotation(snapshot, owns, annotation_category)
    }

    pub(crate) fn set_owns_annotation_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns,
        cardinality: AnnotationCardinality,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Cardinality(cardinality);

        self.validate_set_capability_annotation_general(snapshot, owns, annotation.clone())?;
        self.validate_updated_capability_cardinality_against_schema(snapshot, owns, cardinality)?;

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_owns_instances(
            snapshot,
            self,
            thing_manager,
            owns,
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_capability_annotation(snapshot, owns, annotation)
    }

    pub(crate) fn unset_owns_annotation_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Cardinality;

        if self.get_owns_annotation_declared_by_category(snapshot, owns, annotation_category)?.is_some() {
            let updated_cardinality = Owns::get_default_cardinality(owns.get_ordering(snapshot, self)?);

            if updated_cardinality != owns.get_cardinality(snapshot, self)? {
                self.validate_updated_capability_cardinality_against_schema(snapshot, owns, updated_cardinality)?;

                OperationTimeValidation::validate_new_annotation_constraints_compatible_with_owns_instances(
                    snapshot,
                    self,
                    thing_manager,
                    owns,
                    Annotation::Cardinality(updated_cardinality),
                )
                .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
            }
        }

        self.unset_capability_annotation(snapshot, owns, annotation_category)
    }

    pub(crate) fn set_plays_annotation_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        plays: Plays,
        cardinality: AnnotationCardinality,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Cardinality(cardinality);

        self.validate_set_capability_annotation_general(snapshot, plays.clone(), annotation.clone())?;
        self.validate_updated_capability_cardinality_against_schema(snapshot, plays.clone(), cardinality)?;

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_plays_instances(
            snapshot,
            self,
            thing_manager,
            plays.clone(),
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_capability_annotation(snapshot, plays, annotation)
    }

    pub(crate) fn unset_plays_annotation_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        plays: Plays,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Cardinality;

        if self.get_plays_annotation_declared_by_category(snapshot, plays.clone(), annotation_category)?.is_some() {
            let updated_cardinality = Plays::get_default_cardinality();

            if updated_cardinality != plays.get_cardinality(snapshot, self)? {
                self.validate_updated_capability_cardinality_against_schema(
                    snapshot,
                    plays.clone(),
                    updated_cardinality,
                )?;

                OperationTimeValidation::validate_new_annotation_constraints_compatible_with_plays_instances(
                    snapshot,
                    self,
                    thing_manager,
                    plays.clone(),
                    Annotation::Cardinality(updated_cardinality),
                )
                .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
            }
        }

        self.unset_capability_annotation(snapshot, plays, annotation_category)
    }

    pub(crate) fn set_relates_annotation_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relates: Relates,
        cardinality: AnnotationCardinality,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Cardinality(cardinality);

        OperationTimeValidation::validate_relates_is_not_specialising_to_manage_annotations(
            snapshot,
            self,
            relates.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.validate_set_capability_annotation_general(snapshot, relates.clone(), annotation.clone())?;
        self.validate_updated_capability_cardinality_against_schema(snapshot, relates.clone(), cardinality)?;

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_relates_instances(
            snapshot,
            self,
            thing_manager,
            relates.clone(),
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_capability_annotation(snapshot, relates, annotation)
    }

    pub(crate) fn unset_relates_annotation_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        relates: Relates,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Cardinality;

        OperationTimeValidation::validate_relates_is_not_specialising_to_manage_annotations(
            snapshot,
            self,
            relates.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        if self.get_relates_annotation_declared_by_category(snapshot, relates.clone(), annotation_category)?.is_some() {
            let updated_cardinality =
                Relates::get_default_cardinality_for_non_specialising(relates.role().get_ordering(snapshot, self)?);

            if updated_cardinality != relates.get_cardinality(snapshot, self)? {
                self.validate_updated_capability_cardinality_against_schema(
                    snapshot,
                    relates.clone(),
                    updated_cardinality,
                )?;

                OperationTimeValidation::validate_new_annotation_constraints_compatible_with_relates_instances(
                    snapshot,
                    self,
                    thing_manager,
                    relates.clone(),
                    Annotation::Cardinality(updated_cardinality),
                )
                .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
            }
        }

        self.unset_capability_annotation(snapshot, relates, annotation_category)
    }

    pub(crate) fn set_annotation_regex(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        regex: AnnotationRegex,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Regex(regex.clone());

        self.validate_set_type_annotation_general(snapshot, attribute_type, annotation.clone())?;

        OperationTimeValidation::validate_annotation_regex_compatible_value_type(
            snapshot,
            self,
            attribute_type,
            attribute_type.get_value_type_without_source(snapshot, self)?,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_regex_arguments(regex.clone())
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_constraints_on_capabilities_narrow_regex_on_interface_type_transitive(
            snapshot,
            self,
            attribute_type,
            regex.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_type_regex_narrows_supertype_constraints(
            snapshot,
            self,
            attribute_type,
            regex.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_subtypes_narrow_regex(snapshot, self, attribute_type, regex.clone())
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_attribute_type_and_sub_instances(
            snapshot,
            self,
            thing_manager,
            attribute_type,
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_type_annotation(snapshot, attribute_type, annotation)
    }

    pub(crate) fn unset_annotation_regex(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Regex;
        self.unset_type_annotation(snapshot, type_, annotation_category)
    }

    pub(crate) fn set_owns_annotation_regex(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns,
        regex: AnnotationRegex,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Regex(regex.clone());

        self.validate_set_capability_annotation_general(snapshot, owns, annotation.clone())?;

        OperationTimeValidation::validate_annotation_regex_compatible_value_type(
            snapshot,
            self,
            owns.attribute(),
            owns.attribute().get_value_type_without_source(snapshot, self)?,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_regex_arguments(regex.clone())
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_capability_regex_constraint_narrows_interface_type_constraints(
            snapshot,
            self,
            owns,
            regex.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_owns_instances(
            snapshot,
            self,
            thing_manager,
            owns,
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_capability_annotation(snapshot, owns, annotation)
    }

    pub(crate) fn unset_owns_annotation_regex(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Regex;
        self.unset_capability_annotation(snapshot, owns, annotation_category)
    }

    pub(crate) fn set_annotation_cascade(
        &self,
        _snapshot: &mut impl WritableSnapshot,
        _thing_manager: &ThingManager,
        _relation_type: RelationType,
    ) -> Result<(), Box<ConceptWriteError>> {
        unimplemented!("Cascade is temporarily turned off");
        // let annotation = Annotation::Cascade(AnnotationCascade);
        //
        // self.validate_set_type_annotation_general(snapshot, relation_type.clone(), annotation.clone())?;
        //
        // // It won't validate anything, but placing this call here helps maintain the validation consistency
        // OperationTimeValidation::validate_new_annotation_constraints_compatible_with_relation_type_and_sub_instances(
        //     snapshot,
        //     self,
        //     thing_manager,
        //     relation_type.clone(),
        //     annotation.clone(),
        // )
        // .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;
        //
        // self.set_type_annotation(snapshot, relation_type, annotation)
    }

    pub(crate) fn unset_annotation_cascade(
        &self,
        _snapshot: &mut impl WritableSnapshot,
        _type_: RelationType,
    ) -> Result<(), Box<ConceptWriteError>> {
        unimplemented!("Cascade is temporarily turned off");
        // let annotation_category = AnnotationCategory::Cascade;
        // self.unset_type_annotation(snapshot, type_, annotation_category)
    }

    pub(crate) fn set_annotation_range(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        range: AnnotationRange,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Range(range.clone());

        self.validate_set_type_annotation_general(snapshot, attribute_type, annotation.clone())?;

        let type_value_type = attribute_type.get_value_type_without_source(snapshot, self)?;

        OperationTimeValidation::validate_annotation_range_compatible_value_type(
            snapshot,
            self,
            attribute_type,
            type_value_type.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_range_arguments(range.clone(), type_value_type)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_constraints_on_capabilities_narrow_range_on_interface_type_transitive(
            snapshot,
            self,
            attribute_type,
            range.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_type_range_narrows_supertype_constraints(
            snapshot,
            self,
            attribute_type,
            range.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_subtypes_narrow_range(snapshot, self, attribute_type, range.clone())
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        // TODO: Maybe for the future: check if compatible with existing VALUES annotation

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_attribute_type_and_sub_instances(
            snapshot,
            self,
            thing_manager,
            attribute_type,
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_type_annotation(snapshot, attribute_type, annotation)
    }

    pub(crate) fn unset_annotation_range(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Range;
        self.unset_type_annotation(snapshot, type_, annotation_category)
    }

    pub(crate) fn set_owns_annotation_range(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns,
        range: AnnotationRange,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Range(range.clone());

        self.validate_set_capability_annotation_general(snapshot, owns, annotation.clone())?;

        let attribute_value_type = owns.attribute().get_value_type_without_source(snapshot, self)?;

        OperationTimeValidation::validate_annotation_range_compatible_value_type(
            snapshot,
            self,
            owns.attribute(),
            attribute_value_type.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_range_arguments(range.clone(), attribute_value_type)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_capability_range_constraint_narrows_interface_type_constraints(
            snapshot,
            self,
            owns,
            range.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        // TODO: Maybe for the future: check if compatible with existing VALUES annotation

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_owns_instances(
            snapshot,
            self,
            thing_manager,
            owns,
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_capability_annotation(snapshot, owns, annotation)
    }

    pub(crate) fn unset_owns_annotation_range(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Range;
        self.unset_capability_annotation(snapshot, owns, annotation_category)
    }

    pub(crate) fn set_annotation_values(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        values: AnnotationValues,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Values(values.clone());

        self.validate_set_type_annotation_general(snapshot, attribute_type, annotation.clone())?;

        let type_value_type = attribute_type.get_value_type_without_source(snapshot, self)?;

        OperationTimeValidation::validate_annotation_values_compatible_value_type(
            snapshot,
            self,
            attribute_type,
            type_value_type.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_values_arguments(values.clone(), type_value_type)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_constraints_on_capabilities_narrow_values_on_interface_type_transitive(
            snapshot,
            self,
            attribute_type,
            values.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_type_values_narrows_supertype_constraints(
            snapshot,
            self,
            attribute_type,
            values.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_subtypes_narrow_values(snapshot, self, attribute_type, values.clone())
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        // TODO: Maybe for the future: check if compatible with existing RANGE annotation

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_attribute_type_and_sub_instances(
            snapshot,
            self,
            thing_manager,
            attribute_type,
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_type_annotation(snapshot, attribute_type, annotation)
    }

    pub(crate) fn unset_annotation_values(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Values;
        self.unset_type_annotation(snapshot, type_, annotation_category)
    }

    pub(crate) fn set_owns_annotation_values(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        owns: Owns,
        values: AnnotationValues,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation = Annotation::Values(values.clone());

        self.validate_set_capability_annotation_general(snapshot, owns, annotation.clone())?;

        let attribute_value_type = owns.attribute().get_value_type_without_source(snapshot, self)?;

        OperationTimeValidation::validate_annotation_values_compatible_value_type(
            snapshot,
            self,
            owns.attribute(),
            attribute_value_type.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_values_arguments(values.clone(), attribute_value_type)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_capability_values_constraint_narrows_interface_type_constraints(
            snapshot,
            self,
            owns,
            values.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        // TODO: Maybe for the future: check if compatible with existing RANGE annotation

        OperationTimeValidation::validate_new_annotation_constraints_compatible_with_owns_instances(
            snapshot,
            self,
            thing_manager,
            owns,
            annotation.clone(),
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        self.set_capability_annotation(snapshot, owns, annotation)
    }

    pub(crate) fn unset_owns_annotation_values(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns,
    ) -> Result<(), Box<ConceptWriteError>> {
        let annotation_category = AnnotationCategory::Values;
        self.unset_capability_annotation(snapshot, owns, annotation_category)
    }

    fn set_type_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
        annotation: Annotation,
    ) -> Result<(), Box<ConceptWriteError>> {
        storage_save_annotation!(snapshot, type_, annotation, TypeWriter::storage_put_type_vertex_property);
        Ok(())
    }

    fn set_capability_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        capability: impl Capability<'static>,
        annotation: Annotation,
    ) -> Result<(), Box<ConceptWriteError>> {
        storage_save_annotation!(snapshot, capability, annotation, TypeWriter::storage_put_type_edge_property);
        Ok(())
    }

    fn unset_type_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
        annotation_category: AnnotationCategory,
    ) -> Result<(), Box<ConceptWriteError>> {
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
    ) -> Result<(), Box<ConceptWriteError>> {
        storage_delete_annotation!(
            snapshot,
            capability,
            annotation_category,
            TypeReader::get_capability_annotations_declared,
            storage_delete_type_edge_property
        );
        Ok(())
    }

    fn validate_set_type_annotation_general(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
        annotation: Annotation,
    ) -> Result<(), Box<ConceptWriteError>> {
        let category = annotation.category();

        OperationTimeValidation::validate_declared_type_annotation_is_compatible_with_declared_annotations(
            snapshot,
            self,
            type_.clone(),
            category,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        Ok(())
    }

    fn validate_updated_capability_cardinality_against_schema(
        &self,
        snapshot: &mut impl WritableSnapshot,
        capability: impl Capability<'static>,
        cardinality: AnnotationCardinality,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_cardinality_arguments(cardinality)
            .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        OperationTimeValidation::validate_cardinality_of_inheritance_line_with_updated_cardinality(
            snapshot,
            self,
            capability.clone(),
            cardinality,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        Ok(())
    }

    fn validate_set_capability_annotation_general(
        &self,
        snapshot: &mut impl WritableSnapshot,
        capability: impl Capability<'static>,
        annotation: Annotation,
    ) -> Result<(), Box<ConceptWriteError>> {
        let category = annotation.category();

        OperationTimeValidation::validate_declared_capability_annotation_is_compatible_with_declared_annotations(
            snapshot,
            self,
            capability.clone(),
            category,
        )
        .map_err(|typedb_source| ConceptWriteError::SchemaValidation { typedb_source })?;

        Ok(())
    }
}
