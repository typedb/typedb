/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::Arc,
};

use encoding::{
    error::EncodingError,
    graph::{
        definition::{
            definition_key::DefinitionKey,
            definition_key_generator::DefinitionKeyGenerator,
            r#struct::{StructDefinition, StructDefinitionField},
        },
        type_::{
            edge::TypeEdgeEncoding,
            property::{TypeEdgePropertyEncoding, TypeVertexPropertyEncoding},
            vertex::TypeVertexEncoding,
            vertex_generator::TypeVertexGenerator,
            Kind,
        },
    },
    value::{label::Label, value_type::ValueType},
};
use primitive::maybe_owns::MaybeOwns;
use resource::constants::encoding::StructFieldIDUInt;
use storage::{
    durability_client::DurabilityClient,
    snapshot::{CommittableSnapshot, ReadableSnapshot, WritableSnapshot},
    MVCCStorage,
};
use type_cache::TypeCache;
use type_writer::TypeWriter;
use validation::{commit_time_validation::CommitTimeValidation, operation_time_validation::OperationTimeValidation};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    type_::{
        annotation::{
            Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCascade, AnnotationCategory,
            AnnotationDistinct, AnnotationIndependent, AnnotationKey, AnnotationRange, AnnotationRegex,
            AnnotationUnique, AnnotationValues,
        },
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        entity_type::{EntityType, EntityTypeAnnotation},
        object_type::{with_object_type, ObjectType},
        owns::{Owns, OwnsAnnotation},
        plays::{Plays, PlaysAnnotation},
        relates::{Relates, RelatesAnnotation},
        relation_type::{RelationType, RelationTypeAnnotation},
        role_type::{RoleType, RoleTypeAnnotation},
        type_manager::{type_reader::TypeReader, validation::SchemaValidationError},
        Capability, KindAPI, ObjectTypeAPI, Ordering, PlayerAPI, TypeAPI,
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

impl TypeManager {
    pub fn initialise_types<D: DurabilityClient>(
        storage: Arc<MVCCStorage<D>>,
        definition_key_generator: Arc<DefinitionKeyGenerator>,
        vertex_generator: Arc<TypeVertexGenerator>,
    ) -> Result<(), ConceptWriteError> {
        let mut snapshot = storage.clone().open_snapshot_write();
        {
            let type_manager = TypeManager::new(definition_key_generator, vertex_generator.clone(), None);
            let root_entity = type_manager.create_entity_type(&mut snapshot, &Kind::Entity.root_label(), true)?;
            type_manager.set_annotation_unconditional::<AnnotationAbstract>(&mut snapshot, root_entity, None)?;
            let root_relation = type_manager.create_relation_type(&mut snapshot, &Kind::Relation.root_label(), true)?;
            type_manager.set_annotation_unconditional::<AnnotationAbstract>(
                &mut snapshot,
                root_relation.clone(),
                None,
            )?;
            let root_role = type_manager.create_role_type_unconditional(
                &mut snapshot,
                &Kind::Role.root_label(),
                root_relation,
                true,
                Ordering::Unordered,
            )?;
            type_manager.set_annotation_unconditional::<AnnotationAbstract>(&mut snapshot, root_role, None)?;
            let root_attribute =
                type_manager.create_attribute_type(&mut snapshot, &Kind::Attribute.root_label(), true)?;
            type_manager.set_annotation_unconditional::<AnnotationAbstract>(&mut snapshot, root_attribute, None)?;
        }
        // TODO: pass error up
        snapshot.commit().unwrap();
        Ok(())
    }
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

macro_rules! get_supertypes_methods {
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
                    let supertypes = TypeReader::get_supertypes(snapshot, type_)?;
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

macro_rules! get_type_is_root_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &impl ReadableSnapshot, type_: $type_<'static>
            ) -> Result<bool, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(type_))
                } else {
                    let type_label = TypeReader::get_label(snapshot, type_)?.unwrap();
                    Ok(TypeReader::check_type_is_root(&type_label, $type_::ROOT_KIND))
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
        Ok(self.get_relation_type_relates(snapshot, relation)?.iter().find_map(|(_role, relates)| {
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
        fields_path: &Vec<&str>,
        definition: StructDefinition,
    ) -> Result<Vec<StructFieldIDUInt>, ConceptReadError> {
        let mut resolved: Vec<StructFieldIDUInt> = Vec::with_capacity(fields_path.len());
        let maybe_owns_definition = MaybeOwns::Borrowed(&definition);
        let mut at = maybe_owns_definition;
        for (i, f) in fields_path.iter().enumerate() {
            let field_idx_opt = at.field_names.get(*f);
            if let Some(field_idx) = field_idx_opt {
                resolved.push(*field_idx);
                let next_def: &StructDefinitionField = at.fields.get(&field_idx).unwrap();
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
                                    field_path: fields_path.clone().into_iter().map(|str| str.to_owned()).collect(),
                                },
                            })
                        };
                    }
                }
            } else {
                return Err(ConceptReadError::Encoding {
                    source: EncodingError::StructFieldUnresolvable {
                        struct_name: definition.name,
                        field_path: fields_path.clone().into_iter().map(|str| str.to_owned()).collect(),
                    },
                });
            }
        }

        Err(ConceptReadError::Encoding {
            source: EncodingError::StructPathIncomplete {
                struct_name: definition.name,
                field_path: fields_path.clone().into_iter().map(|str| str.to_owned()).collect(),
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
    ) -> Result<MaybeOwns<'_, HashMap<RoleType<'static>, Relates<'static>>>, ConceptReadError> {
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

    pub(crate) fn get_relates_for_role_type_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType<'static>,
    ) -> Result<MaybeOwns<'_, Relates<'static>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_role_type_relates_declared(role_type.clone())))
        } else {
            let relates = TypeReader::get_role_type_relates_declared(snapshot, role_type.clone())?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_relates_for_role_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<RelationType<'static>, Relates<'static>>>, ConceptReadError> {
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
    ) -> Result<MaybeOwns<'_, HashMap<AttributeType<'static>, Owns<'static>>>, ConceptReadError> {
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
    ) -> Result<MaybeOwns<'_, HashMap<AttributeType<'static>, Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns(relation_type)))
        } else {
            let owns = TypeReader::get_capabilities::<Owns<'static>>(snapshot, relation_type.into_owned_object_type())?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_entity_type_overridden_owns(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<AttributeType<'static>, Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_overridden_owns(entity_type)))
        } else {
            let owns =
                TypeReader::get_overridden_interfaces::<Owns<'static>>(snapshot, entity_type.into_owned_object_type())?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_relation_type_overridden_owns(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<AttributeType<'static>, Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_overridden_owns(relation_type)))
        } else {
            let owns = TypeReader::get_overridden_interfaces::<Owns<'static>>(
                snapshot,
                relation_type.into_owned_object_type(),
            )?;
            Ok(MaybeOwns::Owned(owns))
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
        for (_, relates) in relates.iter() {
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
    ) -> Result<MaybeOwns<'_, HashMap<RoleType<'static>, Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays(entity_type)))
        } else {
            let plays = TypeReader::get_capabilities::<Plays<'static>>(snapshot, entity_type.into_owned_object_type())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_entity_type_overridden_plays(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<RoleType<'static>, Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_overridden_plays(entity_type)))
        } else {
            let plays = TypeReader::get_overridden_interfaces::<Plays<'static>>(
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
    ) -> Result<MaybeOwns<'_, HashMap<RoleType<'static>, Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays(relation_type)))
        } else {
            let plays =
                TypeReader::get_capabilities::<Plays<'static>>(snapshot, relation_type.into_owned_object_type())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_relation_type_overridden_plays(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<RoleType<'static>, Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_overridden_plays(relation_type)))
        } else {
            let plays = TypeReader::get_overridden_interfaces::<Plays<'static>>(
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

    pub(crate) fn get_owns_overridden(
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

    pub(crate) fn get_relates_overridden(
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

    pub(crate) fn get_owns_annotations_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
    ) -> Result<MaybeOwns<'this, HashSet<OwnsAnnotation>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_annotations_declared(owns)))
        } else {
            let annotations: HashSet<OwnsAnnotation> = TypeReader::get_type_edge_annotations_declared(snapshot, owns)?
                .into_iter()
                .map(|annotation| OwnsAnnotation::from(annotation))
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
            let annotations: HashMap<OwnsAnnotation, Owns<'static>> =
                TypeReader::get_type_edge_annotations(snapshot, owns)?
                    .into_iter()
                    .map(|(annotation, owns)| (OwnsAnnotation::from(annotation), owns))
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
            let annotations: HashSet<PlaysAnnotation> =
                TypeReader::get_type_edge_annotations_declared(snapshot, plays)?
                    .into_iter()
                    .map(|annotation| PlaysAnnotation::from(annotation))
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
            let annotations: HashMap<PlaysAnnotation, Plays<'static>> =
                TypeReader::get_type_edge_annotations(snapshot, plays)?
                    .into_iter()
                    .map(|(annotation, plays)| (PlaysAnnotation::from(annotation), plays))
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
            let annotations: HashSet<RelatesAnnotation> =
                TypeReader::get_type_edge_annotations_declared(snapshot, relates)?
                    .into_iter()
                    .map(|annotation| RelatesAnnotation::from(annotation))
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
            let annotations: HashMap<RelatesAnnotation, Relates<'static>> =
                TypeReader::get_type_edge_annotations(snapshot, relates)?
                    .into_iter()
                    .map(|(annotation, relates)| (RelatesAnnotation::from(annotation), relates))
                    .collect();
            Ok(MaybeOwns::Owned(annotations))
        }
    }

    pub fn get_cardinality<'a, CAP: Capability<'a>>(
        &self,
        snapshot: &impl ReadableSnapshot,
        interface_impl: CAP,
    ) -> Result<AnnotationCardinality, ConceptReadError> {
        let annotations = interface_impl.get_annotations(snapshot, self)?;
        let card = annotations
            .iter()
            .filter_map(|(annotation, _)| match annotation.clone().into() {
                // Cardinality and Key cannot be set together
                Annotation::Cardinality(card) => Some(card),
                Annotation::Key(_) => Some(AnnotationKey::CARDINALITY),
                _ => None,
            })
            .next()
            .unwrap_or(interface_impl.get_default_cardinality(snapshot, self)?);
        Ok(card)
    }

    // Specialised APIs to simplify architectures

    pub(crate) fn get_independent_attribute_types(
        &self,
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Arc<HashSet<AttributeType<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_independent_attribute_types())
        } else {
            let root_type = self.get_attribute_type(snapshot, &AttributeType::ROOT_KIND.root_label())?.unwrap();
            let mut independent = HashSet::new();
            for type_ in self.get_attribute_type_subtypes_transitive(snapshot, root_type)?.into_iter() {
                if type_.is_independent(snapshot, self)? {
                    independent.insert(type_.clone());
                }
            }
            Ok(Arc::new(independent))
        }
    }
}

// TODO: Remove this set of comments
// DO: Do all validation, and call TypeWriter methods.
//  All validation must occur before any writes.  All writes must be done through TypeWriter
//      (If this feels like unnecessary indirection, feel free to refactor. I just need structure)
//  Avoid cross-calling methods if it violates the above.
impl TypeManager {
    pub fn finalise(self, snapshot: &impl WritableSnapshot) -> Result<(), Vec<ConceptWriteError>> {
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
        field_name: String,
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
        definition_key: &DefinitionKey<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_deleted_struct_is_not_used(snapshot, definition_key)
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_delete_struct(snapshot, definition_key);
        Ok(())
    }

    pub fn create_entity_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        label: &Label<'_>,
        is_root: bool,
    ) -> Result<EntityType<'static>, ConceptWriteError> {
        OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let type_vertex = self
            .vertex_generator
            .create_entity_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let entity = EntityType::new(type_vertex);

        TypeWriter::storage_put_label(snapshot, entity.clone(), label);
        if !is_root {
            TypeWriter::storage_put_supertype(
                snapshot,
                entity.clone(),
                self.get_entity_type(snapshot, &Kind::Entity.root_label()).unwrap().unwrap(),
            );
        }
        Ok(entity)
    }

    pub fn create_relation_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        label: &Label<'_>,
        is_root: bool,
    ) -> Result<RelationType<'static>, ConceptWriteError> {
        OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let type_vertex = self
            .vertex_generator
            .create_relation_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let relation = RelationType::new(type_vertex);

        TypeWriter::storage_put_label(snapshot, relation.clone(), label);
        if !is_root {
            TypeWriter::storage_put_supertype(
                snapshot,
                relation.clone(),
                self.get_relation_type(snapshot, &Kind::Relation.root_label()).unwrap().unwrap(),
            );
        }
        Ok(relation)
    }

    pub(crate) fn create_role_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        label: &Label<'_>,
        relation_type: RelationType<'static>,
        ordering: Ordering,
    ) -> Result<RoleType<'static>, ConceptWriteError> {
        OperationTimeValidation::validate_can_modify_type(snapshot, relation_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_new_role_name_uniqueness(
            snapshot,
            relation_type.clone().into_owned(),
            &label.clone().into_owned(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.create_role_type_unconditional(snapshot, label, relation_type, false, ordering)
    }

    fn create_role_type_unconditional(
        &self,
        snapshot: &mut impl WritableSnapshot,
        label: &Label<'_>,
        relation_type: RelationType<'static>,
        is_root: bool,
        ordering: Ordering,
    ) -> Result<RoleType<'static>, ConceptWriteError> {
        let type_vertex = self
            .vertex_generator
            .create_role_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let role = RoleType::new(type_vertex);

        TypeWriter::storage_put_label(snapshot, role.clone(), label);
        TypeWriter::storage_put_relates(snapshot, relation_type, role.clone());
        TypeWriter::storage_put_type_vertex_property(snapshot, role.clone(), Some(ordering));
        if !is_root {
            self.set_role_type_root_supertype(snapshot, role.clone());
        }
        Ok(role)
    }

    fn set_role_type_root_supertype(&self, snapshot: &mut impl WritableSnapshot, role: RoleType<'static>) {
        TypeWriter::storage_put_supertype(
            snapshot,
            role,
            self.get_role_type(snapshot, &Kind::Role.root_label()).unwrap().unwrap(),
        );
    }

    pub fn create_attribute_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        label: &Label<'_>,
        is_root: bool,
    ) -> Result<AttributeType<'static>, ConceptWriteError> {
        OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let type_vertex = self
            .vertex_generator
            .create_attribute_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let attribute_type = AttributeType::new(type_vertex);

        TypeWriter::storage_put_label(snapshot, attribute_type.clone(), label);
        if !is_root {
            TypeWriter::storage_put_supertype(
                snapshot,
                attribute_type.clone(),
                self.get_attribute_type(snapshot, &Kind::Attribute.root_label()).unwrap().unwrap(),
            );
        }
        Ok(attribute_type)
    }

    pub(crate) fn delete_entity_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        entity_type: EntityType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_can_modify_type(snapshot, entity_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_no_subtypes_for_type_deletion(snapshot, entity_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // TODO: Re-enable when we get the thing_manager
        // OperationTimeValidation::validate_exact_type_no_instances_entity(snapshot, entity_type.clone())
        //     .map_err(|source| ConceptWriteError::SchemaValidation {source})?;

        TypeWriter::storage_delete_label(snapshot, entity_type.clone());
        TypeWriter::storage_delete_supertype(snapshot, entity_type);
        Ok(())
    }

    pub(crate) fn delete_relation_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_can_modify_type(snapshot, relation_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_no_subtypes_for_type_deletion(snapshot, relation_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // TODO: Re-enable when we get the thing_manager
        // OperationTimeValidation::validate_exact_type_no_instances_relation(snapshot, relation_type.clone())
        //     .map_err(|source| ConceptWriteError::SchemaValidation {source})?;

        let declared_relates = TypeReader::get_capabilities::<Relates<'static>>(snapshot, relation_type.clone())?;
        for (_role_type, relates) in declared_relates.iter() {
            self.delete_role_type(snapshot, relates.role())?;
        }

        TypeWriter::storage_delete_label(snapshot, relation_type.clone());
        TypeWriter::storage_delete_supertype(snapshot, relation_type);
        Ok(())
    }

    pub(crate) fn delete_attribute_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_can_modify_type(snapshot, attribute_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_no_subtypes_for_type_deletion(snapshot, attribute_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // TODO: Re-enable when we get the thing_manager
        // OperationTimeValidation::validate_exact_type_no_instances_attribute(snapshot, self, attribute.clone())
        //     .map_err(|source| ConceptWriteError::SchemaValidation {  source } )?;

        for owns in
            TypeReader::get_capabilities_for_interface_declared::<Owns<'static>>(snapshot, attribute_type.clone())?
        {
            self.unset_owns(snapshot, owns.owner().into(), owns.attribute())?
        }

        TypeWriter::storage_delete_label(snapshot, attribute_type.clone());
        TypeWriter::storage_delete_supertype(snapshot, attribute_type);
        Ok(())
    }

    pub(crate) fn delete_role_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        role_type: RoleType<'static>,
    ) -> Result<(), ConceptWriteError> {
        // TODO: We have a commit-time check for overrides not being left hanging, but maybe we should clean it/reject here. We DO check that there are no subtypes for type deletion.
        OperationTimeValidation::validate_can_modify_type(snapshot, role_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_no_subtypes_for_type_deletion(snapshot, role_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // TODO: Re-enable when we get the thing_manager
        // OperationTimeValidation::validate_exact_type_no_instances_role(snapshot, role_type.clone().into_owned())
        //     .map_err(|source| ConceptWriteError::SchemaValidation {source})?;

        for plays in TypeReader::get_capabilities_for_interface_declared::<Plays<'static>>(snapshot, role_type.clone())?
        {
            self.unset_plays(snapshot, plays.player(), plays.role())?
        }

        let relates = TypeReader::get_role_type_relates_declared(snapshot, role_type.clone())?;
        TypeWriter::storage_delete_relates(snapshot, relates.relation(), role_type.clone());
        TypeWriter::storage_delete_label(snapshot, role_type.clone());
        TypeWriter::storage_delete_supertype(snapshot, role_type);
        Ok(())
    }

    pub(crate) fn set_label<T: KindAPI<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: T,
        label: &Label<'_>,
    ) -> Result<(), ConceptWriteError> {
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, type_.clone()).is_ok());

        OperationTimeValidation::validate_can_modify_type(snapshot, type_.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

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

        OperationTimeValidation::validate_can_modify_type(snapshot, relation_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

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

        OperationTimeValidation::validate_can_modify_type(snapshot, role_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

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

        OperationTimeValidation::validate_can_modify_type(snapshot, role_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

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
        attribute_type: AttributeType<'static>,
        value_type: ValueType,
    ) -> Result<(), ConceptWriteError> {
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, attribute_type.clone()).is_ok());

        OperationTimeValidation::validate_can_modify_type(snapshot, attribute_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let existing_value_type_with_source = TypeReader::get_value_type(snapshot, attribute_type.clone())?;

        OperationTimeValidation::validate_value_type_compatible_with_inherited_value_type(
            snapshot,
            attribute_type.clone(),
            Some(value_type.clone()),
            existing_value_type_with_source.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_attribute_type_value_type_compatible_with_declared_annotations(
            snapshot,
            attribute_type.clone(),
            Some(value_type.clone()),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_value_type_compatible_with_all_owns_annotations(
            snapshot,
            attribute_type.clone(),
            Some(value_type.clone()),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // We can't change value type now, but it's safer to have this check already called
        if let Some((existing_value_type, _)) = existing_value_type_with_source {
            if value_type != existing_value_type {
                // TODO: Re-enable when we get the thing_manager
                // OperationTimeValidation::validate_exact_type_no_instances_attribute(snapshot, self, attribute.clone())
                //     .map_err(|source| ConceptWriteError::SchemaValidation {  source } )?;
            }
        }

        TypeWriter::storage_set_value_type(snapshot, attribute_type, value_type);
        Ok(())
    }

    pub(crate) fn unset_value_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_can_modify_type(snapshot, attribute_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_value_type_can_be_unset(snapshot, attribute_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_unset_value_type(snapshot, attribute_type);
        Ok(())
    }

    fn set_supertype<K>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        subtype: K,
        supertype: K,
    ) -> Result<(), ConceptWriteError>
    where
        K: KindAPI<'static>,
    {
        debug_assert! {
            OperationTimeValidation::validate_type_exists(snapshot, subtype.clone()).is_ok() &&
                OperationTimeValidation::validate_type_exists(snapshot, supertype.clone()).is_ok()
        };

        OperationTimeValidation::validate_can_modify_type(snapshot, subtype.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_sub_does_not_create_cycle(snapshot, subtype.clone(), supertype.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_supertype_annotations_compatibility(
            snapshot,
            subtype.clone(),
            supertype.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_delete_supertype(snapshot, subtype.clone());
        TypeWriter::storage_put_supertype(snapshot, subtype.clone(), supertype.clone());
        Ok(())
    }

    pub(crate) fn set_attribute_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        subtype: AttributeType<'static>,
        supertype: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_value_type_is_compatible_with_new_supertypes_value_type(
            snapshot,
            subtype.clone(),
            supertype.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_attribute_type_supertype_is_abstract(snapshot, self, supertype.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_attribute_type_does_not_lose_independent_annotation_with_new_supertype(
            snapshot,
            subtype.clone(),
            supertype.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_supertype(snapshot, subtype, supertype)
    }

    pub(crate) fn set_entity_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        subtype: EntityType<'static>,
        supertype: EntityType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_owns_compatible_with_new_supertype(
            snapshot,
            subtype.clone().into_owned_object_type(),
            supertype.clone().into_owned_object_type(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_owns_overrides_compatible_with_new_supertype(
            snapshot,
            subtype.clone(),
            supertype.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_plays_compatible_with_new_supertype(
            snapshot,
            subtype.clone().into_owned_object_type(),
            supertype.clone().into_owned_object_type(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_plays_overrides_compatible_with_new_supertype(
            snapshot,
            subtype.clone(),
            supertype.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_supertype(snapshot, subtype, supertype)
    }

    pub(crate) fn set_relation_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        subtype: RelationType<'static>,
        supertype: RelationType<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_roles_compatible_with_new_relation_supertype(
            snapshot,
            subtype.clone(),
            supertype.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_relates_overrides_compatible_with_new_supertype(
            snapshot,
            subtype.clone(),
            supertype.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_owns_compatible_with_new_supertype(
            snapshot,
            subtype.clone().into_owned_object_type(),
            supertype.clone().into_owned_object_type(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_owns_overrides_compatible_with_new_supertype(
            snapshot,
            subtype.clone(),
            supertype.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_plays_compatible_with_new_supertype(
            snapshot,
            subtype.clone().into_owned_object_type(),
            supertype.clone().into_owned_object_type(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_plays_overrides_compatible_with_new_supertype(
            snapshot,
            subtype.clone(),
            supertype.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_relation_type_does_not_acquire_cascade_annotation_with_new_supertype(
            snapshot,
            subtype.clone(),
            supertype.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_supertype(snapshot, subtype, supertype)
    }

    pub(crate) fn set_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: ObjectType<'static>,
        attribute: AttributeType<'static>,
        ordering: Ordering,
    ) -> Result<(), ConceptWriteError> {
        with_object_type!(owner, |type_| {
            OperationTimeValidation::validate_can_modify_type(snapshot, type_)
                .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        });

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
        TypeWriter::storage_put_capability(snapshot, owns.clone());
        TypeWriter::storage_put_type_edge_property(snapshot, owns, Some(ordering));
        Ok(())
    }

    pub(crate) fn unset_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: ObjectType<'static>,
        attribute: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        with_object_type!(owner, |type_| {
            OperationTimeValidation::validate_can_modify_type(snapshot, type_)
                .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        });

        // TODO: We have a commit-time check for overrides not being left hanging, but maybe we should clean it/reject here. We DO check that there are no subtypes for type deletion.
        OperationTimeValidation::validate_unset_owns_is_not_inherited(snapshot, owner.clone(), attribute.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let owns = Owns::new(ObjectType::new(owner.clone().into_vertex()), attribute.clone());
        TypeWriter::storage_delete_type_edge_property::<Ordering>(snapshot, owns.clone());
        TypeWriter::storage_delete_capability(snapshot, owns.clone());
        Ok(())
    }

    pub(crate) fn set_owns_overridden(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
        overridden: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        // TODO: More validation - instances exist.
        OperationTimeValidation::validate_owns_is_inherited(snapshot, owns.owner(), overridden.attribute())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_overridden_owns_attribute_type_is_supertype_or_self(
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

        OperationTimeValidation::validate_edge_override_annotations_compatibility(
            snapshot,
            &self,
            owns.clone(),
            overridden.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let overridden_value_type_with_source = TypeReader::get_value_type(snapshot, overridden.attribute())?;
        let value_type = TypeReader::get_value_type_without_source(snapshot, owns.attribute())?;

        OperationTimeValidation::validate_value_type_compatible_with_inherited_value_type(
            snapshot,
            owns.attribute(),
            value_type.clone(),
            overridden_value_type_with_source.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_owns_value_type_compatible_with_annotations(
            snapshot,
            overridden.clone(),
            value_type.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_set_type_edge_overridden(snapshot, owns, overridden);
        Ok(())
    }

    pub(crate) fn unset_owns_overridden(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        TypeWriter::storage_delete_type_edge_overridden(snapshot, owns.clone());
        Ok(())
    }

    pub(crate) fn set_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        player: ObjectType<'static>,
        role: RoleType<'static>,
    ) -> Result<Plays<'static>, ConceptWriteError> {
        with_object_type!(player, |type_| {
            OperationTimeValidation::validate_can_modify_type(snapshot, type_)
                .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        });

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
        TypeWriter::storage_put_capability(snapshot, plays.clone());
        Ok(plays)
    }

    pub(crate) fn unset_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        player: ObjectType<'static>,
        role: RoleType<'static>,
    ) -> Result<(), ConceptWriteError> {
        with_object_type!(player, |type_| {
            OperationTimeValidation::validate_can_modify_type(snapshot, type_)
                .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        });

        // TODO: We have a commit-time check for overrides not being left hanging, but maybe we should clean it/reject here. We DO check that there are no subtypes for type deletion.
        OperationTimeValidation::validate_unset_plays_is_not_inherited(snapshot, player.clone(), role.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let plays = Plays::new(ObjectType::new(player.into_vertex()), role);
        TypeWriter::storage_delete_capability(snapshot, plays);
        Ok(())
    }

    pub(crate) fn set_plays_overridden(
        &self,
        snapshot: &mut impl WritableSnapshot,
        plays: Plays<'static>,
        overridden: Plays<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_plays_is_inherited(snapshot, plays.player(), overridden.role())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_overridden_plays_role_type_is_supertype_or_self(
            snapshot,
            plays.clone(),
            overridden.role(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_edge_override_annotations_compatibility(
            snapshot,
            &self,
            plays.clone(),
            overridden.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_set_type_edge_overridden(snapshot, plays, overridden);
        Ok(())
    }

    pub(crate) fn unset_plays_overridden(
        &self,
        snapshot: &mut impl WritableSnapshot,
        plays: Plays<'static>,
    ) -> Result<(), ConceptWriteError> {
        TypeWriter::storage_delete_type_edge_overridden(snapshot, plays);
        Ok(())
    }

    pub(crate) fn set_owns_ordering(
        &self,
        snapshot: &mut impl WritableSnapshot,
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
        match owns_override_opt {
            Some(owns_override) => {
                OperationTimeValidation::validate_owns_override_ordering_match(
                    snapshot,
                    owns.clone(),
                    owns_override.clone(),
                    Some(ordering),
                )
                .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
            }
            None => {}
        }

        TypeWriter::storage_set_owns_ordering(snapshot, owns, ordering);
        Ok(())
    }

    pub(crate) fn set_role_ordering(
        &self,
        snapshot: &mut impl WritableSnapshot,
        role_type: RoleType<'static>,
        ordering: Ordering,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_can_modify_type(snapshot, role_type.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let relates = self.get_role_type_relates(snapshot, role_type.clone().into_owned())?;
        OperationTimeValidation::validate_relates_distinct_annotation_ordering(
            snapshot,
            relates.clone(),
            Some(ordering),
            None,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let relates_override_opt = TypeReader::get_capability_override(snapshot, relates.clone())?;
        match relates_override_opt {
            Some(relates_override) => {
                OperationTimeValidation::validate_role_supertype_ordering_match(
                    snapshot,
                    relates.role(),
                    relates_override.role(),
                    Some(ordering),
                )
                .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
            }
            None => {}
        }

        TypeWriter::storage_put_type_vertex_property(snapshot, role_type, Some(ordering));
        Ok(())
    }

    pub(crate) fn set_role_type_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
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

        self.set_annotation_abstract(snapshot, role_type)
    }

    pub(crate) fn set_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_can_modify_type(snapshot, type_.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_type_supertype_abstractness(
            snapshot,
            type_.clone(),
            None,       // supertype: will be read from storage
            Some(true), // set abstract annotation
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // TODO: Validation: existing instances (or is it schema/data validation?)

        self.set_annotation::<AnnotationAbstract>(snapshot, type_, AnnotationCategory::Abstract, None)
    }

    pub(crate) fn unset_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.unset_annotation::<AnnotationAbstract>(snapshot, type_, AnnotationCategory::Abstract)
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
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.set_annotation::<AnnotationIndependent>(snapshot, type_, AnnotationCategory::Independent, None)
    }

    pub(crate) fn unset_annotation_independent(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.unset_annotation::<AnnotationIndependent>(snapshot, type_, AnnotationCategory::Independent)
    }

    pub(crate) fn set_relates_overridden(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relates: Relates<'static>,
        overridden: Relates<'static>,
    ) -> Result<(), ConceptWriteError> {
        // TODO: More validation - instances exist.
        OperationTimeValidation::validate_relates_is_inherited(snapshot, relates.relation(), overridden.role())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_role_supertype_ordering_match(
            snapshot,
            relates.role(),
            overridden.role(),
            None,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_edge_override_annotations_compatibility(
            snapshot,
            &self,
            relates.clone(),
            overridden.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_supertype(snapshot, relates.role(), overridden.role())?;
        TypeWriter::storage_set_type_edge_overridden(snapshot, relates, overridden);
        Ok(())
    }

    pub(crate) fn unset_relates_overridden(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relates: Relates<'static>,
    ) -> Result<(), ConceptWriteError> {
        TypeWriter::storage_delete_supertype(snapshot, relates.role());
        self.set_role_type_root_supertype(snapshot, relates.role());
        TypeWriter::storage_delete_type_edge_overridden(snapshot, relates);
        Ok(())
    }

    pub(crate) fn set_owns_annotation_distinct(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_owns_distinct_annotation_ordering(snapshot, owns.clone(), None, Some(true))
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_edge_annotation::<AnnotationDistinct>(snapshot, owns, AnnotationCategory::Distinct, None)
    }

    pub(crate) fn set_relates_annotation_distinct(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relates: Relates<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_relates_distinct_annotation_ordering(
            snapshot,
            relates.clone(),
            None,
            Some(true),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_edge_annotation::<AnnotationDistinct>(snapshot, relates, AnnotationCategory::Distinct, None)
    }

    pub(crate) fn unset_edge_annotation_distinct<EDGE>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: EDGE,
    ) -> Result<(), ConceptWriteError>
    where
        EDGE: Capability<'static>,
    {
        self.unset_edge_annotation::<AnnotationDistinct>(snapshot, edge, AnnotationCategory::Distinct)
    }

    pub(crate) fn set_owns_annotation_unique(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_owns_value_type_compatible_with_unique_annotation(
            snapshot,
            owns.clone(),
            TypeReader::get_value_type_without_source(snapshot, owns.attribute())?,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_edge_annotation::<AnnotationUnique>(snapshot, owns, AnnotationCategory::Unique, None)
    }

    pub(crate) fn unset_owns_annotation_unique(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.unset_edge_annotation::<AnnotationUnique>(snapshot, owns, AnnotationCategory::Unique)
    }

    pub(crate) fn set_owns_annotation_key(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_owns_value_type_compatible_with_key_annotation(
            snapshot,
            owns.clone(),
            TypeReader::get_value_type_without_source(snapshot, owns.attribute())?,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        if let Some(override_owns) = TypeReader::get_capability_override(snapshot, owns.clone())? {
            OperationTimeValidation::validate_key_narrows_inherited_cardinality(
                snapshot,
                &self,
                owns.clone(),
                override_owns,
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        }

        self.set_edge_annotation::<AnnotationKey>(snapshot, owns, AnnotationCategory::Key, None)
    }

    pub(crate) fn unset_edge_annotation_key<EDGE>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: EDGE,
    ) -> Result<(), ConceptWriteError>
    where
        EDGE: Capability<'static>,
    {
        self.unset_edge_annotation::<AnnotationKey>(snapshot, edge, AnnotationCategory::Key)
    }

    pub(crate) fn set_edge_annotation_cardinality<EDGE>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: EDGE,
        cardinality: AnnotationCardinality,
    ) -> Result<(), ConceptWriteError>
    where
        EDGE: Capability<'static>,
    {
        OperationTimeValidation::validate_cardinality_arguments(cardinality)
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        if let Some(override_edge) = TypeReader::get_capability_override(snapshot, edge.clone())? {
            OperationTimeValidation::validate_cardinality_narrows_inherited_cardinality(
                snapshot,
                &self,
                edge.clone(),
                override_edge,
                cardinality,
            )
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        }

        self.set_edge_annotation::<AnnotationCardinality>(
            snapshot,
            edge,
            AnnotationCategory::Cardinality,
            Some(cardinality),
        )
    }

    pub(crate) fn unset_edge_annotation_cardinality<EDGE>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: EDGE,
    ) -> Result<(), ConceptWriteError>
    where
        EDGE: Capability<'static>,
    {
        self.unset_edge_annotation::<AnnotationCardinality>(snapshot, edge, AnnotationCategory::Cardinality)
    }

    pub(crate) fn set_annotation_regex(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: AttributeType<'static>,
        regex: AnnotationRegex,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_regex_arguments(regex.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_annotation_regex_compatible_value_type(
            snapshot,
            type_.clone(),
            TypeReader::get_value_type_without_source(snapshot, type_.clone())?,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_annotation_set_only_for_interface::<Owns<'static>>(
            snapshot,
            type_.clone(),
            AnnotationCategory::Regex,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_type_regex_narrows_inherited_regex(
            snapshot,
            type_.clone(),
            None, // supertype: will be read from storage
            regex.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_annotation::<AnnotationRegex>(snapshot, type_, AnnotationCategory::Regex, Some(regex))
    }

    pub(crate) fn unset_annotation_regex(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.unset_annotation::<AnnotationRegex>(snapshot, type_, AnnotationCategory::Regex)
    }

    pub(crate) fn set_owns_annotation_regex(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
        regex: AnnotationRegex,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_regex_arguments(regex.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_annotation_regex_compatible_value_type(
            snapshot,
            owns.attribute(),
            TypeReader::get_value_type_without_source(snapshot, owns.attribute())?,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_annotation_set_only_for_capability(
            snapshot,
            owns.clone(),
            AnnotationCategory::Regex,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_edge_regex_narrows_inherited_regex(
            snapshot,
            owns.clone(),
            None, // overridden_owns: will be read from storage
            regex.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_edge_annotation::<AnnotationRegex>(snapshot, owns, AnnotationCategory::Regex, Some(regex))
    }

    pub(crate) fn unset_edge_annotation_regex<EDGE>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: EDGE,
    ) -> Result<(), ConceptWriteError>
    where
        EDGE: Capability<'static>,
    {
        self.unset_edge_annotation::<AnnotationRegex>(snapshot, edge, AnnotationCategory::Regex)
    }

    pub(crate) fn set_annotation_cascade(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.set_annotation::<AnnotationCascade>(snapshot, type_, AnnotationCategory::Cascade, None)
    }

    pub(crate) fn unset_annotation_cascade(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.unset_annotation::<AnnotationCascade>(snapshot, type_, AnnotationCategory::Cascade)
    }

    pub(crate) fn set_annotation_range(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: AttributeType<'static>,
        range: AnnotationRange,
    ) -> Result<(), ConceptWriteError> {
        let type_value_type = TypeReader::get_value_type_without_source(snapshot, type_.clone())?;

        OperationTimeValidation::validate_annotation_range_compatible_value_type(
            snapshot,
            type_.clone(),
            type_value_type.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_range_arguments(range.clone(), type_value_type)
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_annotation_set_only_for_interface::<Owns<'static>>(
            snapshot,
            type_.clone(),
            AnnotationCategory::Range,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_type_range_narrows_inherited_range(
            snapshot,
            type_.clone(),
            None, // supertype: will be read from storage
            range.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // TODO: Maybe for the future: check if compatible with existing VALUES annotation

        self.set_annotation::<AnnotationRange>(snapshot, type_, AnnotationCategory::Range, Some(range))
    }

    pub(crate) fn unset_annotation_range(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.unset_annotation::<AnnotationRange>(snapshot, type_, AnnotationCategory::Range)
    }

    pub(crate) fn set_owns_annotation_range(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
        range: AnnotationRange,
    ) -> Result<(), ConceptWriteError> {
        let owns_value_type = TypeReader::get_value_type_without_source(snapshot, owns.attribute())?;

        OperationTimeValidation::validate_annotation_range_compatible_value_type(
            snapshot,
            owns.attribute(),
            owns_value_type.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_range_arguments(range.clone(), owns_value_type)
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_annotation_set_only_for_capability(
            snapshot,
            owns.clone(),
            AnnotationCategory::Range,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_edge_range_narrows_inherited_range(
            snapshot,
            owns.clone(),
            None, // overridden_owns: will be read from storage
            range.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // TODO: Maybe for the future: check if compatible with existing VALUES annotation

        self.set_edge_annotation::<AnnotationRange>(snapshot, owns, AnnotationCategory::Range, Some(range))
    }

    pub(crate) fn unset_edge_annotation_range<EDGE: Capability<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: EDGE,
    ) -> Result<(), ConceptWriteError> {
        self.unset_edge_annotation::<AnnotationRange>(snapshot, edge, AnnotationCategory::Range)
    }

    pub(crate) fn set_annotation_values(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: AttributeType<'static>,
        values: AnnotationValues,
    ) -> Result<(), ConceptWriteError> {
        let type_value_type = TypeReader::get_value_type_without_source(snapshot, type_.clone())?;

        OperationTimeValidation::validate_annotation_values_compatible_value_type(
            snapshot,
            type_.clone(),
            type_value_type.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_values_arguments(values.clone(), type_value_type)
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_annotation_set_only_for_interface::<Owns<'static>>(
            snapshot,
            type_.clone(),
            AnnotationCategory::Values,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_type_values_narrows_inherited_values(
            snapshot,
            type_.clone(),
            None, // supertype: will be read from storage
            values.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // TODO: Maybe for the future: check if compatible with existing RANGE annotation

        self.set_annotation::<AnnotationValues>(snapshot, type_, AnnotationCategory::Values, Some(values))
    }

    pub(crate) fn unset_annotation_values(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.unset_annotation::<AnnotationValues>(snapshot, type_, AnnotationCategory::Values)
    }

    pub(crate) fn set_owns_annotation_values(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
        values: AnnotationValues,
    ) -> Result<(), ConceptWriteError> {
        let owns_value_type = TypeReader::get_value_type_without_source(snapshot, owns.attribute())?;

        OperationTimeValidation::validate_annotation_values_compatible_value_type(
            snapshot,
            owns.attribute(),
            owns_value_type.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_values_arguments(values.clone(), owns_value_type)
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_annotation_set_only_for_capability(
            snapshot,
            owns.clone(),
            AnnotationCategory::Values,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_edge_values_narrows_inherited_values(
            snapshot,
            owns.clone(),
            None, // overridden_owns: will be read from storage
            values.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        // TODO: Maybe for the future: check if compatible with existing RANGE annotation
        self.set_edge_annotation::<AnnotationValues>(snapshot, owns, AnnotationCategory::Values, Some(values))
    }

    pub(crate) fn unset_edge_annotation_values<EDGE: Capability<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: EDGE,
    ) -> Result<(), ConceptWriteError> {
        self.unset_edge_annotation::<AnnotationValues>(snapshot, edge, AnnotationCategory::Values)
    }

    fn set_annotation<A: TypeVertexPropertyEncoding<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
        annotation_category: AnnotationCategory,
        annotation_value: Option<A>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_can_modify_type(snapshot, type_.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_declared_annotation_is_compatible_with_other_declared_annotations(
            snapshot,
            type_.clone(),
            annotation_category,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_declared_annotation_is_compatible_with_other_inherited_annotations(
            snapshot,
            type_.clone(),
            annotation_category,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        self.set_annotation_unconditional(snapshot, type_, annotation_value)
    }

    fn set_annotation_unconditional<A: TypeVertexPropertyEncoding<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
        annotation_value: Option<A>,
    ) -> Result<(), ConceptWriteError> {
        match annotation_value {
            None => TypeWriter::storage_put_type_vertex_property::<A>(snapshot, type_, annotation_value),
            Some(_) => TypeWriter::storage_insert_type_vertex_property::<A>(snapshot, type_, annotation_value),
        }
        Ok(())
    }

    fn set_edge_annotation<A: TypeEdgePropertyEncoding<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: impl Capability<'static>,
        annotation_category: AnnotationCategory,
        annotation_value: Option<A>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_can_modify_type(snapshot, edge.interface())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_declared_edge_annotation_is_compatible_with_declared_annotations(
            snapshot,
            edge.clone(),
            annotation_category,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_declared_edge_annotation_is_compatible_with_other_inherited_annotations(
            snapshot,
            edge.clone(),
            annotation_category,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        match annotation_value {
            None => TypeWriter::storage_put_type_edge_property::<A>(snapshot, edge, annotation_value),
            Some(_) => TypeWriter::storage_insert_type_edge_property::<A>(snapshot, edge, annotation_value),
        }
        Ok(())
    }

    fn unset_annotation<A: TypeVertexPropertyEncoding<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl KindAPI<'static>,
        annotation_category: AnnotationCategory,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_can_modify_type(snapshot, type_.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_unset_annotation_is_not_inherited(
            snapshot,
            type_.clone(),
            annotation_category,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_delete_type_vertex_property::<A>(snapshot, type_);
        Ok(())
    }

    fn unset_edge_annotation<A: TypeEdgePropertyEncoding<'static>>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: impl Capability<'static>,
        annotation_category: AnnotationCategory,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_can_modify_type(snapshot, edge.interface())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        OperationTimeValidation::validate_unset_edge_annotation_is_not_inherited(
            snapshot,
            edge.clone(),
            annotation_category,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_delete_type_edge_property::<A>(snapshot, edge);
        Ok(())
    }
}
