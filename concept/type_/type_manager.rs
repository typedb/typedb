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
            definition_key::DefinitionKey,
            definition_key_generator::DefinitionKeyGenerator,
            r#struct::{StructDefinition, StructDefinitionField},
        },
        type_::{
            edge::TypeEdgeEncoding,
            property::TypeEdgePropertyEncoding,
            vertex::{PrefixedTypeVertexEncoding, TypeVertexEncoding},
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

use super::annotation::{AnnotationDistinct, AnnotationIndependent, AnnotationKey, AnnotationRegex, AnnotationUnique};
use crate::{
    error::{ConceptReadError, ConceptWriteError},
    type_::{
        annotation::{AnnotationAbstract, AnnotationCardinality},
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        entity_type::{EntityType, EntityTypeAnnotation},
        object_type::ObjectType,
        owns::{Owns, OwnsAnnotation},
        plays::Plays,
        relates::Relates,
        relation_type::{RelationType, RelationTypeAnnotation},
        role_type::{RoleType, RoleTypeAnnotation},
        type_manager::type_reader::TypeReader,
        KindAPI, ObjectTypeAPI, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
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

macro_rules! get_type_annotations {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident | $annotation_type:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &impl ReadableSnapshot, type_: $type_<'static>
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
        Ok(self.get_relation_type_relates_transitive(snapshot, relation)?.iter().find_map(|(_role, relates)| {
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
                        if (i + 1) == fields_path.len() {
                            return Ok(resolved);
                        } else {
                            return Err(ConceptReadError::Encoding {
                                source: EncodingError::IndexingIntoNonStructField {
                                    struct_name: definition.name,
                                    field_path: fields_path.clone().into_iter().map(|str| str.to_owned()).collect(),
                                },
                            });
                        }
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

    pub(crate) fn get_entity_type_owns(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns(entity_type)))
        } else {
            let owns = TypeReader::get_implemented_interfaces(snapshot, entity_type.clone())?;
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
            let owns = TypeReader::get_implemented_interfaces(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_owns_for_attribute(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_for_attribute_type(attribute_type.clone())))
        } else {
            let plays =
                TypeReader::get_implementations_for_interface::<Owns<'static>>(snapshot, attribute_type.clone())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_owners_for_attribute_transitive(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<ObjectType<'static>, Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_for_attribute_type_transitive(attribute_type.clone())))
        } else {
            let owns = TypeReader::get_implementations_for_interface_transitive::<Owns<'static>>(
                snapshot,
                attribute_type.clone(),
            )?;
            Ok(MaybeOwns::Owned(owns))
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
            let relates = TypeReader::get_relates(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_relation_type_relates_transitive(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<RoleType<'static>, Relates<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_relation_type_relates_transitive(relation_type)))
        } else {
            let relates = TypeReader::get_relates_transitive(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::Owned(relates))
        }
    }

    pub(crate) fn get_plays_for_role_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_for_role_type(role_type.clone())))
        } else {
            let plays = TypeReader::get_implementations_for_interface::<Plays<'static>>(snapshot, role_type.clone())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_plays_for_role_type_transitive(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<ObjectType<'static>, Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_for_role_type_transitive(role_type.clone())))
        } else {
            let plays = TypeReader::get_implementations_for_interface_transitive::<Plays<'static>>(
                snapshot,
                role_type.clone(),
            )?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_entity_type_owns_transitive(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<AttributeType<'static>, Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_transitive(entity_type)))
        } else {
            let owns = TypeReader::get_implemented_interfaces_transitive::<Owns<'static>, EntityType<'static>>(
                snapshot,
                entity_type.clone(),
            )?;
            Ok(MaybeOwns::Owned(owns))
        }
    }

    pub(crate) fn get_relation_type_owns_transitive(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<AttributeType<'static>, Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_transitive(relation_type)))
        } else {
            let owns = TypeReader::get_implemented_interfaces_transitive::<Owns<'static>, RelationType<'static>>(
                snapshot,
                relation_type.clone(),
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
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'this, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays(entity_type)))
        } else {
            let plays = TypeReader::get_implemented_interfaces(snapshot, entity_type.clone())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }
    pub(crate) fn get_entity_type_plays_transitive(
        &self,
        snapshot: &impl ReadableSnapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<RoleType<'static>, Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_transitive(entity_type)))
        } else {
            let plays = TypeReader::get_implemented_interfaces_transitive::<Plays<'static>, EntityType<'static>>(
                snapshot,
                entity_type.clone(),
            )?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_relation_type_plays<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'this, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays(relation_type)))
        } else {
            let plays = TypeReader::get_implemented_interfaces(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_relation_type_plays_transitive(
        &self,
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashMap<RoleType<'static>, Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_transitive(relation_type)))
        } else {
            let plays = TypeReader::get_implemented_interfaces_transitive::<Plays<'static>, RelationType<'static>>(
                snapshot,
                relation_type.clone(),
            )?;
            Ok(MaybeOwns::Owned(plays))
        }
    }

    pub(crate) fn get_plays_overridden(
        &self,
        snapshot: &impl ReadableSnapshot,
        plays: Plays<'static>,
    ) -> Result<MaybeOwns<'_, Option<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_plays_override(plays)))
        } else {
            Ok(MaybeOwns::Owned(TypeReader::get_implementation_override(snapshot, plays)?))
        }
    }

    pub(crate) fn get_attribute_type_value_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_attribute_type_value_type(attribute_type).clone())
        } else {
            Ok(TypeReader::get_value_type(snapshot, attribute_type)?)
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
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
    ) -> Result<MaybeOwns<'_, Option<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_override(owns)))
        } else {
            Ok(MaybeOwns::Owned(TypeReader::get_implementation_override(snapshot, owns)?))
        }
    }

    pub(crate) fn get_owns_effective_annotations<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
    ) -> Result<MaybeOwns<'this, HashMap<OwnsAnnotation, Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::Borrowed(cache.get_owns_effective_annotations(owns)))
        } else {
            let annotations: HashMap<OwnsAnnotation, Owns<'static>> =
                TypeReader::get_effective_type_edge_annotations(snapshot, owns)?
                    .into_iter()
                    .map(|(annotation, owns)| (OwnsAnnotation::from(annotation), owns))
                    .collect();
            Ok(MaybeOwns::Owned(annotations))
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
            Ok(TypeReader::get_type_edge_property::<Ordering>(snapshot, owns)?.unwrap())
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

    pub(crate) const fn role_default_cardinality(&self, ordering: Ordering) -> AnnotationCardinality {
        // TODO: read from database properties the default role cardinality the db was created with
        match ordering {
            Ordering::Unordered => AnnotationCardinality::new(1, Some(1)),
            Ordering::Ordered => AnnotationCardinality::new(0, None),
        }
    }
}

// TODO: Remove this set of comments
// DO: Do all validation, and call TypeWriter methods.
//  All validation must occur before any writes.  All writes must be done through TypeWriter
//      (If this feels like unnecessary indirection, feel free to refactor. I just need structure)
//  Avoid cross-calling methods if it violates the above.
impl TypeManager {
    pub fn create_struct(
        &self,
        snapshot: &mut impl WritableSnapshot,
        name: String,
    ) -> Result<DefinitionKey<'static>, ConceptWriteError> {
        // TODO: Validation
        let definition_key = self
            .definition_key_generator
            .create_struct(snapshot)
            .map_err(|source| ConceptWriteError::Encoding { source })?;
        TypeWriter::storage_put_struct(snapshot, definition_key.clone(), StructDefinition::new(name));
        Ok(definition_key)
    }

    pub fn add_struct_field(
        &self,
        snapshot: &mut impl WritableSnapshot,
        definition_key: DefinitionKey<'static>,
        field_name: String,
        value_type: ValueType,
        is_optional: bool,
    ) -> Result<(), ConceptWriteError> {
        // TODO: Validation
        let mut struct_definition = TypeReader::get_struct_definition(snapshot, definition_key.clone())
            .map_err(|source| ConceptWriteError::ConceptRead { source })?;
        struct_definition
            .add_field(field_name, value_type, is_optional)
            .map_err(|source| ConceptWriteError::Encoding { source })?;
        TypeWriter::storage_put_struct(snapshot, definition_key.clone(), struct_definition);
        Ok(())
    }

    pub fn delete_struct_field(
        &self,
        snapshot: &mut impl WritableSnapshot,
        definition_key: DefinitionKey<'static>,
        field_name: String,
    ) -> Result<(), ConceptWriteError> {
        // TODO: Validation
        let mut struct_definition = TypeReader::get_struct_definition(snapshot, definition_key.clone())
            .map_err(|source| ConceptWriteError::ConceptRead { source })?;
        struct_definition.delete_field(field_name).map_err(|source| ConceptWriteError::Encoding { source })?;
        TypeWriter::storage_put_struct(snapshot, definition_key.clone(), struct_definition);
        Ok(())
    }

    pub fn finalise(self, snapshot: &impl WritableSnapshot) -> Result<(), Vec<ConceptWriteError>> {
        let type_errors = CommitTimeValidation::validate(snapshot);
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
        is_root: bool,
        ordering: Ordering,
    ) -> Result<RoleType<'static>, ConceptWriteError> {
        OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let type_vertex = self
            .vertex_generator
            .create_role_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let role = RoleType::new(type_vertex);
        TypeWriter::storage_put_label(snapshot, role.clone(), label);
        TypeWriter::storage_put_relates(snapshot, relation_type, role.clone());
        self.set_role_ordering(snapshot, role.clone(), ordering);
        if !is_root {
            TypeWriter::storage_put_supertype(
                snapshot,
                role.clone(),
                self.get_role_type(snapshot, &Kind::Role.root_label()).unwrap().unwrap(),
            );
        }
        Ok(role)
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
        entity_type: EntityType<'_>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_type_is_not_root(snapshot, entity_type.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        OperationTimeValidation::validate_no_subtypes(snapshot, entity_type.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        // TODO: Re-enable when we get the thing_manager
        // OperationTimeValidation::validate_exact_type_no_instances_entity(snapshot, entity_type.clone().into_owned())
        //     .map_err(|source| ConceptWriteError::SchemaValidation {source})?;

        TypeWriter::storage_delete_label(snapshot, entity_type.clone().into_owned());
        TypeWriter::storage_delete_supertype(snapshot, entity_type.clone().into_owned());
        Ok(())
    }

    pub(crate) fn delete_relation_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        relation_type: RelationType<'_>,
    ) -> Result<(), ConceptWriteError> {
        // Sufficient to guarantee the roles have no subtypes or instances either
        OperationTimeValidation::validate_type_is_not_root(snapshot, relation_type.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        OperationTimeValidation::validate_no_subtypes(snapshot, relation_type.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        // TODO: Re-enable when we get the thing_manager
        // OperationTimeValidation::validate_exact_type_no_instances_relation(snapshot, relation_type.clone().into_owned())
        //     .map_err(|source| ConceptWriteError::SchemaValidation {source})?;

        let declared_relates =
            TypeReader::get_relates_transitive(snapshot, relation_type.clone().into_owned()).unwrap();
        for (_role_type, relates) in declared_relates.iter() {
            self.delete_role_type(snapshot, relates.role().clone())?; // TODO: Should we replace it with individual calls?
        }
        TypeWriter::storage_delete_label(snapshot, relation_type.clone().into_owned());
        TypeWriter::storage_delete_supertype(snapshot, relation_type.clone().into_owned());
        Ok(())
    }

    pub(crate) fn delete_attribute_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute_type: AttributeType<'_>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_type_is_not_root(snapshot, attribute_type.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        OperationTimeValidation::validate_no_subtypes(snapshot, attribute_type.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        // TODO: Re-enable when we get the thing_manager
        // OperationTimeValidation::validate_exact_type_no_instances_attribute(snapshot, attribute_type.clone().into_owned(), self)
        //     .map_err(|source| ConceptWriteError::SchemaValidation {source})?;

        TypeWriter::storage_delete_label(snapshot, attribute_type.clone().into_owned());
        TypeWriter::storage_delete_supertype(snapshot, attribute_type.clone().into_owned());
        Ok(())
    }

    pub(crate) fn delete_role_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        role_type: RoleType<'_>,
    ) -> Result<(), ConceptWriteError> {
        OperationTimeValidation::validate_type_is_not_root(snapshot, role_type.clone().into_owned())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        // TODO: More validation
        // TODO: Re-enable when we get the thing_manager
        // OperationTimeValidation::validate_exact_type_no_instances_role(snapshot, role_type.clone().into_owned())
        //     .map_err(|source| ConceptWriteError::SchemaValidation {source})?;

        let relates = TypeReader::get_relation(snapshot, role_type.clone().into_owned()).unwrap();
        let relation = relates.relation();
        let role = relates.role();
        TypeWriter::storage_delete_relates(snapshot, relation.clone(), role.clone());
        TypeWriter::storage_delete_label(snapshot, role.clone().into_owned());
        TypeWriter::storage_delete_supertype(snapshot, role.clone().into_owned());
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
            Kind::Role => todo!("Validate uniqueness in ancestry"),
            Kind::Entity | Kind::Attribute | Kind::Relation => {
                OperationTimeValidation::validate_label_uniqueness(snapshot, &label.clone().into_owned())
                    .map_err(|source| ConceptWriteError::SchemaValidation { source })
                    .unwrap(); // TODO: Propagate error instead
            }
        }
        TypeWriter::storage_delete_label(snapshot, type_.clone());
        TypeWriter::storage_put_label(snapshot, type_, &label);
        Ok(())
    }

    // TODO: TypeWriter Refactor
    pub(crate) fn set_value_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        attribute: AttributeType<'static>,
        value_type: ValueType,
    ) -> Result<(), ConceptWriteError> {
        // TODO: Validation
        debug_assert!(OperationTimeValidation::validate_type_exists(snapshot, attribute.clone()).is_ok());
        // Check no instances
        if let Some(existing_value_type) = TypeReader::get_value_type(snapshot, attribute.clone())
            .map_err(|source| ConceptWriteError::ConceptRead { source })?
        {
            if value_type != existing_value_type {
                // TODO: Re-enable when we get the thing_manager
                // OperationTimeValidation::validate_exact_type_no_instances_attribute(snapshot, attribute.clone(), self)
                //     .map_err(|source| ConceptWriteError::SchemaValidation {  source } )?;
            }
        }
        // Compatibility with supertype value-type must be done at commit time.

        TypeWriter::storage_set_value_type(snapshot, attribute, value_type);
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
            OperationTimeValidation::validate_type_exists(snapshot, subtype.clone()).is_ok()  &&
            OperationTimeValidation::validate_type_exists(snapshot, supertype.clone()).is_ok()
        };
        // TODO: Validation. This may have to split per type.
        OperationTimeValidation::validate_sub_does_not_create_cycle(snapshot, subtype.clone(), supertype.clone())
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
        OperationTimeValidation::validate_value_types_compatible(
            TypeReader::get_value_type(snapshot, subtype.clone())?,
            TypeReader::get_value_type(snapshot, supertype.clone())?,
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;
        OperationTimeValidation::validate_type_is_abstract(snapshot, supertype.clone())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        Self::set_supertype(self, snapshot, subtype, supertype)
    }

    pub(crate) fn set_entity_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        subtype: EntityType<'static>,
        supertype: EntityType<'static>,
    ) -> Result<(), ConceptWriteError> {
        // TODO: EntityType specific validation (probably nothing)
        Self::set_supertype(self, snapshot, subtype, supertype)
    }

    pub(crate) fn set_relation_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        subtype: RelationType<'static>,
        supertype: RelationType<'static>,
    ) -> Result<(), ConceptWriteError> {
        // TODO: RelationType specific validation
        Self::set_supertype(self, snapshot, subtype, supertype)
    }

    pub(crate) fn set_role_type_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        subtype: RoleType<'static>,
        supertype: RoleType<'static>,
    ) -> Result<(), ConceptWriteError> {
        // TODO: RoleType specific validation
        Self::set_supertype(self, snapshot, subtype, supertype)
    }

    // TODO: If the validation for owns and plays can be made generic, we should see if we can make these functions generic as well.
    pub(crate) fn set_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: impl OwnerAPI<'static>,
        attribute: AttributeType<'static>,
        ordering: Ordering,
    ) {
        // TODO: Validation

        let owns = Owns::new(ObjectType::new(owner.clone().into_vertex()), attribute.clone());
        TypeWriter::storage_put_interface_impl(snapshot, owns.clone());
        TypeWriter::storage_put_type_edge_property(snapshot, owns, Some(ordering));
    }

    pub(crate) fn delete_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owner: impl ObjectTypeAPI<'static>,
        attribute: AttributeType<'static>,
    ) {
        // TODO: Validation

        let owns = Owns::new(ObjectType::new(owner.clone().into_vertex()), attribute.clone());
        TypeWriter::storage_delete_type_edge_property::<Ordering>(snapshot, owns.clone());
        TypeWriter::storage_delete_interface_impl(snapshot, owns.clone());
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
        OperationTimeValidation::validate_overridden_is_supertype(snapshot, owns.attribute(), overridden.attribute())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_set_type_edge_overridden(snapshot, owns.clone(), overridden.clone()); // .attribute().clone());
        Ok(())
    }

    pub(crate) fn delete_owns_overridden(
        &self,
        snapshot: &mut impl WritableSnapshot,
        owns: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        // TODO: validation
        TypeWriter::storage_delete_type_edge_overridden(snapshot, owns.clone()); // .attribute().clone());
        Ok(())
    }

    pub(crate) fn set_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        player: impl KindAPI<'static> + ObjectTypeAPI<'static> + PlayerAPI<'static>,
        role: RoleType<'static>,
    ) -> Result<Plays<'static>, ConceptWriteError> {
        // TODO: Validation
        let plays = Plays::new(ObjectType::new(player.into_vertex()), role);
        TypeWriter::storage_put_interface_impl(snapshot, plays.clone());
        Ok(plays)
    }

    pub(crate) fn delete_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        player: impl ObjectTypeAPI<'static> + PlayerAPI<'static>,
        role: RoleType<'static>,
    ) -> Result<(), ConceptWriteError> {
        // TODO: Validation.
        // TODO: This could really return the plays
        OperationTimeValidation::validate_plays_is_declared(
            snapshot,
            ObjectType::new(player.clone().into_vertex()),
            role.clone(),
        )
        .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        let plays = Plays::new(ObjectType::new(player.into_vertex()), role);
        TypeWriter::storage_delete_interface_impl(snapshot, plays);
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
        OperationTimeValidation::validate_overridden_is_supertype(snapshot, plays.role(), overridden.role())
            .map_err(|source| ConceptWriteError::SchemaValidation { source })?;

        TypeWriter::storage_set_type_edge_overridden(snapshot, plays, overridden); //.role());
        Ok(())
    }

    pub(crate) fn delete_plays_overridden(
        &self,
        snapshot: &mut impl WritableSnapshot,
        plays: Plays<'static>,
    ) -> Result<(), ConceptWriteError> {
        // TODO: Validation
        TypeWriter::storage_delete_type_edge_overridden(snapshot, plays);
        Ok(())
    }

    pub(crate) fn set_owns_ordering(&self, snapshot: &mut impl WritableSnapshot, owns: Owns<'_>, ordering: Ordering) {
        TypeWriter::storage_set_owns_ordering(snapshot, owns, ordering)
    }

    pub(crate) fn delete_owns_ordering(&self, snapshot: &mut impl WritableSnapshot, owns: Owns<'_>) {
        TypeWriter::storage_delete_owns_ordering(snapshot, owns)
    }

    pub(crate) fn set_role_ordering(
        &self,
        snapshot: &mut impl WritableSnapshot,
        role: RoleType<'_>,
        ordering: Ordering,
    ) {
        TypeWriter::storage_put_type_vertex_property(snapshot, role, Some(ordering))
    }

    pub(crate) fn set_annotation_abstract(&self, snapshot: &mut impl WritableSnapshot, type_: impl TypeAPI<'static>) {
        // TODO: Validation
        TypeWriter::storage_put_type_vertex_property::<AnnotationAbstract>(snapshot, type_, None)
    }

    pub(crate) fn delete_annotation_abstract(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl TypeAPI<'static>,
    ) {
        // TODO: Validation
        TypeWriter::storage_delete_type_vertex_property::<AnnotationAbstract>(snapshot, type_)
    }

    pub(crate) fn set_annotation_distinct(&self, snapshot: &mut impl WritableSnapshot, type_: impl TypeAPI<'static>) {
        // TODO: Validation
        TypeWriter::storage_put_type_vertex_property::<AnnotationDistinct>(snapshot, type_, None)
    }

    pub(crate) fn delete_annotation_distinct(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl TypeAPI<'static>,
    ) {
        // TODO: Validation
        TypeWriter::storage_delete_type_vertex_property::<AnnotationDistinct>(snapshot, type_)
    }

    pub(crate) fn set_annotation_independent(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl TypeAPI<'static>,
    ) {
        // TODO: Validation
        TypeWriter::storage_put_type_vertex_property::<AnnotationIndependent>(snapshot, type_, None)
    }

    pub(crate) fn delete_annotation_independent(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl TypeAPI<'static>,
    ) {
        // TODO: Validation
        TypeWriter::storage_delete_type_vertex_property::<AnnotationIndependent>(snapshot, type_)
    }

    pub(crate) fn set_annotation_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl TypeAPI<'static>,
        annotation: AnnotationCardinality,
    ) {
        // TODO: Validation
        TypeWriter::storage_put_type_vertex_property::<AnnotationCardinality>(snapshot, type_, Some(annotation))
    }

    pub(crate) fn delete_annotation_cardinality(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl TypeAPI<'static>,
    ) {
        TypeWriter::storage_delete_type_vertex_property::<AnnotationCardinality>(snapshot, type_)
    }

    pub(crate) fn set_annotation_regex(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_: impl TypeAPI<'static>,
        regex: AnnotationRegex,
    ) {
        TypeWriter::storage_put_type_vertex_property::<AnnotationRegex>(snapshot, type_, Some(regex))
    }

    pub(crate) fn delete_annotation_regex(&self, snapshot: &mut impl WritableSnapshot, type_: impl TypeAPI<'static>) {
        // TODO debug assert that stored regex matches
        // TODO: Validation
        TypeWriter::storage_delete_type_vertex_property::<AnnotationRegex>(snapshot, type_)
    }

    pub(crate) fn set_edge_annotation_distinct<'b>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: impl TypeEdgeEncoding<'b>,
    ) {
        // TODO: Validation
        TypeWriter::storage_put_type_edge_property::<AnnotationDistinct>(snapshot, edge, None)
    }

    pub(crate) fn delete_edge_annotation_distinct<'b>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: impl TypeEdgeEncoding<'b>,
    ) {
        // TODO: Validation
        TypeWriter::storage_delete_type_edge_property::<AnnotationDistinct>(snapshot, edge)
    }

    pub(crate) fn set_edge_annotation_unique<'b>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: impl TypeEdgeEncoding<'b>,
    ) {
        // TODO: Validation
        TypeWriter::storage_put_type_edge_property::<AnnotationUnique>(snapshot, edge, None)
    }

    pub(crate) fn delete_edge_annotation_unique<'b>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: impl TypeEdgeEncoding<'b>,
    ) {
        // TODO: Validation
        TypeWriter::storage_delete_type_edge_property::<AnnotationUnique>(snapshot, edge)
    }

    pub(crate) fn set_edge_annotation_key<'b>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: impl TypeEdgeEncoding<'b>,
    ) {
        TypeWriter::storage_put_type_edge_property::<AnnotationKey>(snapshot, edge, None)
    }

    pub(crate) fn delete_edge_annotation_key<'b>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: impl TypeEdgeEncoding<'b>,
    ) {
        TypeWriter::storage_delete_type_edge_property::<AnnotationKey>(snapshot, edge)
    }

    pub(crate) fn set_edge_annotation_cardinality<'b>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: impl TypeEdgeEncoding<'b>,
        annotation: AnnotationCardinality,
    ) {
        TypeWriter::storage_put_type_edge_property::<AnnotationCardinality>(snapshot, edge, Some(annotation))
    }

    pub(crate) fn delete_edge_annotation_cardinality<'b>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        edge: impl TypeEdgeEncoding<'b>,
    ) {
        TypeWriter::storage_delete_type_edge_property::<AnnotationCardinality>(snapshot, edge)
    }
}
