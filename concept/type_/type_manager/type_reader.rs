/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use encoding::{
    error::EncodingError,
    graph::{
        definition::{definition_key::DefinitionKey, r#struct::StructDefinition, DefinitionValueEncoding},
        type_::{
            edge::{TypeEdge, TypeEdgeEncoding},
            index::{IdentifierIndex, LabelToTypeVertexIndex, NameToStructDefinitionIndex},
            property::{TypeEdgeProperty, TypeEdgePropertyEncoding, TypeVertexProperty, TypeVertexPropertyEncoding},
            vertex::{PrefixedTypeVertexEncoding, TypeVertex, TypeVertexEncoding},
            CapabilityKind,
        },
    },
    layout::infix::Infix,
    value::{label::Label, value_type::ValueType},
    Keyable,
};
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::{
    key_range::{KeyRange, RangeEnd, RangeStart},
    snapshot::ReadableSnapshot,
};

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::{
            Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCascade, AnnotationDistinct,
            AnnotationIndependent, AnnotationKey, AnnotationRange, AnnotationRegex, AnnotationUnique, AnnotationValues,
        },
        attribute_type::AttributeType,
        constraint::{
            get_owns_default_constraints, get_plays_default_constraints, get_relates_default_constraints,
            CapabilityConstraint, Constraint, ConstraintScope, TypeConstraint,
        },
        entity_type::EntityType,
        object_type::ObjectType,
        owns::Owns,
        relates::Relates,
        relation_type::RelationType,
        role_type::RoleType,
        sub::Sub,
        Capability, KindAPI, Ordering, TypeAPI,
    },
};

// non-instantiable
pub enum TypeReader {}

impl TypeReader {
    pub(crate) fn get_labelled_type<T>(
        snapshot: &impl ReadableSnapshot,
        label: &Label<'_>,
    ) -> Result<Option<T>, Box<ConceptReadError>>
    where
        T: TypeAPI<'static>,
    {
        let key = LabelToTypeVertexIndex::build(label).into_storage_key();
        match snapshot.get::<BUFFER_KEY_INLINE>(key.as_reference()) {
            Err(error) => Err(Box::new(ConceptReadError::SnapshotGet { source: error })),
            Ok(None) => Ok(None),
            Ok(Some(value)) => match T::from_bytes(Bytes::Array(value)) {
                Ok(type_) => Ok(Some(type_)),
                Err(err) => match err {
                    EncodingError::UnexpectedPrefix { .. } => Ok(None),
                    _ => Err(Box::new(ConceptReadError::Encoding { source: err })),
                },
            },
        }
    }

    pub(crate) fn get_roles_by_name(
        snapshot: &impl ReadableSnapshot,
        name: String,
    ) -> Result<Vec<RoleType>, Box<ConceptReadError>> {
        let mut name_with_colon = name;
        name_with_colon.push(':');
        let key = LabelToTypeVertexIndex::build(&Label::build(name_with_colon.as_str())).into_storage_key();
        let vec = snapshot
            .iterate_range(KeyRange::new_within(
                RangeStart::Inclusive(key),
                IdentifierIndex::<TypeVertex>::FIXED_WIDTH_ENCODING,
            ))
            .collect_cloned_vec(|_key, value| match RoleType::from_bytes(Bytes::copy(value)) {
                Err(_) => None,
                Ok(role_type) => Some(role_type),
            })
            .map_err(|source| Box::new(ConceptReadError::SnapshotIterate { source }))?;
        Ok(vec.into_iter().flatten().collect())
    }

    pub(crate) fn get_struct_definition_key(
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<Option<DefinitionKey<'static>>, Box<ConceptReadError>> {
        let index_key = NameToStructDefinitionIndex::build(name);
        let bytes = snapshot
            .get(index_key.into_storage_key().as_reference())
            .map_err(|source| Box::new(ConceptReadError::SnapshotGet { source }))?;
        Ok(bytes.map(|value| DefinitionKey::new(Bytes::Array(value))))
    }

    pub(crate) fn get_struct_definition(
        snapshot: &impl ReadableSnapshot,
        definition_key: DefinitionKey<'_>,
    ) -> Result<StructDefinition, Box<ConceptReadError>> {
        let bytes = snapshot
            .get::<BUFFER_VALUE_INLINE>(definition_key.clone().into_storage_key().as_reference())
            .map_err(|source| Box::new(ConceptReadError::SnapshotGet { source }))?;
        Ok(StructDefinition::from_bytes(&bytes.unwrap()))
    }

    pub(crate) fn get_struct_definitions_all(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<HashMap<DefinitionKey<'static>, StructDefinition>, Box<ConceptReadError>> {
        snapshot
            .iterate_range(KeyRange::new_within(
                RangeStart::Inclusive(DefinitionKey::build_prefix(StructDefinition::PREFIX)),
                StructDefinition::PREFIX.fixed_width_keys(),
            ))
            .collect_cloned_hashmap(|key, value| {
                (DefinitionKey::new(Bytes::Array(key.bytes().into())), StructDefinition::from_bytes(value))
            })
            .map_err(|source| Box::new(ConceptReadError::SnapshotIterate { source }))
    }

    pub(crate) fn get_struct_definition_usages_in_attribute_types(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<HashMap<DefinitionKey<'static>, HashSet<AttributeType>>, Box<ConceptReadError>> {
        let mut usages: HashMap<DefinitionKey<'static>, HashSet<AttributeType>> = HashMap::new();

        let attribute_types = TypeReader::get_attribute_types(snapshot)?;
        for attribute_type in attribute_types {
            if let Some(ValueType::Struct(definition_key)) =
                TypeReader::get_value_type_declared(snapshot, attribute_type.clone())?
            {
                if !usages.contains_key(&definition_key) {
                    usages.insert(definition_key.clone(), HashSet::new());
                }
                usages.get_mut(&definition_key).unwrap().insert(attribute_type);
            }
        }

        Ok(usages)
    }

    pub(crate) fn get_struct_definition_usages_in_struct_definitions(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<HashMap<DefinitionKey<'static>, HashSet<DefinitionKey<'static>>>, Box<ConceptReadError>> {
        let mut usages: HashMap<DefinitionKey<'static>, HashSet<DefinitionKey<'static>>> = HashMap::new();

        let struct_definitions = TypeReader::get_struct_definitions_all(snapshot)?;
        for (owner_key, struct_definition) in struct_definitions {
            for value_type in struct_definition.fields.values().map(|field| field.value_type.clone()) {
                if let ValueType::Struct(definition_key) = value_type {
                    if !usages.contains_key(&definition_key) {
                        usages.insert(definition_key.clone(), HashSet::new());
                    }
                    usages.get_mut(&definition_key).unwrap().insert(owner_key.clone());
                }
            }
        }

        Ok(usages)
    }

    pub(crate) fn get_object_types(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Vec<ObjectType>, Box<ConceptReadError>> {
        let entity_types = Self::get_entity_types(snapshot)?;
        let relation_types = Self::get_relation_types(snapshot)?;
        Ok((entity_types.into_iter().map(ObjectType::Entity))
            .chain(relation_types.into_iter().map(ObjectType::Relation))
            .collect())
    }

    pub(crate) fn get_entity_types(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Vec<EntityType>, Box<ConceptReadError>> {
        snapshot
            .iterate_range(KeyRange::new_within(
                RangeStart::Inclusive(EntityType::prefix_for_kind()),
                EntityType::PREFIX.fixed_width_keys(),
            ))
            .collect_cloned_vec(|key, _| EntityType::new(TypeVertex::new(Bytes::copy(key.bytes()))))
            .map_err(|error| Box::new(ConceptReadError::SnapshotIterate { source: error }))
    }

    pub(crate) fn get_relation_types(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Vec<RelationType>, Box<ConceptReadError>> {
        snapshot
            .iterate_range(KeyRange::new_within(
                RangeStart::Inclusive(RelationType::prefix_for_kind()),
                RelationType::PREFIX.fixed_width_keys(),
            ))
            .collect_cloned_vec(|key, _| RelationType::new(TypeVertex::new(Bytes::copy(key.bytes()))))
            .map_err(|error| Box::new(ConceptReadError::SnapshotIterate { source: error }))
    }

    pub(crate) fn get_attribute_types(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Vec<AttributeType>, Box<ConceptReadError>> {
        snapshot
            .iterate_range(KeyRange::new_within(RangeStart::Inclusive(AttributeType::prefix_for_kind()), false))
            .collect_cloned_vec(|key, _| AttributeType::new(TypeVertex::new(Bytes::copy(key.bytes()))))
            .map_err(|error| Box::new(ConceptReadError::SnapshotIterate { source: error }))
    }

    pub(crate) fn get_role_types(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Vec<RoleType>, Box<ConceptReadError>> {
        snapshot
            .iterate_range(KeyRange::new_within(
                RangeStart::Inclusive(RoleType::prefix_for_kind()),
                RoleType::PREFIX.fixed_width_keys(),
            ))
            .collect_cloned_vec(|key, _| RoleType::new(TypeVertex::new(Bytes::copy(key.bytes()))))
            .map_err(|error| Box::new(ConceptReadError::SnapshotIterate { source: error }))
    }

    // TODO: Should get_{super/sub}type[s_transitive] return T or T::SelfStatic.
    // T::SelfStatic is the more consistent, more honest interface, but T is convenient.
    pub(crate) fn get_supertype<T>(
        snapshot: &impl ReadableSnapshot,
        subtype: T,
    ) -> Result<Option<T>, Box<ConceptReadError>>
    where
        T: TypeAPI<'static>,
    {
        Ok(snapshot
            .iterate_range(KeyRange::new_within(
                RangeStart::Inclusive(Sub::prefix_for_canonical_edges_from(subtype)),
                TypeEdge::FIXED_WIDTH_ENCODING,
            ))
            .first_cloned()
            .map_err(|error| Box::new(ConceptReadError::SnapshotIterate { source: error }))?
            .map(|(key, _)| Sub::<T>::decode_canonical_edge(Bytes::Array(key.into_byte_array())).supertype()))
    }

    pub(crate) fn get_supertypes_transitive<T: TypeAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        subtype: T,
    ) -> Result<Vec<T>, Box<ConceptReadError>> {
        // Attention: it is important to be ordered!
        let mut supertypes: Vec<T> = Vec::new();
        let mut supertype_opt = TypeReader::get_supertype(snapshot, subtype.clone())?;
        while let Some(supertype) = supertype_opt {
            supertypes.push(supertype.clone());
            supertype_opt = TypeReader::get_supertype(snapshot, supertype.clone())?;
        }
        Ok(supertypes)
    }

    pub(crate) fn get_subtypes<T>(
        snapshot: &impl ReadableSnapshot,
        supertype: T,
    ) -> Result<HashSet<T>, Box<ConceptReadError>>
    where
        T: TypeAPI<'static>,
    {
        snapshot
            .iterate_range(KeyRange::new_within(
                RangeStart::Inclusive(Sub::prefix_for_reverse_edges_from(supertype)),
                TypeEdge::FIXED_WIDTH_ENCODING,
            ))
            .collect_cloned_hashset(|key, _| {
                Sub::<T>::decode_reverse_edge(Bytes::Reference(key.bytes()).into_owned()).subtype()
            })
            .map_err(|error| Box::new(ConceptReadError::SnapshotIterate { source: error }))
    }

    pub fn get_subtypes_transitive<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<Vec<T>, Box<ConceptReadError>>
    where
        T: TypeAPI<'static>,
    {
        let mut subtypes_transitive: Vec<T> = Vec::new();
        let mut types_to_check: Vec<T> = Vec::from([type_]);

        while let Some(type_) = types_to_check.pop() {
            let subtypes = Self::get_subtypes(snapshot, type_)?;
            types_to_check.extend(subtypes.iter().cloned());
            subtypes_transitive.extend(subtypes.into_iter());
        }

        Ok(subtypes_transitive)
    }

    pub(crate) fn get_label<'a>(
        snapshot: &impl ReadableSnapshot,
        type_: impl TypeAPI<'a>,
    ) -> Result<Option<Label<'static>>, Box<ConceptReadError>> {
        Self::get_type_property_declared::<Label<'static>>(snapshot, type_)
    }

    pub(crate) fn get_capabilities_declared<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        owner: impl TypeVertexEncoding<'static>,
    ) -> Result<HashSet<CAP>, Box<ConceptReadError>> {
        let owns_prefix = CAP::prefix_for_canonical_edges_from(CAP::ObjectType::new(owner.into_vertex()));
        snapshot
            .iterate_range(KeyRange::new_within(RangeStart::Inclusive(owns_prefix), TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| CAP::decode_canonical_edge(Bytes::Reference(key.bytes()).into_owned()))
            .map_err(|error| Box::new(ConceptReadError::SnapshotIterate { source: error }))
    }

    pub(crate) fn get_capabilities<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        object_type: CAP::ObjectType,
        allow_specialised: bool,
    ) -> Result<HashSet<CAP>, Box<ConceptReadError>> {
        let mut transitive_capabilities: HashSet<CAP> = HashSet::new();
        let mut transitive_interfaces: HashSet<CAP::InterfaceType> = HashSet::new();
        let mut current_type = Some(object_type);
        while current_type.is_some() {
            let declared_capabilities: HashSet<CAP> =
                Self::get_capabilities_declared(snapshot, current_type.as_ref().unwrap().clone())?;
            for capability in declared_capabilities.into_iter() {
                if allow_specialised || !transitive_interfaces.contains(&capability.interface()) {
                    transitive_capabilities.insert(capability.clone());
                    transitive_interfaces.insert(capability.interface());
                }
            }
            current_type = Self::get_supertype(snapshot, current_type.unwrap())?;
        }
        Ok(transitive_capabilities)
    }

    pub(crate) fn get_capabilities_for_interface<CAP>(
        snapshot: &impl ReadableSnapshot,
        interface_type: CAP::InterfaceType,
    ) -> Result<HashSet<CAP>, Box<ConceptReadError>>
    where
        CAP: Capability<'static>,
    {
        let owns_prefix = CAP::prefix_for_reverse_edges_from(interface_type);
        snapshot
            .iterate_range(KeyRange::new_within(RangeStart::Inclusive(owns_prefix), TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| CAP::decode_reverse_edge(Bytes::Array(key.bytes().into())))
            .map_err(|error| Box::new(ConceptReadError::SnapshotIterate { source: error }))
    }

    pub(crate) fn get_object_types_with_capabilities_for_interface<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        interface_type: CAP::InterfaceType,
    ) -> Result<HashMap<CAP::ObjectType, CAP>, Box<ConceptReadError>> {
        let mut capabilities: HashMap<CAP::ObjectType, CAP> = HashMap::new();
        let interface_capabilities: HashSet<CAP> =
            Self::get_capabilities_for_interface(snapshot, interface_type.clone())?;

        for interface_capability in interface_capabilities {
            for object_type in Self::get_object_types_with_capability(snapshot, interface_capability.clone())? {
                debug_assert!(!capabilities.contains_key(&object_type));
                capabilities.insert(object_type, interface_capability.clone());
            }
        }

        Ok(capabilities)
    }

    // Do not expose this method as there is a risk of misuse (it doesn't search for specialised capabilities).
    // If needed, make two explicit methods: with_capability and with_capability_including_specialised or smth
    fn get_object_types_with_capability<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
    ) -> Result<HashSet<CAP::ObjectType>, Box<ConceptReadError>> {
        let mut object_types = HashSet::new();

        let all_subtypes =
            TypeAPI::chain_types(capability.object(), Self::get_subtypes_transitive(snapshot, capability.object())?);
        for object_type in all_subtypes {
            let object_type_capabilities = TypeReader::get_capabilities::<CAP>(snapshot, object_type.clone(), false)?;
            if object_type_capabilities.contains(&capability) {
                object_types.insert(object_type.clone());
            }
        }

        Ok(object_types)
    }

    pub(crate) fn get_role_type_relates_root(
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType,
    ) -> Result<Relates, Box<ConceptReadError>> {
        let mut root_relates = None;
        let all_relates = Self::get_capabilities_for_interface::<Relates>(snapshot, role_type)?;
        for relates in all_relates.into_iter() {
            if !Self::is_relates_specialising(snapshot, relates.clone())? {
                debug_assert!(root_relates.is_none());
                root_relates = Some(relates);
            }
        }
        root_relates.ok_or(Box::new(ConceptReadError::CorruptMissingMandatoryRootRelatesForRole))
    }

    pub(crate) fn get_relation_type_relates_root(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType,
    ) -> Result<HashSet<Relates>, Box<ConceptReadError>> {
        let mut root_relates = HashSet::new();
        let all_declared_relates = Self::get_capabilities_declared::<Relates>(snapshot, relation_type)?;
        for relates in all_declared_relates.into_iter() {
            if !Self::is_relates_specialising(snapshot, relates.clone())? {
                root_relates.insert(relates);
            }
        }
        Ok(root_relates)
    }

    pub(crate) fn is_relates_specialising(
        snapshot: &impl ReadableSnapshot,
        relates: Relates,
    ) -> Result<bool, Box<ConceptReadError>> {
        let relation_type_label =
            Self::get_label(snapshot, relates.relation())?.ok_or(ConceptReadError::CorruptMissingLabelOfType)?;
        let role_type_label =
            Self::get_label(snapshot, relates.role())?.ok_or(ConceptReadError::CorruptMissingLabelOfType)?;
        Ok(relation_type_label.name()
            != role_type_label.scope().ok_or(ConceptReadError::CorruptMissingMandatoryScopeForRoleTypeLabel)?)
    }

    pub(crate) fn get_value_type_declared(
        snapshot: &impl ReadableSnapshot,
        type_: AttributeType,
    ) -> Result<Option<ValueType>, Box<ConceptReadError>> {
        Self::get_type_property_declared::<ValueType>(snapshot, type_)
    }

    pub(crate) fn get_value_type(
        snapshot: &impl ReadableSnapshot,
        type_: AttributeType,
    ) -> Result<Option<(ValueType, AttributeType)>, Box<ConceptReadError>> {
        Self::get_type_property::<ValueType, AttributeType>(snapshot, type_)
    }

    pub(crate) fn get_value_type_without_source(
        snapshot: &impl ReadableSnapshot,
        type_: AttributeType,
    ) -> Result<Option<ValueType>, Box<ConceptReadError>> {
        Self::get_value_type(snapshot, type_).map(|result| result.map(|(value_type, _)| value_type))
    }

    pub(crate) fn get_type_property_declared<'a, PROPERTY>(
        snapshot: &impl ReadableSnapshot,
        type_: impl TypeVertexEncoding<'a>,
    ) -> Result<Option<PROPERTY>, Box<ConceptReadError>>
    where
        PROPERTY: TypeVertexPropertyEncoding<'static>,
    {
        let property = snapshot
            .get_mapped(PROPERTY::build_key(type_).into_storage_key().as_reference(), |value| {
                PROPERTY::from_value_bytes(value)
            })
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?;
        Ok(property)
    }

    pub(crate) fn get_type_property<PROPERTY, SOURCE>(
        snapshot: &impl ReadableSnapshot,
        type_: SOURCE,
    ) -> Result<Option<(PROPERTY, SOURCE)>, Box<ConceptReadError>>
    where
        PROPERTY: TypeVertexPropertyEncoding<'static>,
        SOURCE: TypeAPI<'static> + Clone,
    {
        let mut type_opt = Some(type_);
        while let Some(curr_type) = type_opt {
            if let Some(property) = Self::get_type_property_declared::<PROPERTY>(snapshot, curr_type.clone())? {
                return Ok(Some((property, curr_type.clone())));
            }
            type_opt = Self::get_supertype(snapshot, curr_type)?;
        }
        Ok(None)
    }

    pub(crate) fn get_type_edge_property<'a, PROPERTY>(
        snapshot: &impl ReadableSnapshot,
        edge: impl TypeEdgeEncoding<'a>,
    ) -> Result<Option<PROPERTY>, Box<ConceptReadError>>
    where
        PROPERTY: TypeEdgePropertyEncoding<'static>,
    {
        let property = snapshot
            .get_mapped(PROPERTY::build_key(edge).into_storage_key().as_reference(), |value| {
                PROPERTY::from_value_bytes(value)
            })
            .map_err(|err| Box::new(ConceptReadError::SnapshotGet { source: err }))?;
        Ok(property)
    }

    pub(crate) fn get_type_ordering(
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType,
    ) -> Result<Ordering, Box<ConceptReadError>> {
        match Self::get_type_property_declared(snapshot, role_type)? {
            Some(ordering) => Ok(ordering),
            None => Err(Box::new(ConceptReadError::OrderingValueMissing)),
        }
    }

    pub(crate) fn get_capability_ordering(
        snapshot: &impl ReadableSnapshot,
        owns: Owns,
    ) -> Result<Ordering, Box<ConceptReadError>> {
        match Self::get_type_edge_property::<Ordering>(snapshot, owns)? {
            Some(ordering) => Ok(ordering),
            None => Err(Box::new(ConceptReadError::OrderingValueMissing)),
        }
    }

    pub(crate) fn get_type_annotations_declared<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<HashSet<T::AnnotationType>, Box<ConceptReadError>> {
        snapshot
            .iterate_range(KeyRange::new_variable_width(
                RangeStart::Inclusive(
                    TypeVertexProperty::build(type_.vertex(), Infix::ANNOTATION_MIN).into_storage_key(),
                ),
                RangeEnd::EndPrefixInclusive(
                    TypeVertexProperty::build(type_.vertex(), Infix::ANNOTATION_MAX).into_storage_key(),
                ),
            ))
            .collect_cloned_hashset(|key, value| {
                let annotation_key = TypeVertexProperty::new(Bytes::Reference(key.bytes()));
                let annotation = match annotation_key.infix() {
                    Infix::PropertyAnnotationAbstract => Annotation::Abstract(AnnotationAbstract),
                    Infix::PropertyAnnotationDistinct => Annotation::Distinct(AnnotationDistinct),
                    Infix::PropertyAnnotationIndependent => Annotation::Independent(AnnotationIndependent),
                    Infix::PropertyAnnotationCardinality => Annotation::Cardinality(
                        <AnnotationCardinality as TypeVertexPropertyEncoding>::from_value_bytes(value),
                    ),
                    Infix::PropertyAnnotationRegex => {
                        Annotation::Regex(<AnnotationRegex as TypeVertexPropertyEncoding>::from_value_bytes(value))
                    }
                    Infix::PropertyAnnotationCascade => Annotation::Cascade(AnnotationCascade),
                    Infix::PropertyAnnotationRange => {
                        Annotation::Range(<AnnotationRange as TypeVertexPropertyEncoding>::from_value_bytes(value))
                    }
                    Infix::PropertyAnnotationValues => {
                        Annotation::Values(<AnnotationValues as TypeVertexPropertyEncoding>::from_value_bytes(value))
                    }
                    | Infix::_PropertyAnnotationLast
                    | Infix::PropertyAnnotationUnique
                    | Infix::PropertyAnnotationKey
                    | Infix::PropertyLabel
                    | Infix::PropertyValueType
                    | Infix::PropertyOrdering
                    | Infix::PropertyHasOrder
                    | Infix::PropertyLinksOrder => {
                        unreachable!("Retrieved unexpected infixes while reading annotations.")
                    }
                };
                T::AnnotationType::try_from(annotation).unwrap()
            })
            .map_err(|err| Box::new(ConceptReadError::SnapshotIterate { source: err.clone() }))
    }

    pub(crate) fn get_type_constraints<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<HashSet<TypeConstraint<T>>, Box<ConceptReadError>> {
        let mut all_constraints: HashSet<TypeConstraint<T>> = HashSet::new();
        let mut type_opt = Some(type_.clone());
        while let Some(curr_type) = type_opt {
            let declared_annotations = Self::get_type_annotations_declared(snapshot, curr_type.clone())?;
            for annotation in declared_annotations {
                for constraint in annotation.clone().into().into_type_constraints(curr_type.clone()) {
                    match constraint.scope() {
                        ConstraintScope::SingleInstanceOfType => {
                            if constraint.source() != type_ {
                                continue;
                            }
                        }

                        ConstraintScope::AllInstancesOfTypeOrSubtypes => {
                            if let Some(duplicate) = all_constraints
                                .iter()
                                .find(|saved_constraint| saved_constraint.category() == constraint.category())
                            {
                                let duplicate = duplicate.clone();
                                all_constraints.remove(&duplicate);
                            }
                        }

                        ConstraintScope::SingleInstanceOfTypeOrSubtype
                        | ConstraintScope::AllInstancesOfSiblingTypeOrSubtypes => {}
                    }
                    all_constraints.insert(constraint);
                }
            }
            type_opt = Self::get_supertype(snapshot, curr_type.clone())?;
        }

        Ok(all_constraints)
    }

    pub(crate) fn get_capability_annotations_declared<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
    ) -> Result<HashSet<CAP::AnnotationType>, Box<ConceptReadError>> {
        let type_edge = capability.to_canonical_type_edge();
        snapshot
            .iterate_range(KeyRange::new_variable_width(
                RangeStart::Inclusive(
                    TypeEdgeProperty::build(type_edge.clone(), Infix::ANNOTATION_MIN).into_storage_key(),
                ),
                RangeEnd::EndPrefixInclusive(
                    TypeEdgeProperty::build(type_edge, Infix::ANNOTATION_MAX).into_storage_key(),
                ),
            ))
            .collect_cloned_hashset(|key, value| {
                let annotation_key = TypeEdgeProperty::new(Bytes::Reference(key.bytes()));
                let annotation = match annotation_key.infix() {
                    Infix::PropertyAnnotationDistinct => Annotation::Distinct(AnnotationDistinct),
                    Infix::PropertyAnnotationIndependent => Annotation::Independent(AnnotationIndependent),
                    Infix::PropertyAnnotationUnique => Annotation::Unique(AnnotationUnique),
                    Infix::PropertyAnnotationKey => Annotation::Key(AnnotationKey),
                    Infix::PropertyAnnotationCardinality => Annotation::Cardinality(
                        <AnnotationCardinality as TypeEdgePropertyEncoding>::from_value_bytes(value),
                    ),
                    Infix::PropertyAnnotationRegex => {
                        Annotation::Regex(<AnnotationRegex as TypeEdgePropertyEncoding>::from_value_bytes(value))
                    }
                    Infix::PropertyAnnotationRange => {
                        Annotation::Range(<AnnotationRange as TypeEdgePropertyEncoding>::from_value_bytes(value))
                    }
                    Infix::PropertyAnnotationValues => {
                        Annotation::Values(<AnnotationValues as TypeEdgePropertyEncoding>::from_value_bytes(value))
                    }
                    Infix::PropertyAnnotationAbstract => {
                        Annotation::Abstract(<AnnotationAbstract as TypeEdgePropertyEncoding>::from_value_bytes(value))
                    }
                    | Infix::_PropertyAnnotationLast
                    | Infix::PropertyAnnotationCascade
                    | Infix::PropertyLabel
                    | Infix::PropertyValueType
                    | Infix::PropertyOrdering
                    | Infix::PropertyHasOrder
                    | Infix::PropertyLinksOrder => {
                        unreachable!("Retrieved unexpected infixes while reading annotations.")
                    }
                };
                CAP::AnnotationType::try_from(annotation).unwrap()
            })
            .map_err(|err| Box::new(ConceptReadError::SnapshotIterate { source: err.clone() }))
    }

    pub(crate) fn get_capability_constraints<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
    ) -> Result<HashSet<CapabilityConstraint<CAP>>, Box<ConceptReadError>> {
        let mut constraints: HashSet<CapabilityConstraint<CAP>> = HashSet::new();
        let declared_annotations = Self::get_capability_annotations_declared(snapshot, capability.clone())?;

        for annotation in declared_annotations {
            for constraint in annotation.clone().into().into_capability_constraints(capability.clone()) {
                debug_assert!(!constraints.contains(&constraint));
                debug_assert!(!constraints
                    .iter()
                    .any(|existing_constraint| existing_constraint.category() == constraint.category()));
                constraints.insert(constraint);
            }
        }

        Self::add_capability_default_constraints_if_not_declared(snapshot, capability, &mut constraints)?;
        Ok(constraints)
    }

    pub(crate) fn get_type_capability_constraints<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        object_type: CAP::ObjectType,
        interface_type: CAP::InterfaceType,
    ) -> Result<HashSet<CapabilityConstraint<CAP>>, Box<ConceptReadError>> {
        let mut all_constraints: HashSet<CapabilityConstraint<CAP>> = HashSet::new();
        let object_capability_opt = TypeReader::get_capabilities::<CAP>(snapshot, object_type.clone(), false)?
            .into_iter()
            .find(|capability| capability.interface() == interface_type);
        let object_capability: CAP = if let Some(object_capability) = object_capability_opt {
            object_capability
        } else {
            return Ok(all_constraints);
        };

        let affecting_interface_types: HashSet<CAP::InterfaceType> = CAP::InterfaceType::chain_types(
            interface_type.clone(),
            TypeReader::get_supertypes_transitive(snapshot, interface_type)?,
        )
        .collect();
        let capabilities: HashSet<CAP> = TypeReader::get_capabilities(snapshot, object_type.clone(), true)?;
        let capabilities_for_interface_type: HashSet<CAP> = capabilities
            .into_iter()
            .filter(|capability| affecting_interface_types.contains(&capability.interface()))
            .collect();

        for current_capability in capabilities_for_interface_type {
            for constraint in TypeReader::get_capability_constraints(snapshot, current_capability)? {
                match constraint.scope() {
                    ConstraintScope::SingleInstanceOfType => {
                        // is checked only for source, no need to carry it
                        if constraint.source() != object_capability {
                            continue;
                        }
                    }

                    ConstraintScope::AllInstancesOfTypeOrSubtypes => {
                        if let Some(duplicate) = all_constraints
                            .iter()
                            .find(|saved_constraint| saved_constraint.category() == constraint.category())
                        {
                            let duplicate_source_supertypes =
                                TypeReader::get_supertypes_transitive(snapshot, duplicate.source().interface())?;
                            if duplicate_source_supertypes.contains(&constraint.source().interface()) {
                                let duplicate = duplicate.clone();
                                all_constraints.remove(&duplicate);
                            } else {
                                continue;
                            }
                        }
                    }

                    ConstraintScope::SingleInstanceOfTypeOrSubtype
                    | ConstraintScope::AllInstancesOfSiblingTypeOrSubtypes => {}
                }
                all_constraints.insert(constraint);
            }
        }

        Ok(all_constraints)
    }

    pub(crate) fn get_type_capabilities_constraints<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        object_type: CAP::ObjectType,
    ) -> Result<HashMap<CAP::InterfaceType, HashSet<CapabilityConstraint<CAP>>>, Box<ConceptReadError>> {
        let mut all_constraints: HashMap<CAP::InterfaceType, HashSet<CapabilityConstraint<CAP>>> = HashMap::new();
        for object_capability in TypeReader::get_capabilities::<CAP>(snapshot, object_type.clone(), false)? {
            let interface_type = object_capability.interface();
            debug_assert!(!all_constraints.contains_key(&interface_type), "Specialised are not allowed here!");
            all_constraints.insert(
                interface_type.clone(),
                Self::get_type_capability_constraints(snapshot, object_type.clone(), object_capability.interface())?,
            );
        }
        Ok(all_constraints)
    }

    fn add_capability_default_constraints_if_not_declared<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
        out_constraints: &mut HashSet<CapabilityConstraint<CAP>>,
    ) -> Result<(), Box<ConceptReadError>> {
        match CAP::KIND {
            CapabilityKind::Relates => {
                let relates = Relates::new(
                    RelationType::new(capability.canonical_from().into_vertex()),
                    RoleType::new(capability.canonical_to().into_vertex()),
                );
                let role_ordering = Self::get_type_ordering(snapshot, relates.role())?;
                let is_specialising = Self::is_relates_specialising(snapshot, relates.clone())?;

                for default_constraint in get_relates_default_constraints(capability, role_ordering, is_specialising) {
                    if !out_constraints
                        .iter()
                        .any(|set_constraint| set_constraint.category() == default_constraint.category())
                    {
                        out_constraints.insert(default_constraint);
                    }
                }
            }
            CapabilityKind::Plays => {
                for default_constraint in get_plays_default_constraints(capability) {
                    if !out_constraints
                        .iter()
                        .any(|set_constraint| set_constraint.category() == default_constraint.category())
                    {
                        out_constraints.insert(default_constraint);
                    }
                }
            }
            CapabilityKind::Owns => {
                let owns = Owns::new(
                    ObjectType::new(capability.canonical_from().into_vertex()),
                    AttributeType::new(capability.canonical_to().into_vertex()),
                );
                let ordering = Self::get_capability_ordering(snapshot, owns.clone())?;

                for default_constraint in get_owns_default_constraints(capability, ordering) {
                    if !out_constraints
                        .iter()
                        .any(|set_constraint| set_constraint.category() == default_constraint.category())
                    {
                        out_constraints.insert(default_constraint);
                    }
                }
            }
        }
        Ok(())
    }
}
