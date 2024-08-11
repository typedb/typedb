/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet, VecDeque};

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
        },
    },
    layout::infix::Infix,
    value::{label::Label, value_type::ValueType},
    Keyable,
};
use iterator::Collector;
use lending_iterator::LendingIterator;
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::{
            Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCascade, AnnotationDistinct,
            AnnotationIndependent, AnnotationKey, AnnotationRange, AnnotationRegex, AnnotationUnique, AnnotationValues,
        },
        attribute_type::AttributeType,
        entity_type::EntityType,
        object_type::ObjectType,
        owns::Owns,
        relates::Relates,
        relation_type::RelationType,
        role_type::RoleType,
        sub::Sub,
        type_manager::validation::annotation_compatibility::is_annotation_inheritable,
        Capability, EdgeOverride, KindAPI, Ordering, TypeAPI,
    },
};

pub struct TypeReader {}

impl TypeReader {
    pub(crate) fn get_labelled_type<T>(
        snapshot: &impl ReadableSnapshot,
        label: &Label<'_>,
    ) -> Result<Option<T>, ConceptReadError>
    where
        T: TypeAPI<'static>,
    {
        let key = LabelToTypeVertexIndex::build(label).into_storage_key();
        match snapshot.get::<BUFFER_KEY_INLINE>(key.as_reference()) {
            Err(error) => Err(ConceptReadError::SnapshotGet { source: error }),
            Ok(None) => Ok(None),
            Ok(Some(value)) => match T::from_bytes(Bytes::Array(value)) {
                Ok(type_) => Ok(Some(type_)),
                Err(err) => match err {
                    EncodingError::UnexpectedPrefix { .. } => Ok(None),
                    _ => Err(ConceptReadError::Encoding { source: err }),
                },
            },
        }
    }

    pub(crate) fn get_roles_by_name(
        snapshot: &impl ReadableSnapshot,
        name: String,
    ) -> Result<Vec<RoleType<'static>>, ConceptReadError> {
        let mut name_with_colon = name;
        name_with_colon.push(':');
        let key = LabelToTypeVertexIndex::build(&Label::build(name_with_colon.as_str())).into_storage_key();
        let vec = snapshot
            .iterate_range(KeyRange::new_within(key, IdentifierIndex::<TypeVertex<'static>>::FIXED_WIDTH_ENCODING))
            .collect_cloned_vec(|key, value| match RoleType::from_bytes(Bytes::copy(value.bytes())) {
                Err(_) => None,
                Ok(role_type) => Some(role_type),
            })
            .map_err(|source| ConceptReadError::SnapshotIterate { source })?;
        Ok(vec.into_iter().filter_map(|x| x).collect())
    }

    pub(crate) fn get_struct_definition_key(
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<Option<DefinitionKey<'static>>, ConceptReadError> {
        let index_key = NameToStructDefinitionIndex::build(name);
        let bytes = snapshot
            .get(index_key.into_storage_key().as_reference())
            .map_err(|source| ConceptReadError::SnapshotGet { source })?;
        Ok(bytes.map(|value| DefinitionKey::new(Bytes::Array(value))))
    }

    pub(crate) fn get_struct_definition(
        snapshot: &impl ReadableSnapshot,
        definition_key: DefinitionKey<'_>,
    ) -> Result<StructDefinition, ConceptReadError> {
        let bytes = snapshot
            .get::<BUFFER_VALUE_INLINE>(definition_key.clone().into_storage_key().as_reference())
            .map_err(|source| ConceptReadError::SnapshotGet { source })?;
        Ok(StructDefinition::from_bytes(bytes.unwrap().as_ref()))
    }

    pub(crate) fn get_struct_definitions_all(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<HashMap<DefinitionKey<'static>, StructDefinition>, ConceptReadError> {
        snapshot
            .iterate_range(KeyRange::new_within(
                DefinitionKey::build_prefix(StructDefinition::PREFIX),
                StructDefinition::PREFIX.fixed_width_keys(),
            ))
            .collect_cloned_hashmap(|key, value| {
                (DefinitionKey::new(Bytes::Array(key.byte_ref().into())), StructDefinition::from_bytes(value))
            })
            .map_err(|source| ConceptReadError::SnapshotIterate { source })
    }

    pub(crate) fn get_struct_definition_usages_in_attribute_types(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<HashMap<DefinitionKey<'static>, HashSet<AttributeType<'static>>>, ConceptReadError> {
        let mut usages: HashMap<DefinitionKey<'static>, HashSet<AttributeType<'static>>> = HashMap::new();

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
    ) -> Result<HashMap<DefinitionKey<'static>, HashSet<DefinitionKey<'static>>>, ConceptReadError> {
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
    ) -> Result<Vec<ObjectType<'static>>, ConceptReadError> {
        let entity_types = Self::get_entity_types(snapshot)?;
        let relation_types = Self::get_relation_types(snapshot)?;
        Ok((entity_types.into_iter().map(ObjectType::Entity))
            .chain(relation_types.into_iter().map(ObjectType::Relation))
            .collect())
    }

    pub(crate) fn get_entity_types(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Vec<EntityType<'static>>, ConceptReadError> {
        snapshot
            .iterate_range(KeyRange::new_within(EntityType::prefix_for_kind(), EntityType::PREFIX.fixed_width_keys()))
            .collect_cloned_vec(|key, _| EntityType::new(TypeVertex::new(Bytes::copy(key.bytes()))))
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn get_relation_types(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Vec<RelationType<'static>>, ConceptReadError> {
        snapshot
            .iterate_range(KeyRange::new_within(
                RelationType::prefix_for_kind(),
                RelationType::PREFIX.fixed_width_keys(),
            ))
            .collect_cloned_vec(|key, _| RelationType::new(TypeVertex::new(Bytes::copy(key.bytes()))))
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn get_attribute_types(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Vec<AttributeType<'static>>, ConceptReadError> {
        snapshot
            .iterate_range(KeyRange::new_within(AttributeType::prefix_for_kind(), false))
            .collect_cloned_vec(|key, _| AttributeType::new(TypeVertex::new(Bytes::copy(key.bytes()))))
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn get_role_types(snapshot: &impl ReadableSnapshot) -> Result<Vec<RoleType<'static>>, ConceptReadError> {
        snapshot
            .iterate_range(KeyRange::new_within(RoleType::prefix_for_kind(), RoleType::PREFIX.fixed_width_keys()))
            .collect_cloned_vec(|key, _| RoleType::new(TypeVertex::new(Bytes::copy(key.bytes()))))
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    // TODO: Should get_{super/sub}type[s_transitive] return T or T::SelfStatic.
    // T::SelfStatic is the more consistent, more honest interface, but T is convenient.
    pub(crate) fn get_supertype<T>(snapshot: &impl ReadableSnapshot, subtype: T) -> Result<Option<T>, ConceptReadError>
    where
        T: TypeAPI<'static>,
    {
        Ok(snapshot
            .iterate_range(KeyRange::new_within(
                Sub::prefix_for_canonical_edges_from(subtype),
                TypeEdge::FIXED_WIDTH_ENCODING,
            ))
            .first_cloned()
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })?
            .map(|(key, _)| Sub::<T>::decode_canonical_edge(Bytes::Array(key.into_byte_array())).supertype()))
    }

    pub(crate) fn get_supertypes_transitive<T>(
        snapshot: &impl ReadableSnapshot,
        subtype: T,
    ) -> Result<Vec<T>, ConceptReadError>
    where
        T: TypeAPI<'static>,
    {
        // supertypes do NOT include themselves by design
        let mut supertypes: Vec<T> = Vec::new();
        let mut supertype_opt = TypeReader::get_supertype(snapshot, subtype.clone())?;
        while let Some(supertype) = supertype_opt {
            supertypes.push(supertype.clone());
            supertype_opt = TypeReader::get_supertype(snapshot, supertype.clone())?;
        }
        Ok(supertypes)
    }

    pub(crate) fn get_subtypes<T>(snapshot: &impl ReadableSnapshot, supertype: T) -> Result<Vec<T>, ConceptReadError>
    where
        T: TypeAPI<'static>,
    {
        snapshot
            .iterate_range(KeyRange::new_within(
                Sub::prefix_for_reverse_edges_from(supertype),
                TypeEdge::FIXED_WIDTH_ENCODING,
            ))
            .collect_cloned_vec(|key, _| {
                Sub::<T>::decode_reverse_edge(Bytes::Reference(key.byte_ref()).into_owned()).subtype()
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub fn get_subtypes_transitive<T>(snapshot: &impl ReadableSnapshot, subtype: T) -> Result<Vec<T>, ConceptReadError>
    where
        T: TypeAPI<'static>,
    {
        //subtypes DO NOT include themselves by design
        let mut subtypes: Vec<T> = Vec::new();
        let mut stack = TypeReader::get_subtypes(snapshot, subtype.clone())?;
        while let Some(subtype) = stack.pop() {
            subtypes.push(subtype.clone());
            stack.append(&mut TypeReader::get_subtypes(snapshot, subtype)?);
            // TODO: Should we pass an accumulator instead?
        }
        Ok(subtypes)
    }

    pub(crate) fn get_label<'a>(
        snapshot: &impl ReadableSnapshot,
        type_: impl TypeAPI<'a>,
    ) -> Result<Option<Label<'static>>, ConceptReadError> {
        Self::get_type_property_declared::<Label<'static>>(snapshot, type_)
    }

    pub(crate) fn get_capabilities_declared<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        owner: impl TypeVertexEncoding<'static>,
    ) -> Result<HashSet<CAP>, ConceptReadError> {
        let owns_prefix = CAP::prefix_for_canonical_edges_from(CAP::ObjectType::new(owner.into_vertex()));
        snapshot
            .iterate_range(KeyRange::new_within(owns_prefix, TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| CAP::decode_canonical_edge(Bytes::Reference(key.byte_ref()).into_owned()))
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn get_capabilities<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        object_type: CAP::ObjectType,
    ) -> Result<HashSet<CAP>, ConceptReadError> {
        let mut transitive_capabilities: HashSet<CAP> = HashSet::new();
        let mut overridden_interfaces: HashSet<CAP::InterfaceType> = HashSet::new();
        let mut saved_interfaces: HashSet<CAP::InterfaceType> = HashSet::new();
        let mut current_type = Some(object_type);
        while current_type.is_some() {
            let declared_capabilities =
                Self::get_capabilities_declared::<CAP>(snapshot, current_type.as_ref().unwrap().clone())?;
            for capability in declared_capabilities.into_iter() {
                let interface = capability.interface();
                // If interface capability is redeclared by a subtype, we don't inherit the original one.
                if !overridden_interfaces.contains(&interface) && !saved_interfaces.contains(&interface) {
                    transitive_capabilities.insert(capability.clone());
                    saved_interfaces.insert(interface);
                }
                if let Some(overridden) = Self::get_capability_override(snapshot, capability.clone())? {
                    overridden_interfaces.add(overridden.interface());
                }
            }
            current_type = Self::get_supertype(snapshot, current_type.unwrap())?;
        }
        Ok(transitive_capabilities)
    }

    pub(crate) fn get_object_capabilities_overrides_declared<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        object_type: CAP::ObjectType,
    ) -> Result<HashMap<CAP, CAP>, ConceptReadError> {
        let mut capability_to_overridden: HashMap<CAP, CAP> = HashMap::new();
        let declared_capabilities = Self::get_capabilities_declared::<CAP>(snapshot, object_type)?;
        for capability in declared_capabilities.into_iter() {
            debug_assert!(!capability_to_overridden.contains_key(&capability));
            if let Some(overridden) = Self::get_capability_override(snapshot, capability.clone())? {
                capability_to_overridden.insert(capability, overridden);
            }
        }
        Ok(capability_to_overridden)
    }

    pub(crate) fn get_object_capabilities_overrides<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        object_type: CAP::ObjectType,
    ) -> Result<HashMap<CAP, CAP>, ConceptReadError> {
        let mut capability_to_overridden: HashMap<CAP, CAP> = HashMap::new();
        let mut current_type = Some(object_type);
        while let Some(current_type_val) = &current_type {
            let current_type_capability_to_overridden =
                Self::get_object_capabilities_overrides_declared(snapshot, current_type_val.clone())?;
            capability_to_overridden.extend(current_type_capability_to_overridden.into_iter());
            current_type = Self::get_supertype(snapshot, current_type_val.clone())?;
        }
        Ok(capability_to_overridden)
    }

    pub(crate) fn get_capability_override<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
    ) -> Result<Option<CAP>, ConceptReadError> {
        let override_property_key = EdgeOverride::<CAP>::build_key(capability);
        snapshot
            .get_mapped(override_property_key.into_storage_key().as_reference(), |overridden_edge_bytes| {
                EdgeOverride::<CAP>::from_value_bytes(overridden_edge_bytes).overrides
            })
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    pub(crate) fn get_overriding_capabilities<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
    ) -> Result<Vec<CAP>, ConceptReadError> {
        let mut overriding_capabilities: Vec<CAP> = Vec::new();
        let mut object_types: VecDeque<CAP::ObjectType> = VecDeque::from([capability.object()]);

        while let Some(current_object_type) = object_types.pop_back() {
            let capability_to_overridden =
                Self::get_object_capabilities_overrides_declared::<CAP>(snapshot, current_object_type.clone())?;

            let old_len = overriding_capabilities.len();
            capability_to_overridden
                .into_iter()
                .filter(|(_, overridden)| overridden == &capability)
                .for_each(|(overriding, _)| overriding_capabilities.push(overriding));

            let capability_overridden_for_type = old_len < overriding_capabilities.len();
            if !capability_overridden_for_type {
                let subtypes = Self::get_subtypes(snapshot, current_object_type)?;
                subtypes.into_iter().for_each(|subtype| object_types.push_front(subtype));
            }
        }

        Ok(overriding_capabilities)
    }

    pub(crate) fn get_overriding_capabilities_transitive<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
    ) -> Result<Vec<CAP>, ConceptReadError> {
        let mut overriding_capabilities: Vec<CAP> = Vec::new();
        let mut object_types_and_capabilities: VecDeque<(CAP::ObjectType, CAP)> =
            VecDeque::from([(capability.object(), capability)]);

        while let Some((current_object_type, capability_to_check)) = object_types_and_capabilities.pop_back() {
            let capability_to_overridden =
                Self::get_object_capabilities_overrides_declared::<CAP>(snapshot, current_object_type.clone())?;

            let mut current_overriding_capabilities: Vec<CAP> = capability_to_overridden
                .into_iter()
                .filter(|(_, overridden)| overridden == &capability_to_check)
                .map(|(overriding, _)| overriding)
                .collect();

            if current_overriding_capabilities.is_empty() {
                current_overriding_capabilities.push(capability_to_check.clone());
            } else {
                overriding_capabilities.extend(current_overriding_capabilities.clone());
            }

            let subtypes = Self::get_subtypes(snapshot, current_object_type)?;
            for subtype in subtypes {
                current_overriding_capabilities.iter().cloned().for_each(|overriding_capability| {
                    object_types_and_capabilities.push_front((subtype.clone(), overriding_capability))
                });
            }
        }

        Ok(overriding_capabilities)
    }

    pub(crate) fn get_capabilities_for_interface_declared<CAP>(
        snapshot: &impl ReadableSnapshot,
        interface_type: CAP::InterfaceType,
    ) -> Result<HashSet<CAP>, ConceptReadError>
    where
        CAP: Capability<'static>,
    {
        let owns_prefix = CAP::prefix_for_reverse_edges_from(interface_type);
        snapshot
            .iterate_range(KeyRange::new_within(owns_prefix, TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| CAP::decode_reverse_edge(Bytes::Array(key.byte_ref().into())))
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn get_capabilities_for_interface<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        interface_type: CAP::InterfaceType,
    ) -> Result<HashMap<CAP::ObjectType, CAP>, ConceptReadError> {
        let mut capabilities: HashMap<CAP::ObjectType, CAP> = HashMap::new();
        let capabilities_declared: HashSet<CAP> =
            Self::get_capabilities_for_interface_declared(snapshot, interface_type.clone())?;

        for declared_capability in capabilities_declared {
            let mut stack = Vec::new();
            stack.push(declared_capability.object());
            while let Some(sub_object) = stack.pop() {
                let mut declared_capability_was_overridden = false;
                for sub_object_cap in Self::get_capabilities_declared::<CAP>(snapshot, sub_object.clone())? {
                    if let Some(overridden_cap) = Self::get_capability_override(snapshot, sub_object_cap.clone())? {
                        declared_capability_was_overridden =
                            declared_capability_was_overridden || overridden_cap.interface() == interface_type;
                    }
                }
                if !declared_capability_was_overridden {
                    debug_assert!(!capabilities.contains_key(&sub_object));
                    capabilities.insert(sub_object.clone(), declared_capability.clone());
                    Self::get_subtypes(snapshot, sub_object)?
                        .into_iter()
                        .for_each(|object_type| stack.push(object_type));
                }
            }
        }
        Ok(capabilities)
    }

    pub(crate) fn get_role_type_relates_declared(
        snapshot: &impl ReadableSnapshot,
        role: RoleType<'static>,
    ) -> Result<Relates<'static>, ConceptReadError> {
        let relates = Self::get_capabilities_for_interface_declared::<Relates<'static>>(snapshot, role)?;
        debug_assert!(relates.len() == 1);
        (relates.iter().next().map(|relates| relates.to_owned()))
            .ok_or(ConceptReadError::CorruptMissingMandatoryRelatesForRole)
    }

    pub(crate) fn get_role_type_relates(
        snapshot: &impl ReadableSnapshot,
        role: RoleType<'static>,
    ) -> Result<HashMap<RelationType<'static>, Relates<'static>>, ConceptReadError> {
        let relates_immediate = Self::get_role_type_relates_declared(snapshot, role.clone())?;

        let mut role_overriders: HashSet<RelationType<'static>> = HashSet::new();
        for subrole in Self::get_subtypes_transitive(snapshot, role.clone())? {
            role_overriders.insert(Self::get_role_type_relates_declared(snapshot, subrole)?.relation());
        }

        let mut relates_transitive: HashMap<RelationType<'static>, Relates<'static>> = HashMap::new();
        relates_transitive.insert(relates_immediate.relation(), relates_immediate.clone());
        let mut stack = TypeReader::get_subtypes(snapshot, relates_immediate.relation())?;
        while let Some(subtype) = stack.pop() {
            if !role_overriders.contains(&subtype) {
                relates_transitive.insert(subtype.clone(), Relates::new(subtype.clone(), role.clone()));
                stack.append(&mut TypeReader::get_subtypes(snapshot, subtype)?);
            }
        }
        Ok(relates_transitive)
    }

    pub(crate) fn get_value_type_declared(
        snapshot: &impl ReadableSnapshot,
        type_: AttributeType<'_>,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        Self::get_type_property_declared::<ValueType>(snapshot, type_)
    }

    pub(crate) fn get_value_type(
        snapshot: &impl ReadableSnapshot,
        type_: AttributeType<'static>,
    ) -> Result<Option<(ValueType, AttributeType<'static>)>, ConceptReadError> {
        Self::get_type_property::<ValueType, AttributeType<'static>>(snapshot, type_)
    }

    pub(crate) fn get_value_type_without_source(
        snapshot: &impl ReadableSnapshot,
        type_: AttributeType<'static>,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        Self::get_value_type(snapshot, type_).map(|result| result.map(|(value_type, _)| value_type))
    }

    pub(crate) fn get_type_property_declared<'a, PROPERTY>(
        snapshot: &impl ReadableSnapshot,
        type_: impl TypeVertexEncoding<'a>,
    ) -> Result<Option<PROPERTY>, ConceptReadError>
    where
        PROPERTY: TypeVertexPropertyEncoding<'static>,
    {
        let property = snapshot
            .get_mapped(PROPERTY::build_key(type_).into_storage_key().as_reference(), |value| {
                PROPERTY::from_value_bytes(value)
            })
            .map_err(|err| ConceptReadError::SnapshotGet { source: err })?;
        Ok(property)
    }

    pub(crate) fn get_type_property<PROPERTY, SOURCE>(
        snapshot: &impl ReadableSnapshot,
        type_: SOURCE,
    ) -> Result<Option<(PROPERTY, SOURCE)>, ConceptReadError>
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

    pub(crate) fn get_type_ordering(
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType<'_>,
    ) -> Result<Ordering, ConceptReadError> {
        match Self::get_type_property_declared(snapshot, role_type)? {
            Some(ordering) => Ok(ordering),
            None => Err(ConceptReadError::CorruptMissingMandatoryProperty),
        }
    }

    pub(crate) fn get_type_annotations_declared<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<HashSet<T::AnnotationType>, ConceptReadError> {
        snapshot
            .iterate_range(KeyRange::new_inclusive(
                TypeVertexProperty::build(type_.vertex(), Infix::ANNOTATION_MIN).into_storage_key(),
                TypeVertexProperty::build(type_.vertex(), Infix::ANNOTATION_MAX).into_storage_key(),
            ))
            .collect_cloned_hashset(|key, value| {
                let annotation_key = TypeVertexProperty::new(Bytes::Reference(key.byte_ref()));
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
                    | Infix::PropertyOverride
                    | Infix::PropertyHasOrder
                    | Infix::PropertyLinksOrder => {
                        unreachable!("Retrieved unexpected infixes while reading annotations.")
                    }
                };
                T::AnnotationType::try_from(annotation).unwrap()
            })
            .map_err(|err| ConceptReadError::SnapshotIterate { source: err.clone() })
    }

    pub(crate) fn get_type_annotations<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<HashMap<T::AnnotationType, T>, ConceptReadError> {
        let mut annotations: HashMap<T::AnnotationType, T> = HashMap::new();
        let mut type_opt = Some(type_);
        let mut declared = true;
        while let Some(curr_type) = type_opt {
            let declared_annotations = Self::get_type_annotations_declared(snapshot, curr_type.clone())?;
            for annotation in declared_annotations {
                if declared || is_annotation_inheritable(&annotation, &annotations) {
                    annotations.insert(annotation, curr_type.clone());
                }
            }
            type_opt = Self::get_supertype(snapshot, curr_type.clone())?;
            declared = false;
        }
        Ok(annotations)
    }

    // TODO: this is currently breaking our architectural pattern that none of the Manager methods should operate graphs
    pub(crate) fn get_type_edge_annotations_declared<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        edge: CAP,
    ) -> Result<HashSet<CAP::AnnotationType>, ConceptReadError> {
        let type_edge = edge.to_canonical_type_edge();
        snapshot
            .iterate_range(KeyRange::new_inclusive(
                TypeEdgeProperty::build(type_edge.clone(), Infix::ANNOTATION_MIN).into_storage_key(),
                TypeEdgeProperty::build(type_edge, Infix::ANNOTATION_MAX).into_storage_key(),
            ))
            .collect_cloned_hashset(|key, value| {
                let annotation_key = TypeEdgeProperty::new(Bytes::Reference(key.byte_ref()));
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
                    | Infix::_PropertyAnnotationLast
                    | Infix::PropertyAnnotationAbstract
                    | Infix::PropertyAnnotationCascade
                    | Infix::PropertyLabel
                    | Infix::PropertyValueType
                    | Infix::PropertyOrdering
                    | Infix::PropertyOverride
                    | Infix::PropertyHasOrder
                    | Infix::PropertyLinksOrder => {
                        unreachable!("Retrieved unexpected infixes while reading annotations.")
                    }
                };
                CAP::AnnotationType::from(annotation)
            })
            .map_err(|err| ConceptReadError::SnapshotIterate { source: err.clone() })
    }

    pub(crate) fn get_type_edge_annotations<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        edge: CAP,
    ) -> Result<HashMap<CAP::AnnotationType, CAP>, ConceptReadError> {
        let mut annotations: HashMap<CAP::AnnotationType, CAP> = HashMap::new();
        let mut edge_opt = Some(edge);
        let mut declared = true;
        while let Some(edge) = edge_opt {
            let declared_edge_annotations = Self::get_type_edge_annotations_declared(snapshot, edge.clone())?;
            for annotation in declared_edge_annotations {
                if declared || is_annotation_inheritable(&annotation, &annotations) {
                    annotations.insert(annotation, edge.clone());
                }
            }
            edge_opt = Self::get_capability_override(snapshot, edge.clone())?;
            declared = false;
        }
        Ok(annotations)
    }

    pub(crate) fn get_type_edge_ordering(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'_>,
    ) -> Result<Ordering, ConceptReadError> {
        match Self::get_type_edge_property::<Ordering>(snapshot, owns)? {
            Some(ordering) => Ok(ordering),
            None => Err(ConceptReadError::CorruptMissingMandatoryProperty),
        }
    }

    pub(crate) fn get_type_edge_property<'a, PROPERTY>(
        snapshot: &impl ReadableSnapshot,
        edge: impl TypeEdgeEncoding<'a>,
    ) -> Result<Option<PROPERTY>, ConceptReadError>
    where
        PROPERTY: TypeEdgePropertyEncoding<'static>,
    {
        let property = snapshot
            .get_mapped(PROPERTY::build_key(edge).into_storage_key().as_reference(), |value| {
                PROPERTY::from_value_bytes(value)
            })
            .map_err(|err| ConceptReadError::SnapshotGet { source: err })?;
        Ok(property)
    }
}
