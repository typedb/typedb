/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
};

use encoding::{
    graph::{
        definition::definition_key::DefinitionKey,
        thing::{edge::ThingEdgeLinks, ThingVertex},
        type_::{CapabilityKind, Kind},
    },
    value::{label::Label, value_type::ValueType},
};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::{
    error::ConceptReadError,
    thing::{
        object::ObjectAPI,
        relation::{RolePlayerIterator},
        thing_manager::ThingManager,
    },
    type_::{
        annotation::{
            Annotation, AnnotationCardinality, AnnotationCategory, AnnotationRange, AnnotationRegex, AnnotationUnique,
            AnnotationValues,
        },
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        entity_type::EntityType,
        object_type::{with_object_type, ObjectType},
        owns::Owns,
        plays::Plays,
        relates::Relates,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::{
            type_reader::TypeReader,
            validation::{
                validation::{
                    edge_get_annotation_by_category, edge_get_owner_of_annotation_category,
                    get_label_or_concept_read_err, get_label_or_schema_err, is_interface_hidden_by_overrides,
                    is_ordering_compatible_with_distinct_annotation,
                    is_overridden_interface_object_one_of_supertypes_or_self, type_get_annotation_by_category,
                    type_get_owner_of_annotation_category, type_has_annotation_category,
                    validate_cardinality_narrows_inherited_cardinality,
                    validate_declared_annotation_is_compatible_with_other_inherited_annotations,
                    validate_declared_edge_annotation_is_compatible_with_other_inherited_annotations,
                    validate_edge_annotations_narrowing_of_inherited_annotations,
                    validate_edge_override_ordering_match, validate_edge_range_narrows_inherited_range,
                    validate_edge_regex_narrows_inherited_regex, validate_edge_values_narrows_inherited_values,
                    validate_key_narrows_inherited_cardinality, validate_role_name_uniqueness_non_transitive,
                    validate_type_annotations_narrowing_of_inherited_annotations,
                    validate_type_range_narrows_inherited_range, validate_type_regex_narrows_inherited_regex,
                    validate_type_supertype_abstractness, validate_type_supertype_ordering_match,
                    validate_type_values_narrows_inherited_values,
                },
                SchemaValidationError,
            },
            TypeManager,
        },
        Capability, KindAPI, ObjectTypeAPI, Ordering, OwnerAPI, TypeAPI,
    },
};
use crate::thing::ThingAPI;

macro_rules! object_type_match {
    ($obj_var:ident, $block:block) => {
        match &$obj_var {
            ObjectType::Entity($obj_var) => $block
            ObjectType::Relation($obj_var) => $block
        }
    };
}

pub struct OperationTimeValidation {}

macro_rules! type_or_subtype_capabilities_instances_existence_validation {
    ($func_name:ident, $object_type:ident, $interface_type:ident, $single_type_validation_func:path) => {
        fn $func_name<'a>(
            snapshot: &impl ReadableSnapshot,
            type_manager: &'a TypeManager,
            thing_manager: &'a ThingManager,
            object_type: $object_type<'a>,
            interface_type: $interface_type<'a>,
        ) -> Result<Option<$object_type<'a>>, ConceptReadError> {
            let mut type_that_has_instances = None;

            let mut object_types = VecDeque::new();
            object_types.push_front(object_type);

            while let Some(current_object_type) = object_types.pop_back() {
                if $single_type_validation_func(
                    snapshot,
                    thing_manager,
                    current_object_type.clone(),
                    interface_type.clone(),
                )? {
                    type_that_has_instances = Some(current_object_type.clone());
                    break; // TODO: Maybe we want to return all the corrupted owns here, just moving forward for now
                }

                current_object_type
                    .get_subtypes(snapshot, type_manager)?
                    .iter()
                    .for_each(|subtype| object_types.push_front(subtype.clone()));
            }

            Ok(type_that_has_instances)
        }
    };
}

macro_rules! cannot_unset_capability_with_existing_instances_validation {
    ($func_name:ident, $capability_kind:path, $capability_type:ident, $object_type:ident, $interface_type:ident, $existing_instances_validation_func:path) => {
        pub(crate) fn $func_name<'a>(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            object_type: $object_type<'a>,
            interface_type: $interface_type<'a>,
        ) -> Result<(), SchemaValidationError> {
            if let Some(supertype) = TypeReader::get_supertype(snapshot, object_type.clone().into_owned())
                .map_err(SchemaValidationError::ConceptRead)?
            {
                let supertype_capabilities =
                    TypeReader::get_capabilities::<$capability_type<'static>>(snapshot, supertype)
                        .map_err(SchemaValidationError::ConceptRead)?;
                if supertype_capabilities.contains_key(&interface_type) {
                    return Ok(());
                }
            }

            let type_having_instances = $existing_instances_validation_func(
                snapshot,
                type_manager,
                thing_manager,
                object_type.clone(),
                interface_type.clone(),
            )
            .map_err(SchemaValidationError::ConceptRead)?;

            if let Some(type_having_instances) = type_having_instances {
                Err(SchemaValidationError::CannotUnsetCapabilityWithExistingInstances(
                    $capability_kind,
                    get_label_or_schema_err(snapshot, object_type)?,
                    get_label_or_schema_err(snapshot, type_having_instances)?,
                    get_label_or_schema_err(snapshot, interface_type)?,
                ))
            } else {
                Ok(())
            }
        }
    };
}

macro_rules! cannot_override_capability_with_existing_instances_validation {
    ($func_name:ident, $capability_kind:path, $object_type:ident, $interface_type:ident, $existing_instances_validation_func:path) => {
        pub(crate) fn $func_name<'a>(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            object_type: $object_type<'a>,
            interface_type: $interface_type<'a>,
        ) -> Result<(), SchemaValidationError> {
            let type_having_instances = $existing_instances_validation_func(
                snapshot,
                type_manager,
                thing_manager,
                object_type.clone(),
                interface_type.clone(),
            )
            .map_err(SchemaValidationError::ConceptRead)?;

            if let Some(type_having_instances) = type_having_instances {
                Err(SchemaValidationError::CannotOverrideCapabilityWithExistingInstances(
                    $capability_kind,
                    get_label_or_schema_err(snapshot, object_type)?,
                    get_label_or_schema_err(snapshot, type_having_instances)?,
                    get_label_or_schema_err(snapshot, interface_type)?,
                ))
            } else {
                Ok(())
            }
        }
    };
}

macro_rules! cannot_change_supertype_as_capability_with_existing_instances_is_lost_validation {
    ($func_name:ident, $capability_kind:path, $object_type:ident, $get_lost_capabilities_func:path, $existing_instances_validation_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
            subtype: $object_type<'static>,
            supertype: $object_type<'static>,
        ) -> Result<(), SchemaValidationError> {
            let lost_capabilities = $get_lost_capabilities_func(snapshot, subtype.clone(), supertype.clone())?;

            for capability in lost_capabilities {
                let interface_type = capability.interface();
                let type_having_instances = $existing_instances_validation_func(
                    snapshot,
                    type_manager,
                    thing_manager,
                    subtype.clone(),
                    interface_type.clone(),
                )
                .map_err(SchemaValidationError::ConceptRead)?;

                if let Some(type_having_instances) = type_having_instances {
                    return Err(SchemaValidationError::CannotChangeSupertypeAsCapabilityIsLostWhileHavingHasInstances(
                        $capability_kind,
                        get_label_or_schema_err(snapshot, subtype)?,
                        get_label_or_schema_err(snapshot, supertype)?,
                        get_label_or_schema_err(snapshot, type_having_instances)?,
                        get_label_or_schema_err(snapshot, interface_type)?,
                    ));
                }
            }

            Ok(())
        }
    };
}

impl OperationTimeValidation {
    pub(crate) fn validate_type_exists(
        snapshot: &impl ReadableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), SchemaValidationError> {
        TypeReader::get_label(snapshot, type_).map_err(SchemaValidationError::ConceptRead)?;
        Ok(())
    }

    pub(crate) fn validate_can_modify_type<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<(), SchemaValidationError> {
        let label = TypeReader::get_label(snapshot, type_).map_err(SchemaValidationError::ConceptRead)?.unwrap();
        Ok(())
    }

    pub(crate) fn validate_no_subtypes_for_type_deletion<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<(), SchemaValidationError>
    where
        T: KindAPI<'static>,
    {
        let no_subtypes =
            TypeReader::get_subtypes(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?.is_empty();
        if no_subtypes {
            Ok(())
        } else {
            Err(SchemaValidationError::CannotDeleteTypeWithExistingSubtypes(get_label_or_schema_err(snapshot, type_)?))
        }
    }

    pub(crate) fn validate_no_subtypes_for_type_abstractness_unset(
        snapshot: &impl ReadableSnapshot,
        type_: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let no_subtypes =
            TypeReader::get_subtypes(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?.is_empty();
        if no_subtypes {
            Ok(())
        } else {
            Err(SchemaValidationError::CannotUnsetAbstractnessOfAttributeTypeAsItHasSubtypes(get_label_or_schema_err(
                snapshot, type_,
            )?))
        }
    }

    pub(crate) fn validate_no_capabilities_with_abstract_interfaces_to_unset_abstractness<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: impl KindAPI<'static>,
    ) -> Result<(), SchemaValidationError> {
        TypeReader::get_capabilities_declared::<CAP>(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .iter()
            .map(CAP::interface)
            .try_for_each(|interface_type| {
                if interface_type.is_abstract(snapshot, type_manager).map_err(SchemaValidationError::ConceptRead)? {
                    Err(SchemaValidationError::CannotUnsetAbstractnessAsItHasDeclaredCapabilityOfAbstractInterface(
                        CAP::KIND,
                        get_label_or_schema_err(snapshot, type_.clone())?,
                        get_label_or_schema_err(snapshot, interface_type)?,
                    ))
                } else {
                    Ok(())
                }
            })?;

        Ok(())
    }

    pub(crate) fn validate_label_uniqueness(
        snapshot: &impl ReadableSnapshot,
        new_label: &Label<'static>,
    ) -> Result<(), SchemaValidationError> {
        if TypeReader::get_labelled_type::<AttributeType<'static>>(snapshot, new_label)
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some()
        {
            Err(SchemaValidationError::LabelShouldBeUnique { label: new_label.clone(), existing_kind: Kind::Attribute })
        } else if TypeReader::get_labelled_type::<RelationType<'static>>(snapshot, new_label)
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some()
        {
            Err(SchemaValidationError::LabelShouldBeUnique { label: new_label.clone(), existing_kind: Kind::Relation })
        } else if TypeReader::get_labelled_type::<EntityType<'static>>(snapshot, new_label)
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some()
        {
            Err(SchemaValidationError::LabelShouldBeUnique { label: new_label.clone(), existing_kind: Kind::Entity })
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_new_role_name_uniqueness(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        label: &Label<'static>,
    ) -> Result<(), SchemaValidationError> {
        let existing_relation_supertypes = TypeReader::get_supertypes(snapshot, relation_type.clone().into_owned())
            .map_err(SchemaValidationError::ConceptRead)?;

        validate_role_name_uniqueness_non_transitive(snapshot, relation_type, label)?;
        for relation_supertype in existing_relation_supertypes {
            validate_role_name_uniqueness_non_transitive(snapshot, relation_supertype, label)?;
        }

        Ok(())
    }

    pub(crate) fn validate_roles_compatible_with_new_relation_supertype(
        snapshot: &impl ReadableSnapshot,
        relation_subtype: RelationType<'static>,
        relation_supertype: RelationType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let subtype_relates_declared =
            TypeReader::get_capabilities_declared::<Relates<'static>>(snapshot, relation_subtype)
                .map_err(SchemaValidationError::ConceptRead)?;

        for subtype_relates in subtype_relates_declared {
            let role = subtype_relates.role();
            let role_label = get_label_or_schema_err(snapshot, role)?;
            validate_role_name_uniqueness_non_transitive(snapshot, relation_supertype.clone(), &role_label)?;

            let relation_supertype_supertypes =
                TypeReader::get_supertypes(snapshot, relation_supertype.clone().into_owned())
                    .map_err(SchemaValidationError::ConceptRead)?;
            for supertype in relation_supertype_supertypes {
                validate_role_name_uniqueness_non_transitive(snapshot, supertype, &role_label)?;
            }
        }

        Ok(())
    }

    pub(crate) fn validate_relates_overrides_compatible_with_new_supertype(
        snapshot: &impl ReadableSnapshot,
        relation_subtype: RelationType<'static>,
        relation_supertype: RelationType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let supertype_relates_with_roles: HashMap<RoleType<'static>, Relates<'static>> =
            TypeReader::get_capabilities(snapshot, relation_supertype.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

        let subtype_relates_declared: HashSet<Relates<'static>> =
            TypeReader::get_capabilities_declared(snapshot, relation_subtype.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

        for subtype_relates in subtype_relates_declared {
            if let Some(old_relates_override) = TypeReader::get_capability_override(snapshot, subtype_relates)
                .map_err(SchemaValidationError::ConceptRead)?
            {
                if !supertype_relates_with_roles
                    .iter()
                    .any(|(_, supertype_relates)| supertype_relates == &old_relates_override)
                {
                    let role_type_overridden = old_relates_override.role();
                    return Err(SchemaValidationError::CannotChangeSupertypeAsRelatesOverrideIsImplicitlyLost(
                        get_label_or_schema_err(snapshot, relation_subtype)?,
                        get_label_or_schema_err(snapshot, relation_supertype)?,
                        get_label_or_schema_err(snapshot, role_type_overridden)?,
                    ));
                }
            }
        }

        Ok(())
    }

    pub(crate) fn validate_owns_compatible_with_new_supertype(
        snapshot: &impl ReadableSnapshot,
        owner_subtype: ObjectType<'static>,
        owner_supertype: ObjectType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let subtype_owns_declared: HashSet<Owns<'static>> =
            TypeReader::get_capabilities_declared(snapshot, owner_subtype.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

        for subtype_owns in subtype_owns_declared {
            let attribute_type = subtype_owns.attribute();
            if is_interface_hidden_by_overrides::<Owns<'static>>(
                snapshot,
                owner_supertype.clone(),
                attribute_type.clone(),
            )
            .map_err(SchemaValidationError::ConceptRead)?
            {
                return Err(SchemaValidationError::CannotChangeSupertypeAsOwnsIsOverriddenInTheNewSupertype(
                    get_label_or_schema_err(snapshot, owner_subtype)?,
                    get_label_or_schema_err(snapshot, owner_supertype)?,
                    get_label_or_schema_err(snapshot, attribute_type)?,
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn validate_owns_overrides_compatible_with_new_supertype<T: ObjectTypeAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        owner_subtype: T,
        owner_supertype: T,
    ) -> Result<(), SchemaValidationError> {
        let supertype_owns_with_attributes: HashMap<AttributeType<'static>, Owns<'static>> =
            TypeReader::get_capabilities(snapshot, owner_supertype.clone().into_owned_object_type())
                .map_err(SchemaValidationError::ConceptRead)?;

        let subtype_owns_declared: HashSet<Owns<'static>> =
            TypeReader::get_capabilities_declared(snapshot, owner_subtype.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

        for subtype_owns in subtype_owns_declared {
            if let Some(old_owns_override) = TypeReader::get_capability_override(snapshot, subtype_owns)
                .map_err(SchemaValidationError::ConceptRead)?
            {
                if !supertype_owns_with_attributes
                    .iter()
                    .any(|(_, supertype_owns)| supertype_owns == &old_owns_override)
                {
                    let attribute_type_overridden = old_owns_override.attribute();
                    return Err(SchemaValidationError::CannotChangeSupertypeAsOwnsOverrideIsImplicitlyLost(
                        get_label_or_schema_err(snapshot, owner_subtype)?,
                        get_label_or_schema_err(snapshot, owner_supertype)?,
                        get_label_or_schema_err(snapshot, attribute_type_overridden)?,
                    ));
                }
            }
        }

        Ok(())
    }

    pub(crate) fn validate_plays_compatible_with_new_supertype(
        snapshot: &impl ReadableSnapshot,
        player_subtype: ObjectType<'static>,
        player_supertype: ObjectType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let subtype_plays_declared: HashSet<Plays<'static>> =
            TypeReader::get_capabilities_declared(snapshot, player_subtype.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

        for subtype_plays in subtype_plays_declared {
            let role_type = subtype_plays.role();
            if is_interface_hidden_by_overrides::<Plays<'static>>(snapshot, player_supertype.clone(), role_type.clone())
                .map_err(SchemaValidationError::ConceptRead)?
            {
                return Err(SchemaValidationError::CannotChangeSupertypeAsPlaysIsOverriddenInTheNewSupertype(
                    get_label_or_schema_err(snapshot, player_subtype)?,
                    get_label_or_schema_err(snapshot, player_supertype)?,
                    get_label_or_schema_err(snapshot, role_type)?,
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn validate_plays_overrides_compatible_with_new_supertype<T: ObjectTypeAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        player_subtype: T,
        player_supertype: T,
    ) -> Result<(), SchemaValidationError> {
        let supertype_plays_with_roles: HashMap<RoleType<'static>, Plays<'static>> =
            TypeReader::get_capabilities(snapshot, player_supertype.clone().into_owned_object_type())
                .map_err(SchemaValidationError::ConceptRead)?;

        let subtype_plays_declared: HashSet<Plays<'static>> =
            TypeReader::get_capabilities_declared(snapshot, player_subtype.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

        for subtype_plays in subtype_plays_declared {
            if let Some(old_plays_override) = TypeReader::get_capability_override(snapshot, subtype_plays)
                .map_err(SchemaValidationError::ConceptRead)?
            {
                if !supertype_plays_with_roles.iter().any(|(_, supertype_plays)| supertype_plays == &old_plays_override)
                {
                    let role_type_overridden = old_plays_override.role();
                    return Err(SchemaValidationError::CannotChangeSupertypeAsPlaysOverrideIsImplicitlyLost(
                        get_label_or_schema_err(snapshot, player_subtype)?,
                        get_label_or_schema_err(snapshot, player_supertype)?,
                        get_label_or_schema_err(snapshot, role_type_overridden)?,
                    ));
                }
            }
        }

        Ok(())
    }

    pub(crate) fn validate_struct_name_uniqueness(
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<(), SchemaValidationError> {
        let struct_clash = TypeReader::get_struct_definition_key(snapshot, name)
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some();
        // TODO: Check other types clash?

        if struct_clash {
            Err(SchemaValidationError::StructNameShouldBeUnique(name.to_owned()))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_value_type_is_compatible_with_new_supertypes_value_type(
        snapshot: &impl ReadableSnapshot,
        subtype: AttributeType<'static>,
        supertype: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let subtype_declared_value_type = TypeReader::get_value_type_declared(snapshot, subtype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        let subtype_transitive_value_type = TypeReader::get_value_type_without_source(snapshot, subtype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        let supertype_value_type = TypeReader::get_value_type_without_source(snapshot, supertype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        match (&subtype_declared_value_type, &subtype_transitive_value_type, &supertype_value_type) {
            (None, None, None) => Ok(()),
            (None, None, Some(_)) => Ok(()),
            (None, Some(_), None) => {
                Self::validate_when_attribute_type_loses_value_type(snapshot, subtype, subtype_transitive_value_type)
            }
            (Some(_), None, None) => Ok(()),
            (Some(_), Some(_), None) => Ok(()),
            | (None, Some(old_value_type), Some(new_value_type))
            | (Some(old_value_type), None, Some(new_value_type))
            | (Some(old_value_type), Some(_), Some(new_value_type)) => {
                if old_value_type == new_value_type {
                    Ok(())
                } else {
                    Err(SchemaValidationError::ChangingAttributeTypeSupertypeWillImplicitlyChangeItsValueType(
                        get_label_or_schema_err(snapshot, subtype)?,
                        subtype_declared_value_type,
                    ))
                }
            }
        }
    }

    pub(crate) fn validate_value_type_can_be_unset(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let value_type_with_source =
            TypeReader::get_value_type(snapshot, attribute_type.clone()).map_err(SchemaValidationError::ConceptRead)?;
        match value_type_with_source {
            Some((value_type, source)) => {
                if source != attribute_type {
                    return Err(SchemaValidationError::CannotUnsetInheritedValueType(
                        value_type,
                        get_label_or_schema_err(snapshot, source)?,
                    ));
                }

                let attribute_supertype = TypeReader::get_supertype(snapshot, attribute_type.clone())
                    .map_err(SchemaValidationError::ConceptRead)?;
                match &attribute_supertype {
                    Some(supertype) => {
                        let supertype_value_type =
                            TypeReader::get_value_type_without_source(snapshot, supertype.clone())
                                .map_err(SchemaValidationError::ConceptRead)?;
                        match supertype_value_type {
                            Some(_) => Ok(()),
                            None => Self::validate_when_attribute_type_loses_value_type(
                                snapshot,
                                attribute_type,
                                Some(value_type),
                            ),
                        }
                    }
                    None => {
                        Self::validate_when_attribute_type_loses_value_type(snapshot, attribute_type, Some(value_type))
                    }
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_value_type_compatible_with_abstractness(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
        abstract_set: Option<bool>,
    ) -> Result<(), SchemaValidationError> {
        let is_abstract = abstract_set.unwrap_or(type_has_annotation_category(
            snapshot,
            attribute_type.clone(),
            AnnotationCategory::Abstract,
        )?);

        match &value_type {
            Some(_) => Ok(()),
            None => {
                if is_abstract {
                    Ok(())
                } else {
                    Err(SchemaValidationError::AttributeTypeWithoutValueTypeShouldBeAbstract(get_label_or_schema_err(
                        snapshot,
                        attribute_type,
                    )?))
                }
            }
        }
    }

    pub(crate) fn validate_annotation_regex_compatible_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        if AnnotationRegex::value_type_valid(value_type.clone()) {
            Ok(())
        } else {
            Err(SchemaValidationError::ValueTypeIsNotCompatibleWithRegexAnnotation(
                get_label_or_schema_err(snapshot, attribute_type)?,
                value_type,
            ))
        }
    }

    pub(crate) fn validate_annotation_range_compatible_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        if AnnotationRange::value_type_valid(value_type.clone()) {
            Ok(())
        } else {
            Err(SchemaValidationError::ValueTypeIsNotCompatibleWithRangeAnnotation(
                get_label_or_schema_err(snapshot, attribute_type)?,
                value_type,
            ))
        }
    }

    pub(crate) fn validate_annotation_values_compatible_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        if AnnotationValues::value_type_valid(value_type.clone()) {
            Ok(())
        } else {
            Err(SchemaValidationError::ValueTypeIsNotCompatibleWithValuesAnnotation(
                get_label_or_schema_err(snapshot, attribute_type)?,
                value_type,
            ))
        }
    }

    pub(crate) fn validate_annotation_set_only_for_interface<CAP>(
        snapshot: &impl ReadableSnapshot,
        interface: CAP::InterfaceType,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError>
    where
        CAP: Capability<'static, ObjectType = ObjectType<'static>>,
    {
        let implementations = TypeReader::get_capabilities_for_interface::<CAP>(snapshot, interface.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for (_, implementation) in implementations {
            let implementation_annotations = TypeReader::get_type_edge_annotations(snapshot, implementation)
                .map_err(SchemaValidationError::ConceptRead)?;
            if implementation_annotations.keys().map(|annotation| annotation.category()).contains(&annotation_category)
            {
                return Err(
                    SchemaValidationError::CannotSetAnnotationToInterfaceBecauseItAlreadyExistsForItsImplementation(
                        get_label_or_schema_err(snapshot, interface)?,
                        annotation_category,
                    ),
                );
            }
        }

        Ok(())
    }

    pub(crate) fn validate_annotation_set_only_for_capability<CAP>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError>
    where
        CAP: Capability<'static, ObjectType = ObjectType<'static>>,
    {
        let interface = capability.interface();
        let interface_annotations = TypeReader::get_type_annotations(snapshot, interface.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        if interface_annotations
            .keys()
            .map(|annotation| annotation.clone().into().category())
            .contains(&annotation_category)
        {
            return Err(SchemaValidationError::CannotSetAnnotationToCapabilityBecauseItAlreadyExistsForItsInterface(
                get_label_or_schema_err(snapshot, interface)?,
                annotation_category,
            ));
        }

        Ok(())
    }

    pub(crate) fn validate_attribute_type_supertype_is_abstract<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: T,
    ) -> Result<(), SchemaValidationError> {
        if type_.is_abstract(snapshot, type_manager).map_err(SchemaValidationError::ConceptRead)? {
            Ok(())
        } else {
            Err(SchemaValidationError::AttributeTypeSupertypeIsNotAbstract(get_label_or_schema_err(snapshot, type_)?))
        }
    }

    pub(crate) fn validate_capability_abstractness<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        object: impl TypeAPI<'static>,
        interface_type: CAP::InterfaceType,
        set_abstract: Option<bool>,
    ) -> Result<(), SchemaValidationError> {
        let is_object_abstract =
            object.is_abstract(snapshot, type_manager).map_err(SchemaValidationError::ConceptRead)?;
        let is_interface_abstract = set_abstract
            .unwrap_or(interface_type.is_abstract(snapshot, type_manager).map_err(SchemaValidationError::ConceptRead)?);

        match (&is_object_abstract, &is_interface_abstract) {
            (true, true) | (false, false) | (true, false) => Ok(()),
            (false, true) => Err(SchemaValidationError::NonAbstractTypeCannotHaveAbstractCapability(
                CAP::KIND,
                get_label_or_schema_err(snapshot, object)?,
                get_label_or_schema_err(snapshot, interface_type)?,
            )),
        }
    }

    pub(crate) fn validate_cardinality_arguments(
        cardinality: AnnotationCardinality,
    ) -> Result<(), SchemaValidationError> {
        if cardinality.valid() {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidCardinalityArguments(cardinality))
        }
    }

    pub(crate) fn validate_regex_arguments(regex: AnnotationRegex) -> Result<(), SchemaValidationError> {
        if regex.valid() {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidRegexArguments(regex))
        }
    }

    pub(crate) fn validate_range_arguments(
        range: AnnotationRange,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        if range.valid(value_type) {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidRangeArguments(range))
        }
    }

    pub(crate) fn validate_values_arguments(
        values: AnnotationValues,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        if values.valid(value_type) {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidValuesArguments(values))
        }
    }

    pub(crate) fn validate_key_narrows_inherited_cardinality<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        edge: CAP,
        overridden_edge: CAP,
    ) -> Result<(), SchemaValidationError> {
        validate_key_narrows_inherited_cardinality(snapshot, type_manager, edge, overridden_edge)
    }

    pub(crate) fn validate_cardinality_narrows_inherited_cardinality<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        edge: CAP,
        overridden_edge: CAP,
        cardinality: AnnotationCardinality,
    ) -> Result<(), SchemaValidationError> {
        validate_cardinality_narrows_inherited_cardinality(snapshot, type_manager, edge, overridden_edge, cardinality)
    }

    pub(crate) fn validate_type_supertype_abstractness<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        attribute_type: T,
        supertype: Option<T>,
        set_subtype_abstract: Option<bool>,
    ) -> Result<(), SchemaValidationError> {
        validate_type_supertype_abstractness(snapshot, attribute_type, supertype, set_subtype_abstract)
    }

    pub(crate) fn validate_type_regex_narrows_inherited_regex<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        attribute_type: T,
        supertype: Option<T>,
        regex: AnnotationRegex,
    ) -> Result<(), SchemaValidationError> {
        validate_type_regex_narrows_inherited_regex(snapshot, attribute_type, supertype, regex)
    }

    pub(crate) fn validate_edge_regex_narrows_inherited_regex<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        owns: CAP,
        overridden_owns: Option<CAP>,
        regex: AnnotationRegex,
    ) -> Result<(), SchemaValidationError> {
        validate_edge_regex_narrows_inherited_regex(snapshot, owns, overridden_owns, regex)
    }

    pub(crate) fn validate_type_range_narrows_inherited_range<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        attribute_type: T,
        supertype: Option<T>,
        range: AnnotationRange,
    ) -> Result<(), SchemaValidationError> {
        validate_type_range_narrows_inherited_range(snapshot, attribute_type, supertype, range)
    }

    // TODO: Wrap into a macro to call the same function from validation.rs?
    pub(crate) fn validate_edge_range_narrows_inherited_range<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        owns: CAP,
        overridden_owns: Option<CAP>,
        range: AnnotationRange,
    ) -> Result<(), SchemaValidationError> {
        validate_edge_range_narrows_inherited_range(snapshot, owns, overridden_owns, range)
    }

    pub(crate) fn validate_type_values_narrows_inherited_values<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        attribute_type: T,
        supertype: Option<T>,
        values: AnnotationValues,
    ) -> Result<(), SchemaValidationError> {
        validate_type_values_narrows_inherited_values(snapshot, attribute_type, supertype, values)
    }

    pub(crate) fn validate_edge_values_narrows_inherited_values<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        owns: CAP,
        overridden_owns: Option<CAP>,
        values: AnnotationValues,
    ) -> Result<(), SchemaValidationError> {
        validate_edge_values_narrows_inherited_values(snapshot, owns, overridden_owns, values)
    }

    pub(crate) fn validate_relates_distinct_annotation_ordering(
        snapshot: &impl ReadableSnapshot,
        relates: Relates<'static>,
        ordering: Option<Ordering>,
        distinct_set: Option<bool>,
    ) -> Result<(), SchemaValidationError> {
        let role = relates.role();
        let ordering = ordering.unwrap_or(
            TypeReader::get_type_ordering(snapshot, role.clone()).map_err(SchemaValidationError::ConceptRead)?,
        );
        let distinct_set = distinct_set
            .unwrap_or(edge_get_annotation_by_category(snapshot, relates, AnnotationCategory::Distinct)?.is_some());

        if is_ordering_compatible_with_distinct_annotation(ordering, distinct_set) {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidOrderingForDistinctAnnotation(get_label_or_schema_err(snapshot, role)?))
        }
    }

    pub(crate) fn validate_owns_distinct_annotation_ordering(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        ordering: Option<Ordering>,
        distinct_set: Option<bool>,
    ) -> Result<(), SchemaValidationError> {
        let attribute = owns.attribute();
        let ordering = ordering.unwrap_or(
            TypeReader::get_type_edge_ordering(snapshot, owns.clone()).map_err(SchemaValidationError::ConceptRead)?,
        );
        let distinct_set = distinct_set
            .unwrap_or(edge_get_annotation_by_category(snapshot, owns, AnnotationCategory::Distinct)?.is_some());

        if is_ordering_compatible_with_distinct_annotation(ordering, distinct_set) {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidOrderingForDistinctAnnotation(get_label_or_schema_err(
                snapshot, attribute,
            )?))
        }
    }

    pub(crate) fn validate_role_supertype_ordering_match(
        snapshot: &impl ReadableSnapshot,
        subtype_role: RoleType<'static>,
        supertype_role: RoleType<'static>,
        set_subtype_role_ordering: Option<Ordering>,
    ) -> Result<(), SchemaValidationError> {
        validate_type_supertype_ordering_match(snapshot, subtype_role, supertype_role, set_subtype_role_ordering)
    }

    pub(crate) fn validate_owns_override_ordering_match(
        snapshot: &impl ReadableSnapshot,
        subtype_owns: Owns<'static>,
        supertype_owns: Owns<'static>,
        set_subtype_owns_ordering: Option<Ordering>,
    ) -> Result<(), SchemaValidationError> {
        validate_edge_override_ordering_match(snapshot, subtype_owns, supertype_owns, set_subtype_owns_ordering)
    }

    pub(crate) fn validate_supertype_annotations_compatibility<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        subtype: T,
        supertype: T,
    ) -> Result<(), SchemaValidationError> {
        let subtype_declared_annotations = TypeReader::get_type_annotations_declared(snapshot, subtype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        for subtype_annotation in subtype_declared_annotations {
            let category = subtype_annotation.clone().into().category();

            Self::validate_declared_annotation_is_compatible_with_other_inherited_annotations(
                snapshot,
                supertype.clone(),
                category,
            )?;

            validate_type_annotations_narrowing_of_inherited_annotations(
                snapshot,
                subtype.clone(),
                supertype.clone(),
                subtype_annotation,
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_edge_override_annotations_compatibility<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        edge: CAP,
        overridden_edge: CAP,
    ) -> Result<(), SchemaValidationError> {
        let subtype_declared_annotations = TypeReader::get_type_edge_annotations_declared(snapshot, edge.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        for subtype_annotation in subtype_declared_annotations {
            let category = subtype_annotation.category();

            Self::validate_declared_edge_annotation_is_compatible_with_other_inherited_annotations(
                snapshot,
                overridden_edge.clone(),
                category,
            )?;

            validate_edge_annotations_narrowing_of_inherited_annotations(
                snapshot,
                type_manager,
                edge.clone(),
                overridden_edge.clone(),
                subtype_annotation,
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_sub_does_not_create_cycle<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        supertype: T,
    ) -> Result<(), SchemaValidationError> {
        let existing_supertypes =
            TypeReader::get_supertypes(snapshot, supertype.clone()).map_err(SchemaValidationError::ConceptRead)?;
        if supertype == type_ || existing_supertypes.contains(&type_) {
            Err(SchemaValidationError::CycleFoundInTypeHierarchy(
                get_label_or_schema_err(snapshot, type_)?,
                get_label_or_schema_err(snapshot, supertype)?,
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_relates_is_inherited(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        role_type: RoleType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let super_relation =
            TypeReader::get_supertype(snapshot, relation_type.clone()).map_err(SchemaValidationError::ConceptRead)?;
        if let Some(super_relation) = super_relation {
            let is_inherited = TypeReader::get_capabilities::<Relates<'_>>(snapshot, super_relation)
                .map_err(SchemaValidationError::ConceptRead)?
                .contains_key(&role_type);
            if is_inherited {
                Ok(())
            } else {
                Err(SchemaValidationError::RelatesNotInherited(relation_type, role_type))
            }
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_owns_is_inherited(
        snapshot: &impl ReadableSnapshot,
        owner: ObjectType<'static>,
        attribute: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let is_inherited = with_object_type!(owner, |owner| {
            let super_owner =
                TypeReader::get_supertype(snapshot, owner.clone()).map_err(SchemaValidationError::ConceptRead)?;
            if super_owner.is_none() {
                return Ok(());
            }
            let owns_transitive: HashMap<AttributeType<'static>, Owns<'static>> =
                TypeReader::get_capabilities(snapshot, super_owner.unwrap().clone().into_owned_object_type())
                    .map_err(SchemaValidationError::ConceptRead)?;
            owns_transitive.contains_key(&attribute)
        });
        if is_inherited {
            Ok(())
        } else {
            Err(SchemaValidationError::OwnsNotInherited(owner, attribute))
        }
    }

    pub(crate) fn validate_overridden_owns_attribute_type_is_supertype_or_self(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        attribute_type_overridden: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        if is_overridden_interface_object_one_of_supertypes_or_self(
            snapshot,
            owns.attribute(),
            attribute_type_overridden.clone(),
        )
        .map_err(SchemaValidationError::ConceptRead)?
        {
            Ok(())
        } else {
            Err(SchemaValidationError::OverriddenCapabilityInterfaceIsNotSupertype(
                CapabilityKind::Owns,
                get_label_or_schema_err(snapshot, owns.owner())?,
                get_label_or_schema_err(snapshot, owns.attribute())?,
                get_label_or_schema_err(snapshot, attribute_type_overridden)?,
            ))
        }
    }

    pub(crate) fn validate_overridden_plays_role_type_is_supertype_or_self(
        snapshot: &impl ReadableSnapshot,
        plays: Plays<'static>,
        role_type_overridden: RoleType<'static>,
    ) -> Result<(), SchemaValidationError> {
        if is_overridden_interface_object_one_of_supertypes_or_self(
            snapshot,
            plays.role(),
            role_type_overridden.clone(),
        )
        .map_err(SchemaValidationError::ConceptRead)?
        {
            Ok(())
        } else {
            Err(SchemaValidationError::OverriddenCapabilityInterfaceIsNotSupertype(
                CapabilityKind::Plays,
                get_label_or_schema_err(snapshot, plays.player())?,
                get_label_or_schema_err(snapshot, plays.role())?,
                get_label_or_schema_err(snapshot, role_type_overridden)?,
            ))
        }
    }

    pub(crate) fn validate_interface_not_overridden<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        object_type: CAP::ObjectType,
        interface_type: CAP::InterfaceType,
    ) -> Result<(), SchemaValidationError> {
        if !is_interface_hidden_by_overrides::<CAP>(snapshot, object_type.clone(), interface_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?
        {
            Ok(())
        } else {
            Err(SchemaValidationError::OverriddenCapabilityCannotBeRedeclared(
                CapabilityKind::Owns,
                get_label_or_schema_err(snapshot, object_type)?,
                get_label_or_schema_err(snapshot, interface_type)?,
            ))
        }
    }

    pub(crate) fn validate_plays_is_inherited(
        snapshot: &impl ReadableSnapshot,
        player: ObjectType<'static>,
        role_type: RoleType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let is_inherited = with_object_type!(player, |player| {
            let super_player =
                TypeReader::get_supertype(snapshot, player.clone()).map_err(SchemaValidationError::ConceptRead)?;
            if super_player.is_none() {
                return Ok(());
            }
            let plays_transitive: HashMap<RoleType<'static>, Plays<'static>> =
                TypeReader::get_capabilities(snapshot, super_player.unwrap().clone().into_owned_object_type())
                    .map_err(SchemaValidationError::ConceptRead)?;
            plays_transitive.contains_key(&role_type)
        });
        if is_inherited {
            Ok(())
        } else {
            Err(SchemaValidationError::PlaysNotInherited(player, role_type))
        }
    }

    pub(crate) fn validate_value_type_compatible_with_inherited_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
        inherited_value_type_with_source: Option<(ValueType, AttributeType<'static>)>,
    ) -> Result<(), SchemaValidationError> {
        match value_type {
            Some(value_type) => {
                if let Some((inherited_value_type, inherited_value_type_source)) = inherited_value_type_with_source {
                    if inherited_value_type == value_type {
                        Ok(())
                    } else {
                        Err(SchemaValidationError::ValueTypeNotCompatibleWithInheritedValueType(
                            get_label_or_schema_err(snapshot, attribute_type)?,
                            get_label_or_schema_err(snapshot, inherited_value_type_source)?,
                            value_type,
                            inherited_value_type,
                        ))
                    }
                } else {
                    Ok(())
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_when_attribute_type_loses_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        match value_type {
            Some(_) => {
                Self::validate_value_type_compatible_with_abstractness(snapshot, attribute_type.clone(), None, None)?;
                Self::validate_attribute_type_value_type_compatible_with_declared_annotations(
                    snapshot,
                    attribute_type.clone(),
                    None,
                )?;
                Self::validate_value_type_compatible_with_all_owns_annotations(snapshot, attribute_type, None)
                // TODO: Re-enable when we get the thing_manager
                // Self::validate_exact_type_no_instances_attribute(snapshot, self, attribute.clone())
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_relation_type_does_not_acquire_cascade_annotation_with_new_supertype(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        new_supertype: RelationType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let old_annotation =
            type_get_annotation_by_category(snapshot, relation_type.clone(), AnnotationCategory::Cascade)?;
        match old_annotation {
            None => {
                let new_supertype_annotation =
                    type_get_annotation_by_category(snapshot, new_supertype.clone(), AnnotationCategory::Cascade)?;
                match new_supertype_annotation {
                    None => Ok(()),
                    Some(_) => Err(SchemaValidationError::ChangingRelationSupertypeLeadsToImplicitCascadeAnnotationAcquisitionAndUnexpectedDataLoss(
                        get_label_or_schema_err(snapshot, relation_type)?,
                        get_label_or_schema_err(snapshot, new_supertype)?,
                    )),
                }
            }
            Some(_) => Ok(()),
        }
    }

    pub(crate) fn validate_attribute_type_does_not_lose_independent_annotation_with_new_supertype(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        new_supertype: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let annotation_owner =
            type_get_owner_of_annotation_category(snapshot, attribute_type.clone(), AnnotationCategory::Independent)?;
        match annotation_owner {
            Some(owner) => {
                if attribute_type == owner {
                    Ok(())
                } else {
                    let new_supertype_annotation = type_get_annotation_by_category(
                        snapshot,
                        new_supertype.clone(),
                        AnnotationCategory::Independent,
                    )?;
                    match new_supertype_annotation {
                    Some(_) => Ok(()),
                    None => Err(SchemaValidationError::ChangingAttributeSupertypeLeadsToImplicitIndependentAnnotationLossAndUnexpectedDataLoss(
                        get_label_or_schema_err(snapshot, attribute_type)?,
                        get_label_or_schema_err(snapshot, new_supertype)?,
                    )),
                }
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_unset_owns_is_not_inherited(
        snapshot: &impl ReadableSnapshot,
        owner: ObjectType<'static>,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let all_owns: HashMap<AttributeType<'static>, Owns<'static>> =
            TypeReader::get_capabilities(snapshot, owner.clone().into_owned_object_type())
                .map_err(SchemaValidationError::ConceptRead)?;
        let found_owns =
            all_owns.iter().find(|(existing_owns_attribute_type, _)| **existing_owns_attribute_type == attribute_type);

        match found_owns {
            Some((_, owns)) => {
                if owner == owns.owner() {
                    Ok(())
                } else {
                    let owns_owner = owns.owner();
                    Err(SchemaValidationError::CannotUnsetInheritedOwns(
                        get_label_or_schema_err(snapshot, attribute_type)?,
                        get_label_or_schema_err(snapshot, owns_owner)?,
                    ))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_unset_plays_is_not_inherited(
        snapshot: &impl ReadableSnapshot,
        player: ObjectType<'static>,
        role_type: RoleType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let all_plays: HashMap<RoleType<'static>, Plays<'static>> =
            TypeReader::get_capabilities(snapshot, player.clone().into_owned_object_type())
                .map_err(SchemaValidationError::ConceptRead)?;
        let found_plays =
            all_plays.iter().find(|(existing_plays_role_type, _)| **existing_plays_role_type == role_type);

        match found_plays {
            Some((_, plays)) => {
                if player == plays.player() {
                    Ok(())
                } else {
                    let plays_player = plays.player();
                    Err(SchemaValidationError::CannotUnsetInheritedPlays(
                        get_label_or_schema_err(snapshot, role_type)?,
                        get_label_or_schema_err(snapshot, plays_player)?,
                    ))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_unset_annotation_is_not_inherited<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError> {
        let annotation_owner = type_get_owner_of_annotation_category(snapshot, type_.clone(), annotation_category)?;
        match annotation_owner {
            Some(owner) => {
                if type_ == owner {
                    Ok(())
                } else {
                    Err(SchemaValidationError::CannotUnsetInheritedAnnotation(
                        annotation_category,
                        get_label_or_schema_err(snapshot, owner)?,
                    ))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_unset_edge_annotation_is_not_inherited<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        edge: CAP,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError> {
        let annotation_owner = edge_get_owner_of_annotation_category(snapshot, edge.clone(), annotation_category)?;
        match annotation_owner {
            Some(owner) => {
                if edge == owner {
                    Ok(())
                } else {
                    Err(SchemaValidationError::CannotUnsetInheritedEdgeAnnotation(
                        annotation_category,
                        get_label_or_schema_err(snapshot, edge.object())?,
                        get_label_or_schema_err(snapshot, edge.interface())?,
                    ))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_declared_annotation_is_compatible_with_other_inherited_annotations(
        snapshot: &impl ReadableSnapshot,
        type_: impl KindAPI<'static>,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError> {
        validate_declared_annotation_is_compatible_with_other_inherited_annotations(
            snapshot,
            type_,
            annotation_category,
        )
    }

    pub(crate) fn validate_declared_edge_annotation_is_compatible_with_other_inherited_annotations<CAP>(
        snapshot: &impl ReadableSnapshot,
        edge: CAP,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError>
    where
        CAP: Capability<'static>,
    {
        validate_declared_edge_annotation_is_compatible_with_other_inherited_annotations(
            snapshot,
            edge,
            annotation_category,
        )
    }

    pub(crate) fn validate_declared_annotation_is_compatible_with_other_declared_annotations(
        snapshot: &impl ReadableSnapshot,
        type_: impl KindAPI<'static>,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError> {
        let existing_annotations = TypeReader::get_type_annotations_declared(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for existing_annotation in existing_annotations {
            let existing_annotation_category = existing_annotation.clone().into().category();
            if !existing_annotation_category.declarable_alongside(annotation_category) {
                return Err(SchemaValidationError::AnnotationIsNotCompatibleWithDeclaredAnnotation(
                    annotation_category,
                    existing_annotation_category,
                    get_label_or_concept_read_err(snapshot, type_).map_err(SchemaValidationError::ConceptRead)?,
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn validate_new_unique_annotation_compatible_with_capability_and_subcapabilities_instances<
        CAP: Capability<'static>,
    >(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
    ) -> Result<(), SchemaValidationError> {
        let annotation = Annotation::Unique(AnnotationUnique);

        let existing_annotations = TypeReader::get_type_edge_annotations_declared(snapshot, capability.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        // if !existing_annotations.contains(&annotation) {
        //
        // }
        //
        // for existing_annotation in existing_annotations {
        //     let existing_annotation_category = existing_annotation.category();
        //     if !existing_annotation_category.declarable_alongside(annotation_category) {
        //         let interface = edge.interface();
        //         return Err(SchemaValidationError::AnnotationIsNotCompatibleWithDeclaredAnnotation(
        //             annotation_category,
        //             existing_annotation_category,
        //             get_label_or_concept_read_err(snapshot, interface).map_err(SchemaValidationError::ConceptRead)?,
        //         ));
        //     }
        // }

        Ok(())
    }

    // TODO: Impelement
    fn get_capability_or_its_subcapability_with_violated_annotation_constraints<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        capability: CAP,
        annotations: HashSet<Annotation>,
    ) -> Result<(), SchemaValidationError> {
        // let mut capabilities = VecDeque::new();
        // owners.push_front(owner.into_owned_object_type());
        //
        // while let Some(current_owner) = owners.pop_back() {
        //     if Self::do_instances_of_owns_violate_cardinality(
        //         snapshot,
        //         thing_manager,
        //         current_owner.clone(),
        //         attribute_type.clone(),
        //         cardinality,
        //     )? {
        //         type_with_instances_violating_cardinality = Some(current_owner.clone());
        //         break; // TODO: Maybe we want to return all the corrupted owns here, just moving forward for now
        //     }
        //
        //     current_owner
        //         .get_subtypes(snapshot, type_manager)?
        //         .iter()
        //         .for_each(|subowner| owners.push_front(subowner.clone().into_owned_object_type()));
        // }
        Ok(())
    }

    pub(crate) fn validate_declared_edge_annotation_is_compatible_with_declared_annotations<CAP>(
        snapshot: &impl ReadableSnapshot,
        edge: CAP,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError>
    where
        CAP: Capability<'static>,
    {
        let existing_annotations = TypeReader::get_type_edge_annotations_declared(snapshot, edge.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for existing_annotation in existing_annotations {
            let existing_annotation_category = existing_annotation.category();
            if !existing_annotation_category.declarable_alongside(annotation_category) {
                let interface = edge.interface();
                return Err(SchemaValidationError::AnnotationIsNotCompatibleWithDeclaredAnnotation(
                    annotation_category,
                    existing_annotation_category,
                    get_label_or_concept_read_err(snapshot, interface).map_err(SchemaValidationError::ConceptRead)?,
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn validate_owns_value_type_compatible_with_unique_annotation(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        if Self::is_owns_value_type_keyable(value_type.clone()) {
            Ok(())
        } else {
            let owner = owns.owner();
            let attribute_type = owns.attribute();
            Err(SchemaValidationError::ValueTypeIsNotKeyableForUniqueAnnotation(
                get_label_or_schema_err(snapshot, owner)?,
                get_label_or_schema_err(snapshot, attribute_type)?,
                value_type,
            ))
        }
    }

    pub(crate) fn validate_owns_value_type_compatible_with_key_annotation(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        if Self::is_owns_value_type_keyable(value_type.clone()) {
            Ok(())
        } else {
            let owner = owns.owner();
            let attribute_type = owns.attribute();
            Err(SchemaValidationError::ValueTypeIsNotKeyableForKeyAnnotation(
                get_label_or_schema_err(snapshot, owner)?,
                get_label_or_schema_err(snapshot, attribute_type)?,
                value_type,
            ))
        }
    }

    pub(crate) fn validate_attribute_type_value_type_compatible_with_declared_annotations(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        let annotations = TypeReader::get_type_annotations_declared(snapshot, attribute_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        for annotation in annotations {
            match annotation {
                AttributeTypeAnnotation::Regex(_) => Self::validate_annotation_regex_compatible_value_type(
                    snapshot,
                    attribute_type.clone(),
                    value_type.clone(),
                )?,
                AttributeTypeAnnotation::Range(_) => Self::validate_annotation_range_compatible_value_type(
                    snapshot,
                    attribute_type.clone(),
                    value_type.clone(),
                )?,
                AttributeTypeAnnotation::Values(_) => Self::validate_annotation_values_compatible_value_type(
                    snapshot,
                    attribute_type.clone(),
                    value_type.clone(),
                )?,
                | AttributeTypeAnnotation::Abstract(_) | AttributeTypeAnnotation::Independent(_) => {}
            }
        }

        Ok(())
    }

    pub(crate) fn validate_value_type_compatible_with_all_owns_annotations(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        let all_owns = TypeReader::get_capabilities_for_interface::<Owns<'static>>(snapshot, attribute_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for (_, owns) in all_owns {
            Self::validate_owns_value_type_compatible_with_annotations(snapshot, owns, value_type.clone())?
        }
        Ok(())
    }

    pub(crate) fn validate_owns_value_type_compatible_with_annotations(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        let annotations = TypeReader::get_type_edge_annotations(snapshot, owns.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        for (annotation, _) in annotations {
            match annotation {
                Annotation::Unique(_) => Self::validate_owns_value_type_compatible_with_unique_annotation(
                    snapshot,
                    owns.clone(),
                    value_type.clone(),
                )?,
                Annotation::Key(_) => Self::validate_owns_value_type_compatible_with_key_annotation(
                    snapshot,
                    owns.clone(),
                    value_type.clone(),
                )?,
                Annotation::Regex(_) => Self::validate_annotation_regex_compatible_value_type(
                    snapshot,
                    owns.attribute(),
                    value_type.clone(),
                )?,
                Annotation::Range(_) => Self::validate_annotation_range_compatible_value_type(
                    snapshot,
                    owns.attribute(),
                    value_type.clone(),
                )?,
                Annotation::Values(_) => Self::validate_annotation_values_compatible_value_type(
                    snapshot,
                    owns.attribute(),
                    value_type.clone(),
                )?,
                | Annotation::Abstract(_)
                | Annotation::Distinct(_)
                | Annotation::Independent(_)
                | Annotation::Cardinality(_)
                | Annotation::Cascade(_) => {}
            }
        }
        Ok(())
    }

    pub(crate) fn is_owns_value_type_keyable(value_type_opt: Option<ValueType>) -> bool {
        match value_type_opt {
            Some(value_type) => value_type.keyable(),
            None => true,
        }
    }

    pub(crate) fn validate_no_instances_to_delete<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        type_: impl KindAPI<'a>,
    ) -> Result<(), SchemaValidationError> {
        let has_instances = Self::has_instances_of_type(snapshot, thing_manager, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        if has_instances {
            Err(SchemaValidationError::CannotDeleteTypeWithExistingInstances(get_label_or_schema_err(snapshot, type_)?))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_no_instances_to_set_abstract<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        type_: impl KindAPI<'a>,
    ) -> Result<(), SchemaValidationError> {
        let has_instances = Self::has_instances_of_type(snapshot, thing_manager, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        if has_instances {
            Err(SchemaValidationError::CannotSetAbstractToTypeWithExistingInstances(get_label_or_schema_err(
                snapshot, type_,
            )?))
        } else {
            Ok(())
        }
    }

    fn has_instances_of_type<'a, T: KindAPI<'a>>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        type_: T,
    ) -> Result<bool, ConceptReadError> {
        match T::ROOT_KIND {
            Kind::Entity => {
                let entity_type = EntityType::new(type_.vertex().into_owned());
                let mut iterator = thing_manager.get_entities_in(snapshot, entity_type.clone().into_owned());
                match iterator.next() {
                    None => Ok(false),
                    Some(result) => result.map(|_| true),
                }
            }
            Kind::Attribute => {
                let attribute_type = AttributeType::new(type_.vertex().into_owned());
                let mut iterator = thing_manager.get_attributes_in(snapshot, attribute_type.clone().into_owned())?;
                match iterator.next() {
                    None => Ok(false),
                    Some(result) => result.map(|_| true),
                }
            }
            Kind::Relation => {
                let relation_type = RelationType::new(type_.vertex().into_owned());
                let mut iterator = thing_manager.get_relations_in(snapshot, relation_type.clone().into_owned());
                match iterator.next() {
                    None => Ok(false),
                    Some(result) => result.map(|_| true),
                }
            }
            Kind::Role => {
                let role_type = RoleType::new(type_.vertex().into_owned());
                // TODO: See if we can use existing methods from the ThingManager
                let relation_type =
                    TypeReader::get_role_type_relates_declared(snapshot, role_type.clone().into_owned())?.relation();
                let mut relation_iterator = thing_manager.get_relations_in(snapshot, relation_type.into_owned());
                while let Some(result) = relation_iterator.next() {
                    let prefix = ThingEdgeLinks::prefix_from_relation(result?.into_vertex());
                    let mut role_player_iterator = RolePlayerIterator::new(
                        snapshot.iterate_range(KeyRange::new_within(prefix, ThingEdgeLinks::FIXED_WIDTH_ENCODING)),
                    );
                    if let Some(result) = role_player_iterator.next() {
                        return result.map(|_| true);
                    }
                }
                Ok(false)
            }
        }
    }

    pub(crate) fn validate_modified_owns_cardinality_does_not_violate_existing_instances_while_changing_supertype(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        owner_subtype: ObjectType<'static>,
        owner_supertype: ObjectType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let modified_owns_cardinalities = Self::get_capabilities_with_modified_cardinality_if_supertype_is_changed::<
            Owns<'static>,
        >(
            snapshot, type_manager, owner_subtype.clone(), owner_supertype.clone()
        )?;

        for (owns, cardinality) in modified_owns_cardinalities {
            let attribute_type = owns.attribute();
            // TODO: Should probably get all the lost and new annotations, make decisions based on these values!
            let type_having_violating_instances = Self::type_or_subtype_which_instances_violate_cardinality_of_owns(
                snapshot,
                type_manager,
                thing_manager,
                owner_subtype.clone(),
                attribute_type.clone(),
                cardinality.clone(),
            )
            .map_err(SchemaValidationError::ConceptRead)?;

            if let Some(type_having_violating_instances) = type_having_violating_instances {
                return Err(
                    SchemaValidationError::CannotChangeSupertypeAsUpdatedCardinalityIsViolatedByExistingInstances(
                        CapabilityKind::Owns,
                        get_label_or_schema_err(snapshot, owner_subtype)?,
                        get_label_or_schema_err(snapshot, owner_supertype)?,
                        get_label_or_schema_err(snapshot, type_having_violating_instances)?,
                        get_label_or_schema_err(snapshot, attribute_type)?,
                        cardinality,
                    ),
                );
            }
        }

        Ok(())
    }

    pub(crate) fn validate_modified_plays_cardinality_does_not_violate_existing_instances_while_changing_supertype(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        player_subtype: ObjectType<'static>,
        player_supertype: ObjectType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let modified_plays_cardinalities = Self::get_capabilities_with_modified_cardinality_if_supertype_is_changed::<
            Plays<'static>,
        >(
            snapshot, type_manager, player_subtype.clone(), player_supertype.clone()
        )?;

        for (plays, cardinality) in modified_plays_cardinalities {
            let role_type = plays.role();
            let type_having_violating_instances = Self::type_or_subtype_which_instances_violate_cardinality_of_plays(
                snapshot,
                type_manager,
                thing_manager,
                player_subtype.clone(),
                role_type.clone(),
                cardinality.clone(),
            )
            .map_err(SchemaValidationError::ConceptRead)?;

            if let Some(type_having_violating_instances) = type_having_violating_instances {
                return Err(
                    SchemaValidationError::CannotChangeSupertypeAsUpdatedCardinalityIsViolatedByExistingInstances(
                        CapabilityKind::Plays,
                        get_label_or_schema_err(snapshot, player_subtype)?,
                        get_label_or_schema_err(snapshot, player_supertype)?,
                        get_label_or_schema_err(snapshot, type_having_violating_instances)?,
                        get_label_or_schema_err(snapshot, role_type)?,
                        cardinality,
                    ),
                );
            }
        }

        Ok(())
    }

    // TODO: Do we want to check subtypes in operation time?
    fn type_or_subtype_which_instances_violate_cardinality_of_owns<'a>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        owner: ObjectType<'a>,
        attribute_type: AttributeType<'a>,
        cardinality: AnnotationCardinality,
    ) -> Result<Option<ObjectType<'static>>, ConceptReadError> {
        let mut type_with_instances_violating_cardinality = None;

        let mut owners = VecDeque::new();
        owners.push_front(owner.into_owned_object_type());

        while let Some(current_owner) = owners.pop_back() {
            if Self::do_instances_of_owns_violate_cardinality(
                snapshot,
                thing_manager,
                current_owner.clone(),
                attribute_type.clone(),
                cardinality,
            )? {
                type_with_instances_violating_cardinality = Some(current_owner.clone());
                break; // TODO: Maybe we want to return all the corrupted owns here, just moving forward for now
            }

            current_owner
                .get_subtypes(snapshot, type_manager)?
                .iter()
                .for_each(|subowner| owners.push_front(subowner.clone().into_owned_object_type()));
        }

        Ok(type_with_instances_violating_cardinality)
    }

    // TODO: Do we want to check subtypes in operation time?
    fn type_or_subtype_which_instances_violate_cardinality_of_plays<'a>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        player: ObjectType<'a>,
        role_type: RoleType<'a>,
        cardinality: AnnotationCardinality,
    ) -> Result<Option<ObjectType<'static>>, ConceptReadError> {
        let mut type_with_instances_violating_cardinality = None;

        let mut players = VecDeque::new();
        players.push_front(player.into_owned_object_type());

        while let Some(current_player) = players.pop_back() {
            if Self::do_instances_of_plays_violate_cardinality(
                snapshot,
                thing_manager,
                current_player.clone(),
                role_type.clone(),
                cardinality,
            )? {
                type_with_instances_violating_cardinality = Some(current_player.clone());
                break; // TODO: Maybe we want to return all the corrupted plays here, just moving forward for now
            }

            current_player
                .get_subtypes(snapshot, type_manager)?
                .iter()
                .for_each(|subplayer| players.push_front(subplayer.clone().into_owned_object_type()));
        }

        Ok(type_with_instances_violating_cardinality)
    }

    fn has_instances_of_owns<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: ObjectType<'a>,
        attribute_type: AttributeType<'a>,
    ) -> Result<bool, ConceptReadError> {
        let mut has_instances = false;
        match owner.clone() {
            ObjectType::Entity(entity_type) => {
                let mut iterator = thing_manager.get_entities_in(snapshot, entity_type.clone());
                while let Some(instance) = iterator.next() {
                    let mut iterator = instance?.get_has_type_unordered(
                        snapshot,
                        thing_manager,
                        attribute_type.clone().into_owned(),
                    )?;

                    if iterator.next().is_some() {
                        has_instances = true;
                        break;
                    }
                }
            }
            ObjectType::Relation(relation_type) => {
                let mut iterator = thing_manager.get_relations_in(snapshot, relation_type.clone());
                while let Some(instance) = iterator.next() {
                    let mut iterator = instance?.get_has_type_unordered(
                        snapshot,
                        thing_manager,
                        attribute_type.clone().into_owned(),
                    )?;

                    if iterator.next().is_some() {
                        has_instances = true;
                        break;
                    }
                }
            }
        }

        Ok(has_instances)
    }

    fn has_instances_of_plays<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        player: ObjectType<'a>,
        role_type: RoleType<'a>,
    ) -> Result<bool, ConceptReadError> {
        let mut has_instances = false;
        match player.clone() {
            ObjectType::Entity(entity_type) => {
                let mut iterator = thing_manager.get_entities_in(snapshot, entity_type.clone());
                while let Some(instance) = iterator.next() {
                    let mut iterator =
                        instance?.get_relations_by_role(snapshot, thing_manager, role_type.clone().into_owned());

                    if let Some(first) = iterator.next() {
                        first?;
                        has_instances = true;
                        break;
                    }
                }
            }
            ObjectType::Relation(relation_type) => {
                let mut iterator = thing_manager.get_relations_in(snapshot, relation_type.clone());
                while let Some(instance) = iterator.next() {
                    let mut iterator =
                        instance?.get_relations_by_role(snapshot, thing_manager, role_type.clone().into_owned());

                    if let Some(first) = iterator.next() {
                        first?;
                        has_instances = true;
                        break;
                    }
                }
            }
        }

        Ok(has_instances)
    }

    fn has_instances_of_relates<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType<'a>,
        role_type: RoleType<'a>,
    ) -> Result<bool, ConceptReadError> {
        let mut has_instances = false;

        let mut iterator = thing_manager.get_relations_in(snapshot, relation_type.clone());
        while let Some(instance) = iterator.next() {
            let mut iterator = instance?.get_players_role_type(snapshot, thing_manager, role_type.clone().into_owned());

            if let Some(first) = iterator.next() {
                first?;
                has_instances = true;
                break;
            }
        }

        Ok(has_instances)
    }

    fn do_instances_of_owns_violate_cardinality<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: ObjectType<'a>,
        attribute_type: AttributeType<'a>,
        cardinality: AnnotationCardinality,
    ) -> Result<bool, ConceptReadError> {
        let mut cardinality_violated = false;
        match owner.clone() {
            ObjectType::Entity(entity_type) => {
                let mut iterator = thing_manager.get_entities_in(snapshot, entity_type.clone());
                while let Some(instance) = iterator.next() {
                    let mut iterator = instance?.get_has_type_unordered(
                        snapshot,
                        thing_manager,
                        attribute_type.clone().into_owned(),
                    )?;

                    if !cardinality.value_valid(iterator.count() as u64) {
                        cardinality_violated = true;
                        break;
                    }
                }
            }
            ObjectType::Relation(relation_type) => {
                let mut iterator = thing_manager.get_relations_in(snapshot, relation_type.clone());
                while let Some(instance) = iterator.next() {
                    let mut iterator = instance?.get_has_type_unordered(
                        snapshot,
                        thing_manager,
                        attribute_type.clone().into_owned(),
                    )?;

                    if !cardinality.value_valid(iterator.count() as u64) {
                        cardinality_violated = true;
                        break;
                    }
                }
            }
        }

        Ok(cardinality_violated)
    }

    fn do_instances_of_plays_violate_cardinality<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        player: ObjectType<'a>,
        role_type: RoleType<'a>,
        cardinality: AnnotationCardinality,
    ) -> Result<bool, ConceptReadError> {
        let mut cardinality_violated = false;
        match player.clone() {
            ObjectType::Entity(entity_type) => {
                let mut iterator = thing_manager.get_entities_in(snapshot, entity_type.clone());
                while let Some(instance) = iterator.next() {
                    // let mut iterator = instance?.get_has_type_unordered(snapshot, thing_manager, role_type.clone().into_owned())?; // TODO: get_roleplayer...?
                    //
                    // if !cardinality.value_valid(iterator.count() as u64) {
                    //     cardinality_violated = true;
                    //     break;
                    // }
                }
            }
            ObjectType::Relation(relation_type) => {
                let mut iterator = thing_manager.get_relations_in(snapshot, relation_type.clone());
                while let Some(instance) = iterator.next() {
                    // let mut iterator = instance?.get_has_type_unordered(snapshot, thing_manager, role_type.clone().into_owned())?; // TODO: get_roleplayer...?
                    //
                    // if !cardinality.value_valid(iterator.count() as u64) {
                    //     cardinality_violated = true;
                    //     break;
                    // }
                }
            }
        }

        Ok(cardinality_violated)
    }

    fn get_lost_capabilities_if_supertype_is_changed<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: CAP::ObjectType,
        new_supertype: CAP::ObjectType,
    ) -> Result<HashSet<CAP>, SchemaValidationError> {
        let new_inherited_capabilities = TypeReader::get_capabilities::<CAP>(snapshot, new_supertype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        let current_capabilities =
            TypeReader::get_capabilities::<CAP>(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?;
        let current_inherited_capabilities =
            current_capabilities.values().filter(|capability| capability.object() != type_);

        Ok(current_inherited_capabilities
            .filter(|capability| !new_inherited_capabilities.contains_key(&capability.interface()))
            .map(|capability| capability.clone())
            .collect())
    }

    fn get_capabilities_with_modified_cardinality_if_supertype_is_changed<CAP: Capability<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: CAP::ObjectType,
        new_supertype: CAP::ObjectType,
    ) -> Result<HashMap<CAP, AnnotationCardinality>, SchemaValidationError> {
        let new_inherited_capabilities = TypeReader::get_capabilities::<CAP>(snapshot, new_supertype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        let current_capabilities =
            TypeReader::get_capabilities::<CAP>(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?;
        let current_inherited_capabilities =
            current_capabilities.values().filter(|capability| capability.object() != type_);

        current_inherited_capabilities
            .filter_map(|capability| {
                if let Some(new_capability) = new_inherited_capabilities.get(&capability.interface()) {
                    match (
                        capability.get_cardinality(snapshot, type_manager),
                        new_capability.get_cardinality(snapshot, type_manager),
                    ) {
                        (Ok(old_cardinality), Ok(new_cardinality)) => {
                            if old_cardinality != new_cardinality {
                                Some(Ok((capability.clone(), new_cardinality)))
                            } else {
                                None
                            }
                        }
                        (Err(e), _) => Some(Err(SchemaValidationError::ConceptRead(e))),
                        (_, Err(e)) => Some(Err(SchemaValidationError::ConceptRead(e))),
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn validate_deleted_struct_is_not_used(
        snapshot: &impl ReadableSnapshot,
        definition_key: &DefinitionKey<'static>,
    ) -> Result<(), SchemaValidationError> {
        let struct_definition = TypeReader::get_struct_definition(snapshot, definition_key.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        let usages_in_attribute_types = TypeReader::get_struct_definition_usages_in_attribute_types(snapshot)
            .map_err(SchemaValidationError::ConceptRead)?;
        if let Some(owners) = usages_in_attribute_types.get(definition_key) {
            return Err(SchemaValidationError::StructCannotBeDeletedAsItsUsedAsValueTypeForAttributeTypes(
                struct_definition.name,
                owners.len(),
            ));
        }

        let usages_in_struct_definition_fields =
            TypeReader::get_struct_definition_usages_in_struct_definitions(snapshot)
                .map_err(SchemaValidationError::ConceptRead)?;
        if let Some(owners) = usages_in_struct_definition_fields.get(definition_key) {
            return Err(SchemaValidationError::StructCannotBeDeletedAsItsUsedAsValueTypeForStructs(
                struct_definition.name,
                owners.len(),
            ));
        }

        Ok(())
    }

    type_or_subtype_capabilities_instances_existence_validation!(
        type_or_subtype_that_has_instances_of_owns,
        Owns,
        ObjectType,
        AttributeType,
        Self::has_instances_of_owns
    );
    type_or_subtype_capabilities_instances_existence_validation!(
        type_or_subtype_that_has_instances_of_plays,
        Plays,
        ObjectType,
        RoleType,
        Self::has_instances_of_plays
    );
    type_or_subtype_capabilities_instances_existence_validation!(
        type_or_subtype_that_has_instances_of_relates,
        Relates,
        RelationType,
        RoleType,
        Self::has_instances_of_relates
    );

    cannot_unset_capability_with_existing_instances_validation!(
        validate_no_instances_to_unset_owns,
        CapabilityKind::Owns,
        ObjectType,
        AttributeType,
        Self::type_or_subtype_that_has_instances_of_owns
    );
    cannot_unset_capability_with_existing_instances_validation!(
        validate_no_instances_to_unset_plays,
        CapabilityKind::Plays,
        Plays,
        ObjectType,
        RoleType,
        Self::type_or_subtype_that_has_instances_of_plays
    );
    cannot_unset_capability_with_existing_instances_validation!(
        validate_no_instances_to_unset_relates,
        CapabilityKind::Relates,
        Relates,
        RelationType,
        RoleType,
        Self::type_or_subtype_that_has_instances_of_relates
    );

    cannot_override_capability_with_existing_instances_validation!(
        validate_no_instances_to_override_owns,
        CapabilityKind::Owns,
        ObjectType,
        AttributeType,
        Self::type_or_subtype_that_has_instances_of_owns
    );
    cannot_override_capability_with_existing_instances_validation!(
        validate_no_instances_to_override_plays,
        CapabilityKind::Plays,
        ObjectType,
        RoleType,
        Self::type_or_subtype_that_has_instances_of_plays
    );
    cannot_override_capability_with_existing_instances_validation!(
        validate_no_instances_to_override_relates,
        CapabilityKind::Relates,
        RelationType,
        RoleType,
        Self::type_or_subtype_that_has_instances_of_relates
    );

    cannot_change_supertype_as_capability_with_existing_instances_is_lost_validation!(
        validate_lost_owns_do_not_cause_lost_instances_while_changing_supertype,
        CapabilityKind::Owns,
        ObjectType,
        Self::get_lost_capabilities_if_supertype_is_changed::<Owns<'static>>,
        Self::type_or_subtype_that_has_instances_of_owns
    );
    cannot_change_supertype_as_capability_with_existing_instances_is_lost_validation!(
        validate_lost_plays_do_not_cause_lost_instances_while_changing_supertype,
        CapabilityKind::Plays,
        ObjectType,
        Self::get_lost_capabilities_if_supertype_is_changed::<Plays<'static>>,
        Self::type_or_subtype_that_has_instances_of_plays
    );
    cannot_change_supertype_as_capability_with_existing_instances_is_lost_validation!(
        validate_lost_relates_do_not_cause_lost_instances_while_changing_supertype,
        CapabilityKind::Relates,
        RelationType,
        Self::get_lost_capabilities_if_supertype_is_changed::<Relates<'static>>,
        Self::type_or_subtype_that_has_instances_of_relates
    );
}
