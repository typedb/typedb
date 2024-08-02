/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use encoding::{
    graph::{definition::definition_key::DefinitionKey, thing::edge::ThingEdgeRolePlayer, type_::CapabilityKind},
    value::{label::Label, value_type::ValueType},
};
use itertools::Itertools;
use encoding::graph::type_::Kind;
use lending_iterator::LendingIterator;
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::{
    thing::{relation::RolePlayerIterator, thing_manager::ThingManager, ThingAPI},
    type_::{
        annotation::{
            Annotation, AnnotationCardinality, AnnotationCategory, AnnotationRange, AnnotationRegex, AnnotationValues,
        },
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        entity_type::EntityType,
        object_type::ObjectType,
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
                    get_label_or_concept_read_err, get_label_or_schema_err, is_interface_overridden,
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
        Capability, KindAPI, ObjectTypeAPI, Ordering, TypeAPI,
    },
};

macro_rules! object_type_match {
    ($obj_var:ident, $block:block) => {
        match &$obj_var {
            ObjectType::Entity($obj_var) => $block
            ObjectType::Relation($obj_var) => $block
        }
    };
}

pub struct OperationTimeValidation {}

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
            .is_some() {
            Err(SchemaValidationError::LabelShouldBeUnique { label: new_label.clone(), existing_kind: Kind::Attribute })
        } else if TypeReader::get_labelled_type::<RelationType<'static>>(snapshot, new_label)
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some() {
            Err(SchemaValidationError::LabelShouldBeUnique { label: new_label.clone(), existing_kind: Kind::Relation })
        } else if TypeReader::get_labelled_type::<EntityType<'static>>(snapshot, new_label)
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some() {
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
            if is_interface_overridden::<Owns<'static>>(snapshot, owner_supertype.clone(), attribute_type.clone())
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

    pub(crate) fn validate_owns_overrides_compatible_with_new_supertype<T>(
        snapshot: &impl ReadableSnapshot,
        owner_subtype: T,
        owner_supertype: T,
    ) -> Result<(), SchemaValidationError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
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
            if is_interface_overridden::<Plays<'static>>(snapshot, player_supertype.clone(), role_type.clone())
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

    pub(crate) fn validate_plays_overrides_compatible_with_new_supertype<T>(
        snapshot: &impl ReadableSnapshot,
        player_subtype: T,
        player_supertype: T,
    ) -> Result<(), SchemaValidationError>
    where
        T: ObjectTypeAPI<'static> + KindAPI<'static>,
    {
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
        let is_inherited = object_type_match!(owner, {
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
        if !is_interface_overridden::<CAP>(snapshot, object_type.clone(), interface_type.clone())
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
        let is_inherited = object_type_match!(player, {
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

    pub fn validate_value_type_compatible_with_all_owns_annotations(
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

    // TODO: Refactor / implement and include into type_manager
    pub(crate) fn validate_exact_type_no_instances_entity(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        entity_type: EntityType<'_>,
    ) -> Result<(), SchemaValidationError> {
        let mut entity_iterator = thing_manager.get_entities_in(snapshot, entity_type.clone().into_owned());
        match entity_iterator.next() {
            None => Ok(()),
            Some(Ok(_)) => Err(SchemaValidationError::CannotDeleteTypeWithExistingInstances(get_label_or_schema_err(
                snapshot,
                entity_type,
            )?)),
            Some(Err(concept_read_error)) => Err(SchemaValidationError::ConceptRead(concept_read_error)),
        }
    }

    // TODO: Refactor / implement and include into type_manager
    pub(crate) fn validate_exact_type_no_instances_relation(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType<'_>,
    ) -> Result<(), SchemaValidationError> {
        let mut relation_iterator = thing_manager.get_relations_in(snapshot, relation_type.clone().into_owned());
        match relation_iterator.next() {
            None => Ok(()),
            Some(Ok(_)) => Err(SchemaValidationError::CannotDeleteTypeWithExistingInstances(get_label_or_schema_err(
                snapshot,
                relation_type,
            )?)),
            Some(Err(concept_read_error)) => Err(SchemaValidationError::ConceptRead(concept_read_error)),
        }
    }

    // TODO: Refactor / implement and include into type_manager
    pub(crate) fn validate_exact_type_no_instances_attribute(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'_>,
    ) -> Result<(), SchemaValidationError> {
        let mut attribute_iterator = thing_manager
            .get_attributes_in(snapshot, attribute_type.clone())
            .map_err(|err| SchemaValidationError::ConceptRead(err.clone()))?;
        match attribute_iterator.next() {
            None => Ok(()),
            Some(Ok(_)) => Err(SchemaValidationError::CannotDeleteTypeWithExistingInstances(get_label_or_schema_err(
                snapshot,
                attribute_type,
            )?)),
            Some(Err(err)) => Err(SchemaValidationError::ConceptRead(err.clone())),
        }
    }

    // TODO: Refactor / implement and include into type_manager
    pub(crate) fn validate_exact_type_no_instances_role(
        snapshot: &impl ReadableSnapshot,
        _thing_manager: &ThingManager,
        role_type: RoleType<'_>,
    ) -> Result<(), SchemaValidationError> {
        // TODO: See if we can use existing methods from the ThingManager
        let relation_type = TypeReader::get_role_type_relates_declared(snapshot, role_type.clone().into_owned())
            .map_err(SchemaValidationError::ConceptRead)?
            .relation();
        let mut relation_iterator = _thing_manager.get_relations(snapshot);
        while let Some(result) = relation_iterator.next() {
            let relation_instance = result.map_err(SchemaValidationError::ConceptRead)?;
            let prefix = ThingEdgeRolePlayer::prefix_from_relation(relation_instance.into_vertex());
            let mut role_player_iterator = RolePlayerIterator::new(
                snapshot.iterate_range(KeyRange::new_within(prefix, ThingEdgeRolePlayer::FIXED_WIDTH_ENCODING)),
            );
            match role_player_iterator.next() {
                None => {}
                Some(Ok(_)) => {
                    let role_type_clone = role_type.clone();
                    Err(SchemaValidationError::CannotDeleteTypeWithExistingInstances(get_label_or_schema_err(
                        snapshot,
                        role_type_clone,
                    )?))?;
                }
                Some(Err(concept_read_error)) => {
                    Err(SchemaValidationError::ConceptRead(concept_read_error))?;
                }
            }
        }
        Ok(())
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
}
