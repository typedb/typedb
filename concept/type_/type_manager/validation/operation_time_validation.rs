/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, hash::Hash};

use encoding::{
    graph::{
        thing::{edge::ThingEdgeRolePlayer, vertex_object::ObjectVertex},
        type_::edge::TypeEdgeEncoding,
        Typed,
    },
    layout::prefix::Prefix,
    value::{label::Label, value_type::ValueType},
};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::{
    thing::{
        object::ObjectAPI,
        relation::{RelationIterator, RolePlayerIterator},
        thing_manager::ThingManager,
    },
    type_::{
        annotation::{
            Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCategory, AnnotationKey, AnnotationRange,
            AnnotationRegex, AnnotationValues,
        },
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        entity_type::EntityType,
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::{type_reader::TypeReader, validation::SchemaValidationError},
        InterfaceImplementation, KindAPI, ObjectTypeAPI, Ordering, TypeAPI,
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

macro_rules! get_label {
    ($snapshot: ident, $type_:ident) => {
        TypeReader::get_label($snapshot, $type_).unwrap().unwrap()
    };
}

pub struct OperationTimeValidation {}

impl OperationTimeValidation {
    pub(crate) fn validate_type_exists(
        snapshot: &impl ReadableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<(), SchemaValidationError> {
        TypeReader::get_label(snapshot, type_).map_err(|err| SchemaValidationError::ConceptRead(err))?;
        Ok(())
    }

    pub(crate) fn validate_can_modify_type<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<(), SchemaValidationError> {
        let label = TypeReader::get_label(snapshot, type_).map_err(SchemaValidationError::ConceptRead)?.unwrap();
        let is_root = TypeReader::check_type_is_root(&label, T::ROOT_KIND);
        if is_root {
            Err(SchemaValidationError::CannotModifyRoot)
        } else {
            Ok(())
        }
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
            Err(SchemaValidationError::CannotDeleteTypeWithExistingSubtypes(get_label!(snapshot, type_)))
        }
    }

    pub(crate) fn validate_no_abstract_attribute_types_owned_to_unset_abstractness<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<(), SchemaValidationError>
    where
        T: ObjectTypeAPI<'static>,
    {
        TypeReader::get_implemented_interfaces_declared(snapshot, type_)
            .map_err(SchemaValidationError::ConceptRead)?
            .iter()
            .map(Owns::attribute)
            .try_for_each(|attribute_type: AttributeType<'static>| {
                if Self::type_is_abstract(snapshot, attribute_type.clone())? {
                    Err(SchemaValidationError::CannotUnsetAbstractnessAsItOwnsAbstractTypes(get_label!(
                        snapshot,
                        attribute_type
                    )))
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
        let attribute_clash = TypeReader::get_labelled_type::<AttributeType<'static>>(snapshot, &new_label)
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some();
        let relation_clash = TypeReader::get_labelled_type::<RelationType<'static>>(snapshot, &new_label)
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some();
        let entity_clash = TypeReader::get_labelled_type::<EntityType<'static>>(snapshot, &new_label)
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some();
        // TODO: Check struct clash?

        if attribute_clash || relation_clash || entity_clash {
            Err(SchemaValidationError::LabelShouldBeUnique(new_label.clone()))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_role_name_uniqueness<'a>(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        label: &Label<'static>,
    ) -> Result<(), SchemaValidationError> {
        let existing_relation_supertypes = TypeReader::get_supertypes(snapshot, relation_type.clone().into_owned())
            .map_err(SchemaValidationError::ConceptRead)?;

        Self::validate_role_name_uniqueness_non_transitive(snapshot, relation_type, label)?;
        for relation_supertype in existing_relation_supertypes {
            Self::validate_role_name_uniqueness_non_transitive(snapshot, relation_supertype, label)?;
        }

        Ok(())
    }

    fn validate_role_name_uniqueness_non_transitive<'a>(
        snapshot: &impl ReadableSnapshot,
        relation_type: RelationType<'static>,
        new_label: &Label<'static>,
    ) -> Result<(), SchemaValidationError> {
        let scoped_label = Label::build_scoped(
            new_label.name.as_str(),
            TypeReader::get_label(snapshot, relation_type).unwrap().unwrap().name().as_str(),
        );

        if TypeReader::get_labelled_type::<RoleType<'static>>(snapshot, &scoped_label)
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some()
        {
            Err(SchemaValidationError::RoleNameShouldBeUnique(new_label.clone()))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_struct_name_uniqueness<'a>(
        snapshot: &impl ReadableSnapshot,
        name: &String,
    ) -> Result<(), SchemaValidationError> {
        let struct_clash = TypeReader::get_struct_definition_key(snapshot, name.as_str())
            .map_err(SchemaValidationError::ConceptRead)?
            .is_some();
        // TODO: Check other types clash?

        if struct_clash {
            Err(SchemaValidationError::StructNameShouldBeUnique(name.clone()))
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
                    Err(SchemaValidationError::CannotChangeValueTypeOfAttributeType(
                        get_label!(snapshot, subtype),
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
                    return Ok(());
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
        let is_abstract = abstract_set.unwrap_or(Self::type_has_annotation_category(
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
                    Err(SchemaValidationError::AttributeTypeWithoutValueTypeShouldBeAbstract(get_label!(
                        snapshot,
                        attribute_type
                    )))
                }
            }
        }
    }

    pub(crate) fn validate_annotation_regex_compatible_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        // TODO: Move to AnnotationRegex "value_type_valid"
        let is_compatible = match &value_type {
            Some(ValueType::String) => true,
            _ => false,
        };

        if is_compatible {
            Ok(())
        } else {
            Err(SchemaValidationError::ValueTypeIsNotCompatibleWithRegexAnnotation(
                get_label!(snapshot, attribute_type),
                value_type,
            ))
        }
    }

    pub(crate) fn validate_annotation_range_compatible_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        // TODO: Move to AnnotationRange "value_type_valid"
        let is_compatible = match &value_type {
            Some(value_type) => match &value_type {
                | ValueType::Boolean
                | ValueType::Long
                | ValueType::Double
                | ValueType::Decimal
                | ValueType::Date
                | ValueType::DateTime
                | ValueType::DateTimeTZ
                | ValueType::String => true,

                | ValueType::Duration | ValueType::Struct(_) => false,
            },
            _ => false,
        };

        if is_compatible {
            Ok(())
        } else {
            Err(SchemaValidationError::ValueTypeIsNotCompatibleWithRangeAnnotation(
                get_label!(snapshot, attribute_type),
                value_type,
            ))
        }
    }

    pub(crate) fn validate_annotation_values_compatible_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        // TODO: Move to AnnotationValues "value_type_valid"
        let is_compatible = match &value_type {
            Some(value_type) => match &value_type {
                | ValueType::Boolean
                | ValueType::Long
                | ValueType::Double
                | ValueType::Decimal
                | ValueType::Date
                | ValueType::DateTime
                | ValueType::DateTimeTZ
                | ValueType::Duration
                | ValueType::String => true,

                | ValueType::Struct(_) => false,
            },
            _ => false,
        };

        if is_compatible {
            Ok(())
        } else {
            Err(SchemaValidationError::ValueTypeIsNotCompatibleWithValuesAnnotation(
                get_label!(snapshot, attribute_type),
                value_type,
            ))
        }
    }

    pub(crate) fn validate_annotation_set_only_for_interface<IMPL>(
        snapshot: &impl ReadableSnapshot,
        interface: IMPL::InterfaceType,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError>
    where
        IMPL: InterfaceImplementation<'static, ObjectType = ObjectType<'static>> + Hash + Eq,
    {
        let implementations = TypeReader::get_implementations_for_interface::<IMPL>(snapshot, interface.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for (_, implementation) in implementations {
            let implementation_annotations = TypeReader::get_type_edge_annotations(snapshot, implementation)
                .map_err(SchemaValidationError::ConceptRead)?;
            if implementation_annotations
                .iter()
                .map(|(annotation, _)| annotation.category())
                .contains(&annotation_category)
            {
                return Err(
                    SchemaValidationError::CannotSetAnnotationToInterfaceBecauseItAlreadyExistsForItsImplementation(
                        get_label!(snapshot, interface),
                        annotation_category,
                    ),
                );
            }
        }

        Ok(())
    }

    pub(crate) fn validate_annotation_set_only_for_interface_implementation<IMPL>(
        snapshot: &impl ReadableSnapshot,
        interface_implementation: IMPL,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError>
    where
        IMPL: InterfaceImplementation<'static, ObjectType = ObjectType<'static>> + Hash + Eq,
    {
        let interface = interface_implementation.interface();
        let interface_annotations = TypeReader::get_type_annotations(snapshot, interface.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        if interface_annotations
            .iter()
            .map(|(annotation, _)| annotation.clone().into().category())
            .contains(&annotation_category)
        {
            return Err(SchemaValidationError::CannotSetAnnotationToInterfaceImplementationBecauseItAlreadyExistsForItsInterface(
                get_label!(snapshot, interface), annotation_category,
            ));
        }

        Ok(())
    }

    pub(crate) fn validate_attribute_type_supertype_is_abstract<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<(), SchemaValidationError> {
        if Self::type_is_abstract(snapshot, type_.clone())? {
            Ok(())
        } else {
            Err(SchemaValidationError::AttributeTypeSupertypeIsNotAbstract(get_label!(snapshot, type_)))
        }
    }

    pub(crate) fn type_is_abstract<T>(snapshot: &impl ReadableSnapshot, type_: T) -> Result<bool, SchemaValidationError>
    where
        T: KindAPI<'static>,
    {
        Self::type_has_declared_annotation(snapshot, type_.clone(), Annotation::Abstract(AnnotationAbstract))
    }

    pub(crate) fn validate_ownership_abstractness(
        snapshot: &impl ReadableSnapshot,
        owner: impl KindAPI<'static>,
        attribute: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let is_owner_abstract = Self::type_is_abstract(snapshot, owner.clone())?;
        let is_attribute_abstract = Self::type_is_abstract(snapshot, attribute.clone())?;

        match (&is_owner_abstract, &is_attribute_abstract) {
            (true, true) | (false, false) | (true, false) => Ok(()),
            (false, true) => Err(SchemaValidationError::NonAbstractCannotOwnAbstract(
                get_label!(snapshot, owner),
                get_label!(snapshot, attribute),
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

    pub(crate) fn validate_range_arguments(range: AnnotationRange) -> Result<(), SchemaValidationError> {
        if range.valid() {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidRangeArguments(range))
        }
    }

    pub(crate) fn validate_values_arguments(values: AnnotationValues) -> Result<(), SchemaValidationError> {
        if values.valid() {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidValuesArguments(values))
        }
    }

    pub(crate) fn validate_key_narrows_inherited_cardinality<EDGE>(
        snapshot: &impl ReadableSnapshot,
        supertype_edge: EDGE,
    ) -> Result<(), SchemaValidationError>
    where
        EDGE: InterfaceImplementation<'static> + Clone,
    {
        match Self::get_conflicted_cardinality(snapshot, supertype_edge.clone(), AnnotationKey::CARDINALITY)? {
            Some(conflicted_cardinality) => {
                Err(SchemaValidationError::KeyShouldNarrowInheritedCardinality(conflicted_cardinality.clone()))
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_cardinality_narrows_inherited_cardinality<EDGE>(
        snapshot: &impl ReadableSnapshot,
        supertype_edge: EDGE,
        cardinality: AnnotationCardinality,
    ) -> Result<(), SchemaValidationError>
    where
        EDGE: InterfaceImplementation<'static> + Clone,
    {
        match Self::get_conflicted_cardinality(snapshot, supertype_edge.clone(), cardinality.clone())? {
            Some(conflicted_cardinality) => Err(SchemaValidationError::CardinalityShouldNarrowInheritedCardinality(
                cardinality.clone(),
                conflicted_cardinality.clone(),
            )),
            None => Ok(()),
        }
    }

    fn get_conflicted_cardinality<EDGE, Snapshot>(
        snapshot: &Snapshot,
        supertype_edge: EDGE,
        cardinality: AnnotationCardinality,
    ) -> Result<Option<AnnotationCardinality>, SchemaValidationError>
    where
        Snapshot: ReadableSnapshot,
        EDGE: InterfaceImplementation<'static> + Clone,
    {
        if let Some(supertype_annotation) =
            Self::edge_get_annotation_by_category(snapshot, supertype_edge, AnnotationCategory::Cardinality)?
        {
            match supertype_annotation {
                Annotation::Cardinality(supertype_cardinality) => {
                    if supertype_cardinality.narrowed_correctly_by(&cardinality) {
                        Ok(None)
                    } else {
                        Ok(Some(supertype_cardinality))
                    }
                }
                _ => unreachable!("Should not reach it for Cardinality-related function"),
            }
        } else {
            Ok(None)
        }
    }

    pub(crate) fn validate_type_regex_narrows_inherited_regex(
        snapshot: &impl ReadableSnapshot,
        supertype: impl KindAPI<'static>,
        regex: AnnotationRegex,
    ) -> Result<(), SchemaValidationError> {
        if let Some(supertype_annotation) =
            Self::type_get_annotation_by_category(snapshot, supertype.clone(), AnnotationCategory::Regex)?
        {
            match supertype_annotation {
                Annotation::Regex(supertype_regex) => {
                    if supertype_regex.regex() == regex.regex() {
                        Ok(())
                    } else {
                        Err(SchemaValidationError::OnlyOneRegexCanBeSetForTypeHierarchy(
                            supertype_regex.clone(),
                            get_label!(snapshot, supertype),
                        ))
                    }
                }
                _ => unreachable!("Should not reach it for Regex-related function"),
            }
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_edge_regex_narrows_inherited_regex<EDGE>(
        snapshot: &impl ReadableSnapshot,
        supertype_edge: EDGE,
        regex: AnnotationRegex,
    ) -> Result<(), SchemaValidationError>
    where
        EDGE: InterfaceImplementation<'static> + Clone,
    {
        if let Some(supertype_annotation) =
            Self::edge_get_annotation_by_category(snapshot, supertype_edge.clone(), AnnotationCategory::Regex)?
        {
            match supertype_annotation {
                Annotation::Regex(supertype_regex) => {
                    if supertype_regex.regex() == regex.regex() {
                        Ok(())
                    } else {
                        let object = supertype_edge.object();
                        Err(SchemaValidationError::OnlyOneRegexCanBeSetForTypeHierarchy(
                            supertype_regex.clone(),
                            get_label!(snapshot, object),
                        ))
                    }
                }
                _ => unreachable!("Should not reach it for Regex-related function"),
            }
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_type_range_narrows_inherited_range(
        snapshot: &impl ReadableSnapshot,
        supertype: impl KindAPI<'static>,
        range: AnnotationRange,
    ) -> Result<(), SchemaValidationError> {
        if let Some(supertype_annotation) =
            Self::type_get_annotation_by_category(snapshot, supertype.clone(), AnnotationCategory::Range)?
        {
            match supertype_annotation {
                Annotation::Range(supertype_range) => {
                    if supertype_range.narrowed_correctly_by(&range) {
                        Ok(())
                    } else {
                        Err(SchemaValidationError::RangeShouldNarrowInheritedRange(
                            range.clone(),
                            supertype_range.clone(),
                        ))
                    }
                }
                _ => unreachable!("Should not reach it for Range-related function"),
            }
        } else {
            Ok(())
        }
    }

    // TODO: Wrap into macro all these similar checks?
    pub(crate) fn validate_edge_range_narrows_inherited_range<EDGE>(
        snapshot: &impl ReadableSnapshot,
        supertype_edge: EDGE,
        range: AnnotationRange,
    ) -> Result<(), SchemaValidationError>
    where
        EDGE: InterfaceImplementation<'static> + Clone,
    {
        if let Some(supertype_annotation) =
            Self::edge_get_annotation_by_category(snapshot, supertype_edge.clone(), AnnotationCategory::Range)?
        {
            match supertype_annotation {
                Annotation::Range(supertype_range) => {
                    if supertype_range.narrowed_correctly_by(&range) {
                        Ok(())
                    } else {
                        let object = supertype_edge.object();
                        Err(SchemaValidationError::RangeShouldNarrowInheritedRange(
                            range.clone(),
                            supertype_range.clone(),
                        ))
                    }
                }
                _ => unreachable!("Should not reach it for Range-related function"),
            }
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_type_values_narrows_inherited_values(
        snapshot: &impl ReadableSnapshot,
        supertype: impl KindAPI<'static>,
        values: AnnotationValues,
    ) -> Result<(), SchemaValidationError> {
        if let Some(supertype_annotation) =
            Self::type_get_annotation_by_category(snapshot, supertype.clone(), AnnotationCategory::Values)?
        {
            match supertype_annotation {
                Annotation::Values(supertype_values) => {
                    if supertype_values.narrowed_correctly_by(&values) {
                        Ok(())
                    } else {
                        Err(SchemaValidationError::ValuesShouldNarrowInheritedValues(
                            values.clone(),
                            supertype_values.clone(),
                        ))
                    }
                }
                _ => unreachable!("Should not reach it for Values-related function"),
            }
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_edge_values_narrows_inherited_values<EDGE>(
        snapshot: &impl ReadableSnapshot,
        supertype_edge: EDGE,
        values: AnnotationValues,
    ) -> Result<(), SchemaValidationError>
    where
        EDGE: InterfaceImplementation<'static> + Clone,
    {
        if let Some(supertype_annotation) =
            Self::edge_get_annotation_by_category(snapshot, supertype_edge.clone(), AnnotationCategory::Values)?
        {
            match supertype_annotation {
                Annotation::Values(supertype_values) => {
                    if supertype_values.narrowed_correctly_by(&values) {
                        Ok(())
                    } else {
                        let object = supertype_edge.object();
                        Err(SchemaValidationError::ValuesShouldNarrowInheritedValues(
                            values.clone(),
                            supertype_values.clone(),
                        ))
                    }
                }
                _ => unreachable!("Should not reach it for Values-related function"),
            }
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_type_supertype_abstractness<T>(
        snapshot: &impl ReadableSnapshot,
        subtype: T,
        supertype: T,
    ) -> Result<(), SchemaValidationError>
    where
        T: KindAPI<'static>,
    {
        let subtype_abstract =
            Self::type_has_declared_annotation_category(snapshot, subtype.clone(), AnnotationCategory::Abstract)?;
        let supertype_abstract =
            Self::type_has_declared_annotation_category(snapshot, supertype.clone(), AnnotationCategory::Abstract)?;

        match (subtype_abstract, supertype_abstract) {
            (false, false) | (false, true) | (true, true) => Ok(()),
            (true, false) => Err(SchemaValidationError::CannotSetNonAbstractSupertypeForAbstractType(
                get_label!(snapshot, supertype),
                get_label!(snapshot, subtype),
            )),
        }
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
        let distinct_set = distinct_set.unwrap_or(
            Self::edge_get_annotation_by_category(snapshot, relates, AnnotationCategory::Distinct)?.is_some(),
        );

        if Self::is_ordering_compatible_with_distinct_annotation(ordering, distinct_set) {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidOrderingForDistinctAnnotation(get_label!(snapshot, role)))
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
            .unwrap_or(Self::edge_get_annotation_by_category(snapshot, owns, AnnotationCategory::Distinct)?.is_some());

        if Self::is_ordering_compatible_with_distinct_annotation(ordering, distinct_set) {
            Ok(())
        } else {
            Err(SchemaValidationError::InvalidOrderingForDistinctAnnotation(get_label!(snapshot, attribute)))
        }
    }

    pub(crate) fn validate_role_supertype_ordering_match(
        snapshot: &impl ReadableSnapshot,
        subtype_role: RoleType<'static>,
        supertype_role: RoleType<'static>,
        set_subtype_role_ordering: Option<Ordering>,
    ) -> Result<(), SchemaValidationError> {
        let subtype_ordering = set_subtype_role_ordering.unwrap_or(
            TypeReader::get_type_ordering(snapshot, subtype_role.clone())
                .map_err(SchemaValidationError::ConceptRead)?,
        );
        let supertype_ordering = TypeReader::get_type_ordering(snapshot, supertype_role.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        if subtype_ordering == supertype_ordering {
            Ok(())
        } else {
            Err(SchemaValidationError::OrderingDoesNotMatchWithSupertype(
                get_label!(snapshot, subtype_role),
                get_label!(snapshot, supertype_role),
            ))
        }
    }

    pub(crate) fn validate_owns_override_ordering_match(
        snapshot: &impl ReadableSnapshot,
        subtype_owns: Owns<'static>,
        supertype_owns: Owns<'static>,
        set_subtype_owns_ordering: Option<Ordering>,
    ) -> Result<(), SchemaValidationError> {
        let subtype_ordering = set_subtype_owns_ordering.unwrap_or(
            TypeReader::get_type_edge_ordering(snapshot, subtype_owns.clone())
                .map_err(SchemaValidationError::ConceptRead)?,
        );
        let supertype_ordering = TypeReader::get_type_edge_ordering(snapshot, supertype_owns.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        if subtype_ordering == supertype_ordering {
            Ok(())
        } else {
            let subtype_attribute = subtype_owns.attribute();
            let supertype_attribute = supertype_owns.attribute();
            Err(SchemaValidationError::OrderingDoesNotMatchWithSupertype(
                get_label!(snapshot, subtype_attribute),
                get_label!(snapshot, supertype_attribute),
            ))
        }
    }

    pub(crate) fn validate_supertype_annotations_compatibility(
        snapshot: &impl ReadableSnapshot,
        subtype: impl KindAPI<'static>,
        supertype: impl KindAPI<'static>,
    ) -> Result<(), SchemaValidationError> {
        let subtype_declared_annotations =
            TypeReader::get_type_annotations_declared(snapshot, subtype).map_err(SchemaValidationError::ConceptRead)?;
        for subtype_annotation in subtype_declared_annotations {
            let subtype_annotation = subtype_annotation.clone().into();
            let category = subtype_annotation.category();
            Self::validate_set_annotation_is_compatible_with_inherited_annotations(
                snapshot,
                supertype.clone(),
                category,
            )?;

            match subtype_annotation {
                Annotation::Regex(regex) => {
                    Self::validate_type_regex_narrows_inherited_regex(snapshot, supertype.clone(), regex)?
                }
                Annotation::Range(range) => {
                    Self::validate_type_range_narrows_inherited_range(snapshot, supertype.clone(), range)?
                }
                Annotation::Values(values) => {
                    Self::validate_type_values_narrows_inherited_values(snapshot, supertype.clone(), values)?
                }
                | Annotation::Abstract(_)
                | Annotation::Distinct(_)
                | Annotation::Independent(_)
                | Annotation::Unique(_)
                | Annotation::Key(_)
                | Annotation::Cardinality(_)
                | Annotation::Cascade(_) => {}
            }
        }

        Ok(())
    }

    pub(crate) fn validate_edge_override_annotations_compatibility<EDGE>(
        snapshot: &impl ReadableSnapshot,
        edge: EDGE,
        overridden_edge: EDGE,
    ) -> Result<(), SchemaValidationError>
    where
        EDGE: TypeEdgeEncoding<'static> + InterfaceImplementation<'static> + Clone,
    {
        let subtype_declared_annotations = TypeReader::get_type_edge_annotations_declared(snapshot, edge)
            .map_err(SchemaValidationError::ConceptRead)?;
        for subtype_annotation in subtype_declared_annotations {
            let category = subtype_annotation.category();
            Self::validate_set_edge_annotation_is_compatible_with_inherited_annotations(
                snapshot,
                overridden_edge.clone(),
                category,
            )?;

            match subtype_annotation {
                Annotation::Cardinality(cardinality) => Self::validate_cardinality_narrows_inherited_cardinality(
                    snapshot,
                    overridden_edge.clone(),
                    cardinality,
                )?,
                Annotation::Key(_) => {
                    Self::validate_key_narrows_inherited_cardinality(snapshot, overridden_edge.clone())?
                }
                Annotation::Regex(regex) => {
                    Self::validate_edge_regex_narrows_inherited_regex(snapshot, overridden_edge.clone(), regex)?
                }
                Annotation::Range(range) => {
                    Self::validate_edge_range_narrows_inherited_range(snapshot, overridden_edge.clone(), range)?
                }
                Annotation::Values(values) => {
                    Self::validate_edge_values_narrows_inherited_values(snapshot, overridden_edge.clone(), values)?
                }
                | Annotation::Abstract(_)
                | Annotation::Distinct(_)
                | Annotation::Independent(_)
                | Annotation::Unique(_)
                | Annotation::Cascade(_) => {}
            }
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
                get_label!(snapshot, type_),
                get_label!(snapshot, supertype),
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
        if super_relation.is_none() {
            // TODO: Handle better. This could be misleading.
            return Err(SchemaValidationError::CannotModifyRoot);
        }
        let is_inherited = TypeReader::get_relates(snapshot, super_relation.unwrap())
            .map_err(SchemaValidationError::ConceptRead)?
            .contains_key(&role_type);
        if is_inherited {
            Ok(())
        } else {
            Err(SchemaValidationError::RelatesNotInherited(relation_type, role_type))
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
                return Err(SchemaValidationError::CannotModifyRoot);
            }
            let owns_transitive: HashMap<AttributeType<'static>, Owns<'static>> =
                TypeReader::get_implemented_interfaces(snapshot, super_owner.unwrap())
                    .map_err(SchemaValidationError::ConceptRead)?;
            owns_transitive.contains_key(&attribute)
        });
        if is_inherited {
            Ok(())
        } else {
            Err(SchemaValidationError::OwnsNotInherited(owner, attribute))
        }
    }

    pub(crate) fn validate_overridden_is_supertype<T: KindAPI<'static>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        overridden: T,
    ) -> Result<(), SchemaValidationError> {
        if type_ == overridden {
            return Ok(());
        }

        let supertypes =
            TypeReader::get_supertypes(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?;

        if supertypes.contains(&overridden.clone()) {
            Ok(())
        } else {
            Err(SchemaValidationError::OverriddenInterfaceImplementationObjectIsNotSupertype(
                get_label!(snapshot, type_),
                get_label!(snapshot, overridden),
            ))
        }
    }

    pub(crate) fn validate_attribute_type_owns_not_overridden<T>(
        snapshot: &impl ReadableSnapshot,
        owner: T,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError>
    where
        T: ObjectTypeAPI<'static>,
    {
        let is_overridden = {
            let all_overridden = TypeReader::get_overridden_interfaces::<Owns<'static>, T>(snapshot, owner.clone())
                .map_err(SchemaValidationError::ConceptRead)?;
            all_overridden.contains_key(&attribute_type)
        };
        if !is_overridden {
            Ok(())
        } else {
            Err(SchemaValidationError::OverriddenOwnsCannotBeRedeclared(get_label!(snapshot, owner), attribute_type))
        }
    }

    pub(crate) fn validate_role_plays_not_overridden<T>(
        snapshot: &impl ReadableSnapshot,
        player: T,
        role_type: RoleType<'static>,
    ) -> Result<(), SchemaValidationError>
    where
        T: ObjectTypeAPI<'static>,
    {
        let is_overridden = {
            let all_overridden = TypeReader::get_overridden_interfaces::<Plays<'static>, T>(snapshot, player.clone())
                .map_err(SchemaValidationError::ConceptRead)?;
            all_overridden.contains_key(&role_type)
        };
        if !is_overridden {
            Ok(())
        } else {
            Err(SchemaValidationError::OverriddenPlaysCannotBeRedeclared(get_label!(snapshot, player), role_type))
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
                return Err(SchemaValidationError::CannotModifyRoot);
            }
            let plays_transitive: HashMap<RoleType<'static>, Plays<'static>> =
                TypeReader::get_implemented_interfaces(snapshot, super_player.unwrap())
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
                        Err(SchemaValidationError::ValueTypeNotCompatibleWithInheritedValueTypeOf(
                            get_label!(snapshot, attribute_type),
                            get_label!(snapshot, inherited_value_type_source),
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

    pub(crate) fn validate_value_type_compatible_with_subtypes_value_types(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: ValueType,
    ) -> Result<(), SchemaValidationError> {
        let subtypes = TypeReader::get_subtypes_transitive(snapshot, attribute_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for subtype in subtypes {
            let subtype_value_type_opt = TypeReader::get_value_type_declared(snapshot, subtype.clone())
                .map_err(SchemaValidationError::ConceptRead)?;
            if let Some(subtype_value_type) = subtype_value_type_opt {
                if subtype_value_type != value_type {
                    let subtype = subtype.clone();
                    return Err(SchemaValidationError::ValueTypeNotCompatibleWithInheritedValueTypeOf(
                        get_label!(snapshot, attribute_type),
                        get_label!(snapshot, subtype),
                        subtype_value_type,
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_when_attribute_type_loses_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        match value_type {
            Some(_) => {
                Self::validate_value_type_compatible_with_abstractness(snapshot, attribute_type.clone(), None, None)?;
                Self::validate_current_annotations_do_not_require_unset_value_type(snapshot, attribute_type.clone())?;
                // TODO: It should probably be in schema validation
                Self::validate_no_non_abstract_subtypes_without_value_type_for_value_type_unset(
                    snapshot,
                    attribute_type,
                )
                // TODO: Re-enable when we get the thing_manager
                // Self::validate_exact_type_no_instances_attribute(snapshot, self, attribute.clone())
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_no_non_abstract_subtypes_without_value_type_for_value_type_unset(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let subtypes = TypeReader::get_subtypes_transitive(snapshot, attribute_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        for subtype in subtypes {
            let no_value_type = TypeReader::get_value_type_declared(snapshot, subtype.clone())
                .map_err(SchemaValidationError::ConceptRead)?
                .is_none();

            if no_value_type
                && !Self::type_has_declared_annotation_category(
                    snapshot,
                    subtype.clone(),
                    AnnotationCategory::Abstract,
                )?
            {
                let subtype = subtype.clone();
                return Err(
                    SchemaValidationError::CannotUnsetValueTypeAsThereAreNonAbstractSubtypesWithoutDeclaredValueTypes(
                        get_label!(snapshot, attribute_type),
                        get_label!(snapshot, subtype),
                    ),
                );
            }
        }
        Ok(())
    }

    pub(crate) fn validate_current_annotations_do_not_require_unset_value_type(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let annotations = TypeReader::get_type_annotations(snapshot, attribute_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for (annotation, _) in annotations {
            match annotation {
                AttributeTypeAnnotation::Regex(_) => {
                    Self::validate_annotation_regex_compatible_value_type(snapshot, attribute_type.clone(), None)?
                }
                AttributeTypeAnnotation::Range(_) => {
                    Self::validate_annotation_range_compatible_value_type(snapshot, attribute_type.clone(), None)?
                }
                AttributeTypeAnnotation::Values(_) => {
                    Self::validate_annotation_values_compatible_value_type(snapshot, attribute_type.clone(), None)?
                }
                | AttributeTypeAnnotation::Abstract(_) | AttributeTypeAnnotation::Independent(_) => {}
            }
        }

        let attribute_owns =
            TypeReader::get_implementations_for_interface::<Owns<'static>>(snapshot, attribute_type.clone())
                .map_err(SchemaValidationError::ConceptRead)?;

        for (_, owns) in attribute_owns {
            let owns_annotations = TypeReader::get_type_edge_annotations(snapshot, owns.clone())
                .map_err(SchemaValidationError::ConceptRead)?;
            for (annotation, _) in owns_annotations {
                match annotation {
                    Annotation::Unique(_) => {
                        Self::validate_owns_value_type_compatible_to_unique_annotation(snapshot, owns.clone(), None)?
                    }
                    Annotation::Key(_) => {
                        Self::validate_owns_value_type_compatible_to_key_annotation(snapshot, owns.clone(), None)?
                    }
                    Annotation::Regex(_) => {
                        Self::validate_annotation_regex_compatible_value_type(snapshot, owns.attribute().clone(), None)?
                    }
                    Annotation::Range(_) => {
                        Self::validate_annotation_range_compatible_value_type(snapshot, owns.attribute().clone(), None)?
                    }
                    Annotation::Values(_) => Self::validate_annotation_values_compatible_value_type(
                        snapshot,
                        owns.attribute().clone(),
                        None,
                    )?,
                    | Annotation::Abstract(_)
                    | Annotation::Distinct(_)
                    | Annotation::Independent(_)
                    | Annotation::Cardinality(_)
                    | Annotation::Cascade(_) => {}
                }
            }
        }

        Ok(())
    }

    pub(crate) fn validate_unset_owns_is_not_inherited(
        snapshot: &impl ReadableSnapshot,
        owner: ObjectType<'static>,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), SchemaValidationError> {
        let all_owns: HashMap<AttributeType<'static>, Owns<'static>> =
            TypeReader::get_implemented_interfaces(snapshot, owner.clone())
                .map_err(SchemaValidationError::ConceptRead)?;
        let found_owns = all_owns
            .iter()
            .find(|(existing_owns_attribute_type, existing_owns)| **existing_owns_attribute_type == attribute_type);

        match found_owns {
            Some((_, owns)) => {
                if owner == owns.owner() {
                    Ok(())
                } else {
                    let owns_owner = owns.owner();
                    Err(SchemaValidationError::CannotUnsetInheritedOwns(
                        get_label!(snapshot, attribute_type),
                        get_label!(snapshot, owner),
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
            TypeReader::get_implemented_interfaces(snapshot, player.clone())
                .map_err(SchemaValidationError::ConceptRead)?;
        let found_plays =
            all_plays.iter().find(|(existing_plays_role_type, existing_plays)| **existing_plays_role_type == role_type);

        match found_plays {
            Some((_, plays)) => {
                if player == plays.player() {
                    Ok(())
                } else {
                    let plays_player = plays.player();
                    Err(SchemaValidationError::CannotUnsetInheritedPlays(
                        get_label!(snapshot, role_type),
                        get_label!(snapshot, plays_player),
                    ))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_unset_annotation_is_not_inherited<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError>
    where
        T: KindAPI<'static>,
    {
        let annotations =
            TypeReader::get_type_annotations(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?;
        let found_annotation = annotations
            .iter()
            .map(|(existing_annotation, source)| (existing_annotation.clone().into().category(), source))
            .find(|(existing_category, source)| existing_category.clone() == annotation_category);

        match found_annotation {
            Some((_, owner)) => {
                if type_ == owner.clone() {
                    Ok(())
                } else {
                    let owner = owner.clone();
                    Err(SchemaValidationError::CannotUnsetInheritedAnnotation(
                        annotation_category,
                        get_label!(snapshot, owner),
                    ))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_unset_edge_annotation_is_not_inherited<EDGE>(
        snapshot: &impl ReadableSnapshot,
        edge: EDGE,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError>
    where
        EDGE: TypeEdgeEncoding<'static> + InterfaceImplementation<'static> + Clone,
    {
        let annotations = TypeReader::get_type_edge_annotations(snapshot, edge.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        let found_annotation = annotations
            .iter()
            .map(|(existing_annotation, source)| (existing_annotation.clone().category(), source))
            .find(|(existing_category, source)| existing_category.clone() == annotation_category);

        match found_annotation {
            Some((_, owner)) => {
                if edge == *owner {
                    Ok(())
                } else {
                    let object = edge.object();
                    Err(SchemaValidationError::CannotUnsetInheritedEdgeAnnotation(
                        annotation_category,
                        get_label!(snapshot, object),
                    ))
                }
            }
            None => Ok(()),
        }
    }

    pub(crate) fn validate_set_annotation_is_compatible_with_inherited_annotations(
        snapshot: &impl ReadableSnapshot,
        type_: impl KindAPI<'static>,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError> {
        let existing_annotations =
            TypeReader::get_type_annotations(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?;

        for (existing_annotation, _) in existing_annotations {
            let existing_annotation_category = existing_annotation.clone().into().category();
            if !annotation_category.declarable_below(existing_annotation_category) {
                return Err(SchemaValidationError::DeclaredAnnotationIsNotCompatibleWithInheritedAnnotation(
                    annotation_category,
                    existing_annotation_category,
                    get_label!(snapshot, type_),
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn validate_set_edge_annotation_is_compatible_with_inherited_annotations<EDGE>(
        snapshot: &impl ReadableSnapshot,
        edge: EDGE,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError>
    where
        EDGE: TypeEdgeEncoding<'static> + InterfaceImplementation<'static> + Clone,
    {
        let existing_annotations = TypeReader::get_type_edge_annotations(snapshot, edge.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for (existing_annotation, _) in existing_annotations {
            let existing_annotation_category = existing_annotation.category();
            if !annotation_category.declarable_below(existing_annotation_category) {
                let interface = edge.interface();
                return Err(SchemaValidationError::DeclaredAnnotationIsNotCompatibleWithInheritedAnnotation(
                    annotation_category,
                    existing_annotation_category,
                    get_label!(snapshot, interface),
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn validate_set_annotation_is_compatible_with_declared_annotations(
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
                    get_label!(snapshot, type_),
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn validate_set_edge_annotation_is_compatible_with_declared_annotations<EDGE>(
        snapshot: &impl ReadableSnapshot,
        edge: EDGE,
        annotation_category: AnnotationCategory,
    ) -> Result<(), SchemaValidationError>
    where
        EDGE: TypeEdgeEncoding<'static> + InterfaceImplementation<'static> + Clone,
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
                    get_label!(snapshot, interface),
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn validate_owns_value_type_compatible_to_unique_annotation(
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
                get_label!(snapshot, owner),
                get_label!(snapshot, attribute_type),
                value_type,
            ))
        }
    }

    pub(crate) fn validate_owns_value_type_compatible_to_key_annotation(
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
                get_label!(snapshot, owner),
                get_label!(snapshot, attribute_type),
                value_type,
            ))
        }
    }

    pub fn validate_value_type_compatible_to_all_owns_annotations(
        snapshot: &impl ReadableSnapshot,
        attribute_type: AttributeType<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        let all_owns = TypeReader::get_implementations_for_interface::<Owns<'static>>(snapshot, attribute_type.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        for (_, owns) in all_owns {
            Self::validate_owns_value_type_compatible_to_annotations(snapshot, owns, value_type.clone())?
        }
        Ok(())
    }

    pub(crate) fn validate_owns_value_type_compatible_to_annotations(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
        value_type: Option<ValueType>,
    ) -> Result<(), SchemaValidationError> {
        let annotations = TypeReader::get_type_edge_annotations(snapshot, owns.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        for (annotation, _) in annotations {
            match annotation {
                Annotation::Unique(_) => Self::validate_owns_value_type_compatible_to_unique_annotation(
                    snapshot,
                    owns.clone(),
                    value_type.clone(),
                )?,
                Annotation::Key(_) => Self::validate_owns_value_type_compatible_to_key_annotation(
                    snapshot,
                    owns.clone(),
                    value_type.clone(),
                )?,
                Annotation::Regex(_) => Self::validate_annotation_regex_compatible_value_type(
                    snapshot,
                    owns.attribute().clone(),
                    value_type.clone(),
                )?,
                Annotation::Range(_) => Self::validate_annotation_range_compatible_value_type(
                    snapshot,
                    owns.attribute().clone(),
                    value_type.clone(),
                )?,
                Annotation::Values(_) => Self::validate_annotation_values_compatible_value_type(
                    snapshot,
                    owns.attribute().clone(),
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
        let mut entity_iterator = thing_manager.get_entities_in(snapshot, entity_type.clone());
        match entity_iterator.next() {
            None => Ok(()),
            Some(Ok(_)) => {
                Err(SchemaValidationError::CannotDeleteTypeWithExistingInstances(get_label!(snapshot, entity_type)))
            }
            Some(Err(concept_read_error)) => Err(SchemaValidationError::ConceptRead(concept_read_error)),
        }
    }

    // TODO: Refactor / implement and include into type_manager
    pub(crate) fn validate_exact_type_no_instances_relation(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation_type: RelationType<'_>,
    ) -> Result<(), SchemaValidationError> {
        let mut relation_iterator = thing_manager.get_relations_in(snapshot, relation_type.clone());
        match relation_iterator.next() {
            None => Ok(()),
            Some(Ok(_)) => {
                Err(SchemaValidationError::CannotDeleteTypeWithExistingInstances(get_label!(snapshot, relation_type)))
            }
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
            Some(Ok(_)) => {
                Err(SchemaValidationError::CannotDeleteTypeWithExistingInstances(get_label!(snapshot, attribute_type)))
            }
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
        let relation_type = TypeReader::get_role_type_relates(snapshot, role_type.clone().into_owned())
            .map_err(SchemaValidationError::ConceptRead)?
            .relation();
        let prefix =
            ObjectVertex::build_prefix_type(Prefix::VertexRelation.prefix_id(), relation_type.vertex().type_id_());
        let snapshot_iterator =
            snapshot.iterate_range(KeyRange::new_within(prefix, Prefix::VertexRelation.fixed_width_keys()));
        let mut relation_iterator = RelationIterator::new(snapshot_iterator);
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
                    Err(SchemaValidationError::CannotDeleteTypeWithExistingInstances(get_label!(
                        snapshot,
                        role_type_clone
                    )))?;
                }
                Some(Err(concept_read_error)) => {
                    Err(SchemaValidationError::ConceptRead(concept_read_error))?;
                }
            }
        }
        Ok(())
    }

    // TODO: Try to wrap all these type_has_***annotation and edge_has_***annotation into several macros!

    // TODO: Refactor to type_get_declared_annotaiton
    fn type_has_declared_annotation<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        annotation: Annotation,
    ) -> Result<bool, SchemaValidationError>
    where
        T: KindAPI<'static>,
    {
        let has = TypeReader::get_type_annotations_declared(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .contains(&T::AnnotationType::from(annotation));
        Ok(has)
    }

    fn type_has_declared_annotation_category<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        annotation_category: AnnotationCategory,
    ) -> Result<bool, SchemaValidationError>
    where
        T: KindAPI<'static>,
    {
        let has = TypeReader::get_type_annotations_declared(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .iter()
            .map(|annotation| annotation.clone().into().category())
            .any(|found_category| found_category == annotation_category);
        Ok(has)
    }

    fn type_has_annotation_category<T>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
        annotation_category: AnnotationCategory,
    ) -> Result<bool, SchemaValidationError>
    where
        T: KindAPI<'static>,
    {
        let has = TypeReader::get_type_annotations(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .iter()
            .map(|(annotation, _)| annotation.clone().into().category())
            .any(|found_category| found_category == annotation_category);
        Ok(has)
    }

    fn type_get_annotation_by_category(
        snapshot: &impl ReadableSnapshot,
        type_: impl KindAPI<'static>,
        annotation_category: AnnotationCategory,
    ) -> Result<Option<Annotation>, SchemaValidationError> {
        let annotation = TypeReader::get_type_annotations(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .map(|(found_annotation, _)| found_annotation)
            .find(|found_annotation| found_annotation.clone().into().category() == annotation_category);
        Ok(annotation.map(|val| val.clone().into()))
    }

    fn edge_get_annotation_by_category<EDGE>(
        snapshot: &impl ReadableSnapshot,
        edge: EDGE,
        annotation_category: AnnotationCategory,
    ) -> Result<Option<Annotation>, SchemaValidationError>
    where
        EDGE: InterfaceImplementation<'static> + Clone,
    {
        let annotation = TypeReader::get_type_edge_annotations(snapshot, edge.clone())
            .map_err(SchemaValidationError::ConceptRead)?
            .into_iter()
            .map(|(found_annotation, _)| found_annotation)
            .find(|found_annotation| found_annotation.category() == annotation_category);
        Ok(annotation.map(|val| val.clone()))
    }

    fn is_ordering_compatible_with_distinct_annotation(ordering: Ordering, distinct_set: bool) -> bool {
        if distinct_set {
            ordering == Ordering::Ordered
        } else {
            true
        }
    }
}
