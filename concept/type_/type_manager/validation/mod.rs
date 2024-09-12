/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use encoding::{
    graph::type_::{CapabilityKind, Kind},
    value::{label::Label, value_type::ValueType},
};

use crate::{
    error::ConceptReadError,
    thing::thing_manager::validation::DataValidationError,
    type_::{
        annotation::{AnnotationCardinality, AnnotationCategory, AnnotationRange, AnnotationRegex, AnnotationValues},
        constraint::{ConstraintDescription, ConstraintError},
        relates::Relates,
        relation_type::RelationType,
        role_type::RoleType,
        Ordering,
    },
};

pub(crate) mod commit_time_validation;
pub(crate) mod operation_time_validation;
pub(crate) mod validation;

#[derive(Debug, Clone)]
pub enum SchemaValidationError {
    ConceptRead(ConceptReadError),
    LabelShouldBeUnique {
        label: Label<'static>,
        existing_kind: Kind,
    },
    StructNameShouldBeUnique(String),
    StructShouldHaveAtLeastOneField(String),
    StructCannotBeDeletedAsItsUsedAsValueTypeForAttributeTypes(String, usize),
    StructCannotBeDeletedAsItsUsedAsValueTypeForStructs(String, usize),
    RoleNameShouldBeUniqueForRelationTypeHierarchy(Label<'static>, Label<'static>),
    CycleFoundInTypeHierarchy(Label<'static>, Label<'static>),
    ChangingAttributeTypeSupertypeWillLeadToConflictingValueTypes(Label<'static>, Option<ValueType>, ValueType),
    CannotUnsetAbstractnessOfAttributeTypeAsItHasSubtypes(Label<'static>),
    CannotDeleteTypeWithExistingSubtypes(Label<'static>),
    RelatesNotInherited(RelationType<'static>, RoleType<'static>),
    AttributeTypeSupertypeIsNotAbstract(Label<'static>),
    AbstractTypesSupertypeHasToBeAbstract(Label<'static>, Label<'static>),
    CannotUnsetRelatesAbstractnessAsItIsASpecialisingRelates(Relates<'static>),
    OrderingDoesNotMatchWithSupertype(Label<'static>, Label<'static>, Ordering, Ordering),
    OrderingDoesNotMatchWithCapabilityOfSubtypeInterface(
        Label<'static>,
        Label<'static>,
        Label<'static>,
        Ordering,
        Ordering,
    ),
    SpecialisingRelatesIsNotAbstract(Label<'static>, Label<'static>),
    CannotSetOwnsBecauseItIsAlreadySetWithDifferentOrdering(Label<'static>, Label<'static>, Ordering),
    InvalidOrderingForDistinctConstraint(Label<'static>),
    AttributeTypeWithoutValueTypeShouldBeAbstract(Label<'static>),
    ValueTypeIsNotCompatibleWithRegexAnnotation(Label<'static>, Option<ValueType>),
    ValueTypeIsNotCompatibleWithRangeAnnotation(Label<'static>, Option<ValueType>),
    ValueTypeIsNotCompatibleWithValuesAnnotation(Label<'static>, Option<ValueType>),
    ValueTypeIsNotKeyableForUniqueConstraintOfKeyAnnotationDeclaredOnOwns(
        Label<'static>,
        Label<'static>,
        Option<ValueType>,
    ),
    ValueTypeIsNotKeyableForUniqueConstraintOfUniqueAnnotationDeclaredOnOwns(
        Label<'static>,
        Label<'static>,
        Option<ValueType>,
    ),
    CannotSetAnnotationToInterfaceBecauseItsConstraintIsNotNarrowedByItsCapabilityConstraint(
        Label<'static>,
        ConstraintError,
    ),
    CannotSetAnnotationToCapabilityBecauseItsConstraintDoesNotNarrowItsInterfaceConstraint(
        Label<'static>,
        Label<'static>,
        ConstraintError,
    ),
    InvalidCardinalityArguments(AnnotationCardinality),
    InvalidRegexArguments(AnnotationRegex),
    InvalidRangeArgumentsForValueType(AnnotationRange, Option<ValueType>),
    InvalidValuesArgumentsForValueType(AnnotationValues, Option<ValueType>),
    SubtypeConstraintDoesNotNarrowSupertypeConstraint(Label<'static>, Label<'static>, ConstraintError),
    CannotUnsetInheritedOwns(Label<'static>, Label<'static>),
    CannotUnsetInheritedPlays(Label<'static>, Label<'static>),
    CannotUnsetInheritedValueType(ValueType, Label<'static>),
    ValueTypeNotCompatibleWithInheritedValueType(Label<'static>, Label<'static>, ValueType, ValueType),
    CannotRedeclareInheritedValueTypeWithoutSpecialisation(Label<'static>, Label<'static>, ValueType, ValueType),
    AnnotationIsNotCompatibleWithDeclaredAnnotation(AnnotationCategory, AnnotationCategory, Label<'static>),
    RelationTypeMustRelateAtLeastOneRole(Label<'static>),
    CannotRedeclareInheritedCapabilityWithoutSpecialisation(
        CapabilityKind,
        Label<'static>,
        Label<'static>,
        Label<'static>,
    ),
    CannotRedeclareConstraintOnSubtypeWithoutSpecialisation(
        Kind,
        Label<'static>,
        Label<'static>,
        ConstraintDescription,
    ),
    CapabilityConstraintAlreadyExistsForTheWholeInterfaceType(
        CapabilityKind,
        Label<'static>,
        Label<'static>,
        ConstraintDescription,
    ),
    ChangingAttributeSupertypeLeadsToImplicitIndependentAnnotationLossAndUnexpectedDataLoss(
        Label<'static>,
        Option<Label<'static>>,
        Label<'static>,
    ),
    CannotDeleteTypeWithExistingInstances(Label<'static>),
    CannotSetRoleOrderingWithExistingInstances(Label<'static>),
    CannotSetOwnsOrderingWithExistingInstances(Label<'static>, Label<'static>),
    CannotUnsetValueTypeWithExistingInstances(Label<'static>),
    CannotChangeValueTypeWithExistingInstances(Label<'static>),
    CannotUnsetCapabilityWithExistingInstances(CapabilityKind, Label<'static>, Label<'static>, Label<'static>),
    CannotChangeSupertypeAsCapabilityIsLostWhileHavingHasInstances(
        CapabilityKind,
        Label<'static>,
        Option<Label<'static>>,
        Label<'static>,
        Label<'static>,
    ),
    CannotAcquireCapabilityAsExistingInstancesViolateItsConstraint(DataValidationError),
    CannotSetAnnotationForCapabilityAsExistingInstancesViolateItsConstraint(DataValidationError),
    CannotChangeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(DataValidationError),
    CannotChangeInterfaceTypeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(DataValidationError),
    CannotUnsetInterfaceTypeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(DataValidationError),
    CannotSetAnnotationAsExistingInstancesViolateItsConstraint(DataValidationError),
    CannotChangeSupertypeAsUpdatedConstraintIsViolatedByExistingInstances(DataValidationError),
}

impl fmt::Display for SchemaValidationError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for SchemaValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConceptRead(source) => Some(source),
            Self::LabelShouldBeUnique { .. } => None,
            Self::StructNameShouldBeUnique(_) => None,
            Self::StructShouldHaveAtLeastOneField(_) => None,
            Self::StructCannotBeDeletedAsItsUsedAsValueTypeForAttributeTypes(_, _) => None,
            Self::StructCannotBeDeletedAsItsUsedAsValueTypeForStructs(_, _) => None,
            Self::RoleNameShouldBeUniqueForRelationTypeHierarchy(_, _) => None,
            Self::CycleFoundInTypeHierarchy(_, _) => None,
            Self::ChangingAttributeTypeSupertypeWillLeadToConflictingValueTypes(_, _, _) => None,
            Self::CannotUnsetAbstractnessOfAttributeTypeAsItHasSubtypes(_) => None,
            Self::CannotDeleteTypeWithExistingSubtypes(_) => None,
            Self::RelatesNotInherited(_, _) => None,
            Self::OrderingDoesNotMatchWithSupertype(_, _, _, _) => None,
            Self::OrderingDoesNotMatchWithCapabilityOfSubtypeInterface(_, _, _, _, _) => None,
            Self::SpecialisingRelatesIsNotAbstract(_, _) => None,
            Self::CannotSetOwnsBecauseItIsAlreadySetWithDifferentOrdering(_, _, _) => None,
            Self::InvalidOrderingForDistinctConstraint(_) => None,
            Self::AttributeTypeSupertypeIsNotAbstract(_) => None,
            Self::AbstractTypesSupertypeHasToBeAbstract(_, _) => None,
            Self::CannotUnsetRelatesAbstractnessAsItIsASpecialisingRelates(_) => None,
            Self::AttributeTypeWithoutValueTypeShouldBeAbstract(_) => None,
            Self::ValueTypeIsNotCompatibleWithRegexAnnotation(_, _) => None,
            Self::ValueTypeIsNotCompatibleWithRangeAnnotation(_, _) => None,
            Self::ValueTypeIsNotCompatibleWithValuesAnnotation(_, _) => None,
            Self::ValueTypeIsNotKeyableForUniqueConstraintOfKeyAnnotationDeclaredOnOwns(_, _, _) => None,
            Self::ValueTypeIsNotKeyableForUniqueConstraintOfUniqueAnnotationDeclaredOnOwns(_, _, _) => None,
            Self::CannotSetAnnotationToInterfaceBecauseItsConstraintIsNotNarrowedByItsCapabilityConstraint(_, _) => {
                None
            }
            Self::CannotSetAnnotationToCapabilityBecauseItsConstraintDoesNotNarrowItsInterfaceConstraint(_, _, _) => {
                None
            }
            Self::InvalidCardinalityArguments(_) => None,
            Self::InvalidRegexArguments(_) => None,
            Self::InvalidRangeArgumentsForValueType(_, _) => None,
            Self::InvalidValuesArgumentsForValueType(_, _) => None,
            Self::SubtypeConstraintDoesNotNarrowSupertypeConstraint(_, _, _) => None,
            Self::CannotUnsetInheritedOwns(_, _) => None,
            Self::CannotUnsetInheritedPlays(_, _) => None,
            Self::CannotUnsetInheritedValueType(_, _) => None,
            Self::ValueTypeNotCompatibleWithInheritedValueType(_, _, _, _) => None,
            Self::CannotRedeclareInheritedValueTypeWithoutSpecialisation(_, _, _, _) => None,
            Self::AnnotationIsNotCompatibleWithDeclaredAnnotation(_, _, _) => None,
            Self::RelationTypeMustRelateAtLeastOneRole(_) => None,
            Self::CannotRedeclareInheritedCapabilityWithoutSpecialisation(_, _, _, _) => None,
            Self::CannotRedeclareConstraintOnSubtypeWithoutSpecialisation(_, _, _, _) => None,
            Self::CapabilityConstraintAlreadyExistsForTheWholeInterfaceType(_, _, _, _) => None,
            Self::ChangingAttributeSupertypeLeadsToImplicitIndependentAnnotationLossAndUnexpectedDataLoss(_, _, _) => {
                None
            }
            Self::CannotDeleteTypeWithExistingInstances(_) => None,
            Self::CannotSetRoleOrderingWithExistingInstances(_) => None,
            Self::CannotSetOwnsOrderingWithExistingInstances(_, _) => None,
            Self::CannotUnsetValueTypeWithExistingInstances(_) => None,
            Self::CannotChangeValueTypeWithExistingInstances(_) => None,
            Self::CannotUnsetCapabilityWithExistingInstances(_, _, _, _) => None,
            Self::CannotChangeSupertypeAsCapabilityIsLostWhileHavingHasInstances(_, _, _, _, _) => None,
            Self::CannotAcquireCapabilityAsExistingInstancesViolateItsConstraint(source) => Some(source),
            Self::CannotSetAnnotationForCapabilityAsExistingInstancesViolateItsConstraint(source) => Some(source),
            Self::CannotChangeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(source) => {
                Some(source)
            }
            Self::CannotChangeInterfaceTypeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(
                source,
            ) => Some(source),
            Self::CannotUnsetInterfaceTypeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(
                source,
            ) => Some(source),
            Self::CannotSetAnnotationAsExistingInstancesViolateItsConstraint(source) => Some(source),
            Self::CannotChangeSupertypeAsUpdatedConstraintIsViolatedByExistingInstances(source) => Some(source),
        }
    }
}
