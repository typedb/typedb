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
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::{
            Annotation, AnnotationCardinality, AnnotationCategory, AnnotationRange, AnnotationRegex, AnnotationValues,
        },
        attribute_type::AttributeType,
        object_type::ObjectType,
        relation_type::RelationType,
        role_type::RoleType,
        Capability, Ordering, TypeAPI,
    },
};

pub mod annotation_compatibility;
pub mod commit_time_validation;
pub mod operation_time_validation;
mod validation;

#[derive(Debug, Clone)]
pub enum SchemaValidationError {
    ConceptRead(ConceptReadError),
    RootTypesAreImmutable,
    RootHasBeenCorrupted(Label<'static>),
    LabelShouldBeUnique(Label<'static>),
    StructNameShouldBeUnique(String),
    StructShouldHaveAtLeastOneField(String),
    StructCannotBeDeletedAsItsUsedAsValueTypeForAttributeTypes(String, usize),
    StructCannotBeDeletedAsItsUsedAsValueTypeForStructs(String, usize),
    RoleNameShouldBeUniqueForRelationTypeHierarchy(Label<'static>, Label<'static>),
    CycleFoundInTypeHierarchy(Label<'static>, Label<'static>),
    ChangingAttributeTypeSupertypeWillImplicitlyChangeItsValueType(Label<'static>, Option<ValueType>),
    CannotUnsetAbstractnessOfAttributeTypeAsItHasSubtypes(Label<'static>),
    CannotDeleteTypeWithExistingSubtypes(Label<'static>),
    CannotDeleteTypeWithExistingInstances(Label<'static>),
    RelatesNotInherited(RelationType<'static>, RoleType<'static>),
    OwnsNotInherited(ObjectType<'static>, AttributeType<'static>),
    PlaysNotInherited(ObjectType<'static>, RoleType<'static>),
    OverriddenCapabilityCannotBeRedeclared(CapabilityKind, Label<'static>, Label<'static>),
    OverriddenCapabilityInterfaceIsNotSupertype(CapabilityKind, Label<'static>, Label<'static>, Label<'static>),
    NonAbstractTypeCannotHaveAbstractCapability(CapabilityKind, Label<'static>, Label<'static>),
    AttributeTypeSupertypeIsNotAbstract(Label<'static>),
    AbstractTypesSupertypeHasToBeAbstract(Label<'static>, Label<'static>),
    CannotUnsetAbstractnessAsItHasDeclaredCapabilityOfAbstractInterface(CapabilityKind, Label<'static>, Label<'static>),
    CannotUnsetAbstractnessAsItHasInheritedCapabilityOfAbstractInterface(
        CapabilityKind,
        Label<'static>,
        Label<'static>,
    ),
    OrderingDoesNotMatchWithSupertype(Label<'static>, Label<'static>, Ordering, Ordering),
    OrderingDoesNotMatchWithOverride(Label<'static>, Label<'static>, Label<'static>, Ordering, Ordering),
    CannotChangeSupertypeAsRelatesOverrideIsImplicitlyLost(Label<'static>, Label<'static>, Label<'static>),
    CannotChangeSupertypeAsOwnsOverrideIsImplicitlyLost(Label<'static>, Label<'static>, Label<'static>),
    CannotChangeSupertypeAsPlaysOverrideIsImplicitlyLost(Label<'static>, Label<'static>, Label<'static>),
    CannotChangeSupertypeAsOwnsIsOverriddenInTheNewSupertype(Label<'static>, Label<'static>, Label<'static>),
    CannotChangeSupertypeAsPlaysIsOverriddenInTheNewSupertype(Label<'static>, Label<'static>, Label<'static>),
    RelatesOverrideIsNotInherited(Label<'static>, Label<'static>),
    OwnsOverrideIsNotInherited(Label<'static>, Label<'static>),
    PlaysOverrideIsNotInherited(Label<'static>, Label<'static>),
    InvalidOrderingForDistinctAnnotation(Label<'static>),
    AttributeTypeWithoutValueTypeShouldBeAbstract(Label<'static>),
    ValueTypeIsNotCompatibleWithRegexAnnotation(Label<'static>, Option<ValueType>),
    ValueTypeIsNotCompatibleWithRangeAnnotation(Label<'static>, Option<ValueType>),
    ValueTypeIsNotCompatibleWithValuesAnnotation(Label<'static>, Option<ValueType>),
    ValueTypeIsNotKeyableForKeyAnnotation(Label<'static>, Label<'static>, Option<ValueType>),
    ValueTypeIsNotKeyableForUniqueAnnotation(Label<'static>, Label<'static>, Option<ValueType>),
    CannotSetAnnotationToInterfaceBecauseItAlreadyExistsForItsImplementation(Label<'static>, AnnotationCategory),
    CannotSetAnnotationToCapabilityBecauseItAlreadyExistsForItsInterface(Label<'static>, AnnotationCategory),
    InvalidCardinalityArguments(AnnotationCardinality),
    InvalidRegexArguments(AnnotationRegex),
    InvalidRangeArguments(AnnotationRange),
    InvalidValuesArguments(AnnotationValues),
    KeyShouldNarrowInheritedCardinality(Label<'static>, Label<'static>, Label<'static>, AnnotationCardinality),
    CardinalityDoesNotNarrowInheritedCardinality(
        CapabilityKind,
        Label<'static>,
        Label<'static>,
        Label<'static>,
        AnnotationCardinality,
        AnnotationCardinality,
    ),
    SummarizedCardinalityOfEdgesOverridingSingleEdgeOverflowsOverriddenCardinality(
        CapabilityKind,
        Label<'static>,
        Label<'static>,
        AnnotationCardinality,
        AnnotationCardinality,
    ),
    OnlyOneRegexCanBeSetForTypeHierarchy(Label<'static>, Label<'static>, AnnotationRegex, AnnotationRegex),
    RangeShouldNarrowInheritedRange(Label<'static>, Label<'static>, AnnotationRange, AnnotationRange),
    ValuesShouldNarrowInheritedValues(Label<'static>, Label<'static>, AnnotationValues, AnnotationValues),
    OnlyOneRegexCanBeSetForTypeEdgeHierarchy(
        Label<'static>,
        Label<'static>,
        Label<'static>,
        AnnotationRegex,
        AnnotationRegex,
    ),
    RangeShouldNarrowInheritedEdgeRange(
        Label<'static>,
        Label<'static>,
        Label<'static>,
        AnnotationRange,
        AnnotationRange,
    ),
    ValuesShouldNarrowInheritedEdgeValues(
        Label<'static>,
        Label<'static>,
        Label<'static>,
        AnnotationValues,
        AnnotationValues,
    ),
    CannotUnsetInheritedOwns(Label<'static>, Label<'static>),
    CannotUnsetInheritedPlays(Label<'static>, Label<'static>),
    CannotUnsetInheritedAnnotation(AnnotationCategory, Label<'static>),
    CannotUnsetInheritedEdgeAnnotation(AnnotationCategory, Label<'static>, Label<'static>),
    CannotUnsetInheritedValueType(ValueType, Label<'static>),
    ValueTypeNotCompatibleWithInheritedValueType(Label<'static>, Label<'static>, ValueType, ValueType),
    RedundantValueTypeDeclarationAsItsAlreadyInherited(Label<'static>, Label<'static>, ValueType, ValueType),
    DeclaredAnnotationIsNotCompatibleWithInheritedAnnotation(AnnotationCategory, AnnotationCategory, Label<'static>),
    AnnotationIsNotCompatibleWithDeclaredAnnotation(AnnotationCategory, AnnotationCategory, Label<'static>),
    RelationTypeMustRelateAtLeastOneRole(Label<'static>),
    CannotRedeclareInheritedCapabilityWithoutSpecializationWithOverride(
        CapabilityKind,
        Label<'static>,
        Label<'static>,
        Label<'static>,
    ),
    CannotRedeclareInheritedAnnotationWithoutSpecializationForType(Kind, Label<'static>, Label<'static>, Annotation),
    CannotRedeclareInheritedAnnotationWithoutSpecializationForCapability(
        CapabilityKind,
        Label<'static>,
        Label<'static>,
        Label<'static>,
        Annotation,
    ),
    ChangingRelationSupertypeLeadsToImplicitCascadeAnnotationAcquisitionAndUnexpectedDataLoss(
        Label<'static>,
        Label<'static>,
    ),
    ChangingAttributeSupertypeLeadsToImplicitIndependentAnnotationLossAndUnexpectedDataLoss(
        Label<'static>,
        Label<'static>,
    ),
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
            Self::RootTypesAreImmutable => None,
            Self::RootHasBeenCorrupted(_) => None,
            Self::LabelShouldBeUnique(_) => None,
            Self::StructNameShouldBeUnique(_) => None,
            Self::StructShouldHaveAtLeastOneField(_) => None,
            Self::StructCannotBeDeletedAsItsUsedAsValueTypeForAttributeTypes(_, _) => None,
            Self::StructCannotBeDeletedAsItsUsedAsValueTypeForStructs(_, _) => None,
            Self::RoleNameShouldBeUniqueForRelationTypeHierarchy(_, _) => None,
            Self::CycleFoundInTypeHierarchy(_, _) => None,
            Self::ChangingAttributeTypeSupertypeWillImplicitlyChangeItsValueType(_, _) => None,
            Self::CannotUnsetAbstractnessOfAttributeTypeAsItHasSubtypes(_) => None,
            Self::CannotDeleteTypeWithExistingSubtypes(_) => None,
            Self::CannotDeleteTypeWithExistingInstances(_) => None,
            Self::RelatesNotInherited(_, _) => None,
            Self::OwnsNotInherited(_, _) => None,
            Self::PlaysNotInherited(_, _) => None,
            Self::OverriddenCapabilityCannotBeRedeclared(_, _, _) => None,
            Self::OverriddenCapabilityInterfaceIsNotSupertype(_, _, _, _) => None,
            Self::OrderingDoesNotMatchWithSupertype(_, _, _, _) => None,
            Self::OrderingDoesNotMatchWithOverride(_, _, _, _, _) => None,
            Self::CannotChangeSupertypeAsRelatesOverrideIsImplicitlyLost(_, _, _) => None,
            Self::CannotChangeSupertypeAsOwnsOverrideIsImplicitlyLost(_, _, _) => None,
            Self::CannotChangeSupertypeAsPlaysOverrideIsImplicitlyLost(_, _, _) => None,
            Self::CannotChangeSupertypeAsOwnsIsOverriddenInTheNewSupertype(_, _, _) => None,
            Self::CannotChangeSupertypeAsPlaysIsOverriddenInTheNewSupertype(_, _, _) => None,
            Self::RelatesOverrideIsNotInherited(_, _) => None,
            Self::OwnsOverrideIsNotInherited(_, _) => None,
            Self::PlaysOverrideIsNotInherited(_, _) => None,
            Self::InvalidOrderingForDistinctAnnotation(_) => None,
            Self::NonAbstractTypeCannotHaveAbstractCapability(_, _, _) => None,
            Self::AttributeTypeSupertypeIsNotAbstract(_) => None,
            Self::AbstractTypesSupertypeHasToBeAbstract(_, _) => None,
            Self::CannotUnsetAbstractnessAsItHasDeclaredCapabilityOfAbstractInterface(_, _, _) => None,
            Self::CannotUnsetAbstractnessAsItHasInheritedCapabilityOfAbstractInterface(_, _, _) => None,
            Self::AttributeTypeWithoutValueTypeShouldBeAbstract(_) => None,
            Self::ValueTypeIsNotCompatibleWithRegexAnnotation(_, _) => None,
            Self::ValueTypeIsNotCompatibleWithRangeAnnotation(_, _) => None,
            Self::ValueTypeIsNotCompatibleWithValuesAnnotation(_, _) => None,
            Self::ValueTypeIsNotKeyableForKeyAnnotation(_, _, _) => None,
            Self::ValueTypeIsNotKeyableForUniqueAnnotation(_, _, _) => None,
            Self::CannotSetAnnotationToInterfaceBecauseItAlreadyExistsForItsImplementation(_, _) => None,
            Self::CannotSetAnnotationToCapabilityBecauseItAlreadyExistsForItsInterface(_, _) => None,
            Self::InvalidCardinalityArguments(_) => None,
            Self::InvalidRegexArguments(_) => None,
            Self::InvalidRangeArguments(_) => None,
            Self::InvalidValuesArguments(_) => None,
            Self::KeyShouldNarrowInheritedCardinality(_, _, _, _) => None,
            Self::CardinalityDoesNotNarrowInheritedCardinality(_, _, _, _, _, _) => None,
            Self::SummarizedCardinalityOfEdgesOverridingSingleEdgeOverflowsOverriddenCardinality(_, _, _, _, _) => None,
            Self::OnlyOneRegexCanBeSetForTypeHierarchy(_, _, _, _) => None,
            Self::RangeShouldNarrowInheritedRange(_, _, _, _) => None,
            Self::ValuesShouldNarrowInheritedValues(_, _, _, _) => None,
            Self::OnlyOneRegexCanBeSetForTypeEdgeHierarchy(_, _, _, _, _) => None,
            Self::RangeShouldNarrowInheritedEdgeRange(_, _, _, _, _) => None,
            Self::ValuesShouldNarrowInheritedEdgeValues(_, _, _, _, _) => None,
            Self::CannotUnsetInheritedOwns(_, _) => None,
            Self::CannotUnsetInheritedPlays(_, _) => None,
            Self::CannotUnsetInheritedAnnotation(_, _) => None,
            Self::CannotUnsetInheritedEdgeAnnotation(_, _, _) => None,
            Self::CannotUnsetInheritedValueType(_, _) => None,
            Self::ValueTypeNotCompatibleWithInheritedValueType(_, _, _, _) => None,
            Self::RedundantValueTypeDeclarationAsItsAlreadyInherited(_, _, _, _) => None,
            Self::DeclaredAnnotationIsNotCompatibleWithInheritedAnnotation(_, _, _) => None,
            Self::AnnotationIsNotCompatibleWithDeclaredAnnotation(_, _, _) => None,
            Self::RelationTypeMustRelateAtLeastOneRole(_) => None,
            Self::CannotRedeclareInheritedCapabilityWithoutSpecializationWithOverride(_, _, _, _) => None,
            Self::CannotRedeclareInheritedAnnotationWithoutSpecializationForType(_, _, _, _) => None,
            Self::CannotRedeclareInheritedAnnotationWithoutSpecializationForCapability(_, _, _, _, _) => None,
            Self::ChangingRelationSupertypeLeadsToImplicitCascadeAnnotationAcquisitionAndUnexpectedDataLoss(_, _) => {
                None
            }
            Self::ChangingAttributeSupertypeLeadsToImplicitIndependentAnnotationLossAndUnexpectedDataLoss(_, _) => None,
        }
    }
}
