/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use encoding::value::{label::Label, value_type::ValueType};

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::{AnnotationCardinality, AnnotationCategory, AnnotationRange, AnnotationRegex, AnnotationValues},
        attribute_type::AttributeType,
        object_type::ObjectType,
        relation_type::RelationType,
        role_type::RoleType,
        InterfaceImplementation, TypeAPI,
    },
};

pub mod annotation_compatibility;
pub mod commit_time_validation;
pub mod operation_time_validation;
mod validation;

#[derive(Debug, Clone)]
pub enum SchemaValidationError {
    ConceptRead(ConceptReadError),
    CannotModifyRoot,
    RootHasBeenCorrupted(Label<'static>),
    LabelShouldBeUnique(Label<'static>),
    StructNameShouldBeUnique(String),
    RoleNameShouldBeUniqueForRelationTypeHierarchy(Label<'static>, Label<'static>),
    CycleFoundInTypeHierarchy(Label<'static>, Label<'static>),
    CannotChangeValueTypeOfAttributeType(Label<'static>, Option<ValueType>),
    CannotDeleteTypeWithExistingSubtypes(Label<'static>),
    CannotDeleteTypeWithExistingInstances(Label<'static>),
    RelatesNotInherited(RelationType<'static>, RoleType<'static>),
    OwnsNotInherited(ObjectType<'static>, AttributeType<'static>),
    PlaysNotInherited(ObjectType<'static>, RoleType<'static>),
    OverriddenPlaysCannotBeRedeclared(Label<'static>, RoleType<'static>),
    OverriddenOwnsCannotBeRedeclared(Label<'static>, AttributeType<'static>),
    OverriddenInterfaceImplementationObjectIsNotSupertype(Label<'static>, Label<'static>),
    NonAbstractCannotOwnAbstract(Label<'static>, Label<'static>),
    AttributeTypeSupertypeIsNotAbstract(Label<'static>),
    CannotSetNonAbstractSupertypeForAbstractType(Label<'static>, Label<'static>),
    CannotUnsetAbstractnessAsItOwnsAbstractTypes(Label<'static>),
    OrderingDoesNotMatchWithSupertype(Label<'static>, Label<'static>),
    InvalidOrderingForDistinctAnnotation(Label<'static>),
    AttributeTypeWithoutValueTypeShouldBeAbstract(Label<'static>),
    ValueTypeIsNotCompatibleWithRegexAnnotation(Label<'static>, Option<ValueType>),
    ValueTypeIsNotCompatibleWithRangeAnnotation(Label<'static>, Option<ValueType>),
    ValueTypeIsNotCompatibleWithValuesAnnotation(Label<'static>, Option<ValueType>),
    ValueTypeIsNotKeyableForKeyAnnotation(Label<'static>, Label<'static>, Option<ValueType>),
    ValueTypeIsNotKeyableForUniqueAnnotation(Label<'static>, Label<'static>, Option<ValueType>),
    CannotSetAnnotationToInterfaceBecauseItAlreadyExistsForItsImplementation(Label<'static>, AnnotationCategory),
    CannotSetAnnotationToInterfaceImplementationBecauseItAlreadyExistsForItsInterface(
        Label<'static>,
        AnnotationCategory,
    ),
    InvalidCardinalityArguments(AnnotationCardinality),
    InvalidRegexArguments(AnnotationRegex),
    InvalidRangeArguments(AnnotationRange),
    InvalidValuesArguments(AnnotationValues),
    CardinalityShouldNarrowInheritedCardinality(AnnotationCardinality, AnnotationCardinality),
    KeyShouldNarrowInheritedCardinality(AnnotationCardinality),
    OnlyOneRegexCanBeSetForTypeHierarchy(AnnotationRegex, Label<'static>),
    RangeShouldNarrowInheritedRange(AnnotationRange, AnnotationRange),
    ValuesShouldNarrowInheritedValues(AnnotationValues, AnnotationValues),
    CannotUnsetInheritedOwns(Label<'static>, Label<'static>),
    CannotUnsetInheritedPlays(Label<'static>, Label<'static>),
    CannotUnsetInheritedAnnotation(AnnotationCategory, Label<'static>),
    CannotUnsetInheritedEdgeAnnotation(AnnotationCategory, Label<'static>),
    ValueTypeNotCompatibleWithInheritedValueTypeOf(Label<'static>, Label<'static>, ValueType),
    CannotUnsetValueTypeAsThereAreNonAbstractSubtypesWithoutDeclaredValueTypes(Label<'static>, Label<'static>),
    DeclaredAnnotationIsNotCompatibleWithInheritedAnnotation(AnnotationCategory, AnnotationCategory, Label<'static>),
    AnnotationIsNotCompatibleWithDeclaredAnnotation(AnnotationCategory, AnnotationCategory, Label<'static>),
    RelationTypeMustRelateAtLeastOneRole(Label<'static>),
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
            Self::CannotModifyRoot => None,
            Self::RootHasBeenCorrupted(_) => None,
            Self::LabelShouldBeUnique(_) => None,
            Self::StructNameShouldBeUnique(_) => None,
            Self::RoleNameShouldBeUniqueForRelationTypeHierarchy(_, _) => None,
            Self::CycleFoundInTypeHierarchy(_, _) => None,
            Self::CannotChangeValueTypeOfAttributeType(_, _) => None,
            Self::CannotDeleteTypeWithExistingSubtypes(_) => None,
            Self::CannotDeleteTypeWithExistingInstances(_) => None,
            Self::RelatesNotInherited(_, _) => None,
            Self::OwnsNotInherited(_, _) => None,
            Self::PlaysNotInherited(_, _) => None,
            Self::OverriddenPlaysCannotBeRedeclared(_, _) => None,
            Self::OverriddenOwnsCannotBeRedeclared(_, _) => None,
            Self::OverriddenInterfaceImplementationObjectIsNotSupertype(_, _) => None,
            Self::OrderingDoesNotMatchWithSupertype(_, _) => None,
            Self::InvalidOrderingForDistinctAnnotation(_) => None,
            Self::NonAbstractCannotOwnAbstract(_, _) => None,
            Self::AttributeTypeSupertypeIsNotAbstract(_) => None,
            Self::CannotSetNonAbstractSupertypeForAbstractType(_, _) => None,
            Self::CannotUnsetAbstractnessAsItOwnsAbstractTypes(_) => None,
            Self::AttributeTypeWithoutValueTypeShouldBeAbstract(_) => None,
            Self::ValueTypeIsNotCompatibleWithRegexAnnotation(_, _) => None,
            Self::ValueTypeIsNotCompatibleWithRangeAnnotation(_, _) => None,
            Self::ValueTypeIsNotCompatibleWithValuesAnnotation(_, _) => None,
            Self::ValueTypeIsNotKeyableForKeyAnnotation(_, _, _) => None,
            Self::ValueTypeIsNotKeyableForUniqueAnnotation(_, _, _) => None,
            Self::CannotSetAnnotationToInterfaceBecauseItAlreadyExistsForItsImplementation(_, _) => None,
            Self::CannotSetAnnotationToInterfaceImplementationBecauseItAlreadyExistsForItsInterface(_, _) => None,
            Self::InvalidCardinalityArguments(_) => None,
            Self::InvalidRegexArguments(_) => None,
            Self::InvalidRangeArguments(_) => None,
            Self::InvalidValuesArguments(_) => None,
            Self::CardinalityShouldNarrowInheritedCardinality(_, _) => None,
            Self::KeyShouldNarrowInheritedCardinality(_) => None,
            Self::OnlyOneRegexCanBeSetForTypeHierarchy(_, _) => None,
            Self::RangeShouldNarrowInheritedRange(_, _) => None,
            Self::ValuesShouldNarrowInheritedValues(_, _) => None,
            Self::CannotUnsetInheritedOwns(_, _) => None,
            Self::CannotUnsetInheritedPlays(_, _) => None,
            Self::CannotUnsetInheritedAnnotation(_, _) => None,
            Self::CannotUnsetInheritedEdgeAnnotation(_, _) => None,
            Self::ValueTypeNotCompatibleWithInheritedValueTypeOf(_, _, _) => None,
            Self::CannotUnsetValueTypeAsThereAreNonAbstractSubtypesWithoutDeclaredValueTypes(_, _) => None,
            Self::DeclaredAnnotationIsNotCompatibleWithInheritedAnnotation(_, _, _) => None,
            Self::AnnotationIsNotCompatibleWithDeclaredAnnotation(_, _, _) => None,
            Self::RelationTypeMustRelateAtLeastOneRole(_) => None,
        }
    }
}

macro_rules! get_label {
    ($snapshot: ident, $type_:ident) => {
        // TODO: It is a spicy macro, need to refactor it to return Result<Label, ConceptReadError>
        TypeReader::get_label($snapshot, $type_).unwrap().unwrap()
    };
}

pub(crate) use get_label;
