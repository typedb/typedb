/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::{
    graph::type_::{CapabilityKind, Kind},
    value::{label::Label, value_type::ValueType},
};
use error::typedb_error;

use crate::{
    error::ConceptReadError,
    thing::thing_manager::validation::DataValidationError,
    type_::{
        annotation::{AnnotationCardinality, AnnotationCategory, AnnotationRange, AnnotationRegex, AnnotationValues},
        constraint::{ConstraintDescription, ConstraintError},
        Ordering,
    },
};

pub(crate) mod commit_time_validation;
pub(crate) mod operation_time_validation;
pub(crate) mod validation;

typedb_error!(
    pub SchemaValidationError(component = "Schema validation", prefix = "SVL") {
        ConceptRead(1, "Data validation failed due to concept read error.", (source: Box<ConceptReadError>)),
        LabelShouldBeUnique(2, "Label '{label}' should be unique, but is already used by '{existing_kind}'.", label: Label, existing_kind: Kind),
        StructNameShouldBeUnique(3, "Struct name '{name}' must a unique, unused name.", name: String),
        StructShouldHaveAtLeastOneField(4, "Struct '{name}' should have at least one field.", name: String),
        StructCannotBeDeletedAsItsUsedAsValueTypeForAttributeTypes(5, "Struct '{name}' cannot be deleted as it is used as value type for {usages} attribute types.", name: String, usages: usize),
        StructCannotBeDeletedAsItsUsedAsValueTypeForStructs(6, "Struct '{name}' cannot be deleted as it is used as value type in {usages} structs.", name: String, usages: usize),
        RoleNameShouldBeUniqueForRelationTypeHierarchy(7, "Role name '{role}' should be unique in relation type hierarchy of '{relation}'.", role: Label, relation: Label),
        CycleFoundInTypeHierarchy(8, "A cycle was found in the type hierarchy between '{start}' and '{end}'.", start: Label, end: Label),
        ChangingAttributeTypeSupertypeWillLeadToConflictingValueTypes(9, "Changing supertype of attribute type '{attribute}' will lead to conflicting value types between '{value_type:?}' and '{new_super_value_type}'.", attribute: Label, value_type: Option<ValueType>, new_super_value_type: ValueType),
        CannotUnsetAbstractnessOfAttributeTypeAsItHasSubtypes(10, "Cannot unset the abstractness of attribute type '{attribute}' as it has subtypes.", attribute: Label),
        CannotDeleteTypeWithExistingSubtypes(11, "Cannot delete type '{label}' as it has existing subtypes.", label: Label),
        RelatesNotInherited(12, "Relation type '{relation}' does not inherit role '{role}'.", relation: Label, role: Label),
        RelatesIsAlreadySpecialisedByASupertype(13, "Relation type '{relation}' is already specialised by a supertype for '{role}'.", relation: Label, role: Label),
        CannotManageAnnotationsForSpecialisingRelates(14, "Cannot manage annotations for the specialising relates role '{role}'.", role: Label),
        AttributeTypeSupertypeIsNotAbstract(15, "Supertype of attribute type '{attribute}' is not abstract.", attribute: Label),
        AbstractTypesSupertypeHasToBeAbstract(16, "Abstract type '{attribute}' must have an abstract supertype, but it has '{super_attribute}'.", attribute: Label, super_attribute: Label),
        CannotUnsetRelatesAbstractnessAsItIsASpecialisingRelates(17, "Cannot unset the abstractness of relates role '{role}' as it is a specialising relates.", role: Label),
        CannotChangeRelationTypeSupertypeAsRelatesSpecialiseIsLost(18, "Cannot change supertype of relation type '{relation}' as the relates specialisation for roles '{role_1}' and '{role_2}' will be lost.", relation: Label, role_1: Label, role_2: Label),
        OrderingDoesNotMatchWithSupertype(19, "Ordering for '{label}' '{ordering}' does not match with its supertype '{super_label}' ordering '{super_ordering}.", label: Label, super_label: Label, ordering: Ordering, super_ordering: Ordering),
        OrderingDoesNotMatchWithCapabilityOfSupertypeInterface(20, "Ordering for '{label}' does not match with the capability of its supertype interface '{super_label}' on interface '{interface}' (expected: '{expected}', found: '{found}').", label: Label, super_label: Label, interface: Label, expected: Ordering, found: Ordering),
        SpecialisingRelatesIsNotAbstract(21, "Specialising relates role '{role}' of relation type '{relation}' is not abstract.", role: Label, relation: Label),
        CannotSetOwnsBecauseItIsAlreadySetWithDifferentOrdering(22, "Cannot set 'owns' for '{owner}' because it is already set with a different ordering in '{conflicting_owner}'.", owner: Label, conflicting_owner: Label),
        InvalidOrderingForDistinctAnnotation(23, "Invalid ordering '{ordering}' for distinct annotation on '{label}'.", label: Label, ordering: Ordering),
        AttributeTypeWithoutValueTypeShouldBeAbstract(24, "Attribute type '{attribute}' without a value type should be abstract.", attribute: Label),
        ValueTypeIsNotCompatibleWithRegexAnnotation(25, "Value type '{value_type:?}' is not compatible with regex annotation on '{attribute}'.", attribute: Label, value_type: Option<ValueType>),
        ValueTypeIsNotCompatibleWithRangeAnnotation(26, "Value type '{value_type:?}' is not compatible with range annotation on '{attribute}'.", attribute: Label, value_type: Option<ValueType>),
        ValueTypeIsNotCompatibleWithValuesAnnotation(27, "Value type '{value_type:?}' is not compatible with values annotation on '{attribute}'.", attribute: Label, value_type: Option<ValueType>),
        ValueTypeIsNotKeyableForUniqueConstraintOfKeyAnnotationDeclaredOnOwns(28, "Value type '{value_type:?}' is not keyable for unique constraint of key annotation declared on 'owns' for '{owner}' owning '{attribute}'.", owner: Label, attribute: Label, value_type: Option<ValueType>),
        ValueTypeIsNotKeyableForUniqueConstraintOfUniqueAnnotationDeclaredOnOwns(29, "Value type '{value_type:?}' is not keyable for unique constraint of unique annotation declared on 'owns' for '{owner}' owning '{attribute}'.", owner: Label, attribute: Label, value_type: Option<ValueType>),
        CannotSetAnnotationToInterfaceBecauseItsConstraintIsNotNarrowedByItsCapabilityConstraint(30, "Cannot set annotation to interface '{interface}' because its constraint is not narrowed by its capability constraint.", interface: Label, (typedb_source: Box<ConstraintError>)),
        CannotSetAnnotationToCapabilityBecauseItsConstraintDoesNotNarrowItsInterfaceConstraint(31, "Cannot set annotation on capability '{cap}' of '{interface}' on type '{label}' because its constraint does not narrow its interface constraint.", cap: CapabilityKind, interface: Label, label: Label, (typedb_source: Box<ConstraintError>)),
        InvalidCardinalityArguments(32, "Invalid arguments for cardinality annotation '{card}'.", card: AnnotationCardinality),
        InvalidRegexArguments(33, "Invalid arguments for regex annotation '{regex}'.", regex: AnnotationRegex),
        InvalidRangeArgumentsForValueType(34, "Invalid arguments for range annotation '{range}' for value type '{value_type:?}'.", range: AnnotationRange, value_type: Option<ValueType>),
        InvalidValuesArgumentsForValueType(35, "Invalid arguments for values annotation '{values}' for value type '{value_type:?}'.", values: AnnotationValues, value_type: Option<ValueType>),
        SubtypeConstraintDoesNotNarrowSupertypeConstraint(36, "Subtype constraint on '{subtype}' does not narrow supertype constraint on '{supertype}'.", subtype: Label, supertype: Label, (typedb_source: Box<ConstraintError>)),
        CannotUnsetInheritedOwns(37, "Cannot unset inherited 'owns' constraint from '{supertype}' for '{supertype}'.", supertype: Label, subtype: Label),
        CannotUnsetInheritedPlays(38, "Cannot unset inherited 'plays' constraint from '{supertype}' for '{subtype}'.", supertype: Label, subtype: Label),
        CannotUnsetInheritedValueType(39, "Cannot unset inherited value type '{value_type}' for '{subtype}'.", value_type: ValueType, subtype: Label),
        ValueTypeNotCompatibleWithInheritedValueType(40, "Value type '{value_type}' for '{label}' is not compatible with inherited value type '{super_value_type}' of '{super_label}'.", label: Label, value_type: ValueType, super_label: Label, super_value_type: ValueType),
        CannotRedeclareInheritedValueTypeWithoutSpecialisation(41, "Cannot redeclare inherited value type '{value_type}' for '{label}' without specialising inheritance from {super_label}'.", label: Label, super_label: Label, value_type: ValueType),
        AnnotationIsNotCompatibleWithDeclaredAnnotation(42, "Annotation '{annotation}' is not compatible with declared annotation '{declared_annotation}' for '{label}'.", annotation: AnnotationCategory, declared_annotation: AnnotationCategory, label: Label),
        RelationTypeMustRelateAtLeastOneRole(43, "Relation type '{relation}' must relate at least one role.", relation: Label),
        CannotRedeclareInheritedCapabilityWithoutSpecialisation(44, "Cannot redeclare inherited capability '{cap}' from '{supertype}' on subtype '{subtype}' without specialisation.", cap: CapabilityKind, supertype: Label, subtype: Label),
        CannotRedeclareConstraintOnSubtypeWithoutSpecialisation(45, "Cannot redeclare constraint '{constraint}' on subtype '{subtype}' without specialisation.", constraint: ConstraintDescription,  subtype: Label),
        CapabilityConstraintAlreadyExistsForTheWholeInterfaceType(46, "Capability constraint '{constraint}' already exists for the whole interface type '{cap}' on '{label}'.", cap: CapabilityKind, label: Label, constraint: ConstraintDescription),
        ChangingAttributeSupertypeLeadsToImplicitIndependentAnnotationLossAndUnexpectedDataLoss(47, "Changing supertype of attribute '{attribute}' to '{supertype:?}' will lead to implicit loss of independent annotation and potential data loss.", attribute: Label, supertype: Option<Label>),
        CannotDeleteTypeWithExistingInstances(48, "Cannot delete type '{label}' as it has existing instances.", label: Label),
        CannotSetRoleOrderingWithExistingInstances(49, "Cannot set role ordering for '{label}' as it has existing instances.", label: Label),
        CannotSetOwnsOrderingWithExistingInstances(50, "Cannot set 'owns' ordering for '{owner}' owns '{attribute}' as it has existing instances.", owner: Label, attribute: Label),
        CannotUnsetValueTypeWithExistingInstances(51, "Cannot unset value type for '{attribute}' as it has existing instances.", attribute: Label),
        CannotChangeValueTypeWithExistingInstances(52, "Cannot change value type of '{label}' with existing instances", label: Label),

        CannotUnsetCapabilityWithExistingInstances(53, "Cannot unset '{cap} {interface}' of '{label}' when it has existing matching instances.", cap: CapabilityKind, label: Label, interface: Label),
        CannotChangeSupertypeAsCapabilityIsLostWhileHavingHasInstances(
            54,
            "Cannot change supertype of '{label}' to '{new_super_label:?}' as '{cap} {interface}' would be lost, when there are existing matches instances",
            cap: CapabilityKind,
            interface: Label,
            label: Label,
            new_super_label: Option<Label>
        ),
        // TODO: These all need a lot more information!
        CannotAcquireCapabilityAsExistingInstancesViolateItsConstraint(
            55,
            "Cannot add new schema as existing instances would violate the addition.",
            (typedb_source: Box<DataValidationError>)
        ),
        CannotSetAnnotationForCapabilityAsExistingInstancesViolateItsConstraint(
            56,
            "Cannot set annotation as existing instances would violate the new constraint.",
            ( typedb_source: Box<DataValidationError>)
        ),
        CannotChangeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(
            57,
            "Cannot change supertype as the schema constraints would not be compatible with existing instances.",
            (typedb_source: Box<DataValidationError>)
        ),
        CannotChangeInterfaceTypeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(
            58,
            "Cannot change schema as it would result in schema constraint that are compatible with existing instances.",
            ( typedb_source: Box<DataValidationError>)
        ),
        CannotUnsetInterfaceTypeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(
            59,
            "Cannot remove schema element as the result would not be compatible with existing instances.",
            ( typedb_source : Box<DataValidationError> )
        ),
        CannotSetAnnotationAsExistingInstancesViolateItsConstraint(
            60,
            "Cannot set annotation as the result would not be compatible with existing instances.",
            (typedb_source: Box<DataValidationError> )
        ),
        CannotChangeSupertypeAsUpdatedConstraintIsViolatedByExistingInstances(
            61,
            "Cannot change superty as the resulting schema constraints would be not be compatible with existing instances.",
            ( typedb_source: Box<DataValidationError>)
        ),
    }
);

// #[derive(Debug, Clone)]
// pub enum SchemaValidationError {
//     ConceptRead(ConceptReadError),
//     LabelShouldBeUnique {
//         label: Label,
//         existing_kind: Kind,
//     },
//     StructNameShouldBeUnique(String),
//     StructShouldHaveAtLeastOneField(String),
//     StructCannotBeDeletedAsItsUsedAsValueTypeForAttributeTypes(String, usize),
//     StructCannotBeDeletedAsItsUsedAsValueTypeForStructs(String, usize),
//     RoleNameShouldBeUniqueForRelationTypeHierarchy(Label, Label),
//     CycleFoundInTypeHierarchy(Label, Label),
//     ChangingAttributeTypeSupertypeWillLeadToConflictingValueTypes(Label, Option<ValueType>, ValueType),
//     CannotUnsetAbstractnessOfAttributeTypeAsItHasSubtypes(Label),
//     CannotDeleteTypeWithExistingSubtypes(Label),
//     RelatesNotInherited(RelationType, RoleType),
//     RelatesIsAlreadySpecialisedByASupertype(RelationType, Relates),
//     CannotManageAnnotationsForSpecialisingRelates(Relates),
//     AttributeTypeSupertypeIsNotAbstract(Label),
//     AbstractTypesSupertypeHasToBeAbstract(Label, Label),
//     CannotUnsetRelatesAbstractnessAsItIsASpecialisingRelates(Relates),
//     CannotChangeRelationTypeSupertypeAsRelatesSpecialiseIsLost(
//         RelationType,
//         Option<RelationType>,
//         RoleType,
//         RoleType,
//     ),
//     OrderingDoesNotMatchWithSupertype(Label, Label, Ordering, Ordering),
//     OrderingDoesNotMatchWithCapabilityOfSupertypeInterface(
//         Label,
//         Label,
//         Label,
//         Ordering,
//         Ordering,
//     ),
//     SpecialisingRelatesIsNotAbstract(Label, Label),
//     CannotSetOwnsBecauseItIsAlreadySetWithDifferentOrdering(Label, Label, Ordering),
//     InvalidOrderingForDistinctAnnotation(Label, Ordering),
//     AttributeTypeWithoutValueTypeShouldBeAbstract(Label),
//     ValueTypeIsNotCompatibleWithRegexAnnotation(Label, Option<ValueType>),
//     ValueTypeIsNotCompatibleWithRangeAnnotation(Label, Option<ValueType>),
//     ValueTypeIsNotCompatibleWithValuesAnnotation(Label, Option<ValueType>),
//     ValueTypeIsNotKeyableForUniqueConstraintOfKeyAnnotationDeclaredOnOwns(
//         Label,
//         Label,
//         Option<ValueType>,
//     ),
//     ValueTypeIsNotKeyableForUniqueConstraintOfUniqueAnnotationDeclaredOnOwns(
//         Label,
//         Label,
//         Option<ValueType>,
//     ),
//     CannotSetAnnotationToInterfaceBecauseItsConstraintIsNotNarrowedByItsCapabilityConstraint(
//         Label,
//         ConstraintError,
//     ),
//     CannotSetAnnotationToCapabilityBecauseItsConstraintDoesNotNarrowItsInterfaceConstraint(
//         Label,
//         Label,
//         ConstraintError,
//     ),
//     InvalidCardinalityArguments(AnnotationCardinality),
//     InvalidRegexArguments(AnnotationRegex),
//     InvalidRangeArgumentsForValueType(AnnotationRange, Option<ValueType>),
//     InvalidValuesArgumentsForValueType(AnnotationValues, Option<ValueType>),
//     SubtypeConstraintDoesNotNarrowSupertypeConstraint(Label, Label, ConstraintError),
//     CannotUnsetInheritedOwns(Label, Label),
//     CannotUnsetInheritedPlays(Label, Label),
//     CannotUnsetInheritedValueType(ValueType, Label),
//     ValueTypeNotCompatibleWithInheritedValueType(Label, Label, ValueType, ValueType),
//     CannotRedeclareInheritedValueTypeWithoutSpecialisation(Label, Label, ValueType, ValueType),
//     AnnotationIsNotCompatibleWithDeclaredAnnotation(AnnotationCategory, AnnotationCategory, Label),
//     RelationTypeMustRelateAtLeastOneRole(Label),
//     CannotRedeclareInheritedCapabilityWithoutSpecialisation(
//         CapabilityKind,
//         Label,
//         Label,
//         Label,
//     ),
//     CannotRedeclareConstraintOnSubtypeWithoutSpecialisation(
//         Kind,
//         Label,
//         Label,
//         ConstraintDescription,
//     ),
//     CapabilityConstraintAlreadyExistsForTheWholeInterfaceType(
//         CapabilityKind,
//         Label,
//         Label,
//         ConstraintDescription,
//     ),
//     ChangingAttributeSupertypeLeadsToImplicitIndependentAnnotationLossAndUnexpectedDataLoss(
//         Label,
//         Option<Label>,
//         Label,
//     ),
//     CannotDeleteTypeWithExistingInstances(Label),
//     CannotSetRoleOrderingWithExistingInstances(Label),
//     CannotSetOwnsOrderingWithExistingInstances(Label, Label),
//     CannotUnsetValueTypeWithExistingInstances(Label),
//     CannotChangeValueTypeWithExistingInstances(Label),
//     CannotUnsetCapabilityWithExistingInstances(CapabilityKind, Label, Label, Label),
//     CannotChangeSupertypeAsCapabilityIsLostWhileHavingHasInstances(CapabilityKind, Label, Option<Label>, Label, Label),
//     CannotAcquireCapabilityAsExistingInstancesViolateItsConstraint(DataValidationError),
//     CannotSetAnnotationForCapabilityAsExistingInstancesViolateItsConstraint(DataValidationError),
//     CannotChangeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(DataValidationError),
//     CannotChangeInterfaceTypeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(DataValidationError),
//     CannotUnsetInterfaceTypeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(DataValidationError),
//     CannotSetAnnotationAsExistingInstancesViolateItsConstraint(DataValidationError),
//     CannotChangeSupertypeAsUpdatedConstraintIsViolatedByExistingInstances(DataValidationError),
// }
//
// impl fmt::Display for SchemaValidationError {
//     fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         todo!()
//     }
// }
//
// impl Error for SchemaValidationError {
//     fn source(&self) -> Option<&(dyn Error + 'static)> {
//         match self {
//             Self::ConceptRead(source) => Some(source),
//             Self::LabelShouldBeUnique { .. } => None,
//             Self::StructNameShouldBeUnique(_) => None,
//             Self::StructShouldHaveAtLeastOneField(_) => None,
//             Self::StructCannotBeDeletedAsItsUsedAsValueTypeForAttributeTypes(_, _) => None,
//             Self::StructCannotBeDeletedAsItsUsedAsValueTypeForStructs(_, _) => None,
//             Self::RoleNameShouldBeUniqueForRelationTypeHierarchy(_, _) => None,
//             Self::CycleFoundInTypeHierarchy(_, _) => None,
//             Self::ChangingAttributeTypeSupertypeWillLeadToConflictingValueTypes(_, _, _) => None,
//             Self::CannotUnsetAbstractnessOfAttributeTypeAsItHasSubtypes(_) => None,
//             Self::CannotDeleteTypeWithExistingSubtypes(_) => None,
//             Self::RelatesNotInherited(_, _) => None,
//             Self::RelatesIsAlreadySpecialisedByASupertype(_, _) => None,
//             Self::CannotManageAnnotationsForSpecialisingRelates(_) => None,
//             Self::OrderingDoesNotMatchWithSupertype(_, _, _, _) => None,
//             Self::OrderingDoesNotMatchWithCapabilityOfSupertypeInterface(_, _, _, _, _) => None,
//             Self::SpecialisingRelatesIsNotAbstract(_, _) => None,
//             Self::CannotSetOwnsBecauseItIsAlreadySetWithDifferentOrdering(_, _, _) => None,
//             Self::InvalidOrderingForDistinctAnnotation(_, _) => None,
//             Self::AttributeTypeSupertypeIsNotAbstract(_) => None,
//             Self::AbstractTypesSupertypeHasToBeAbstract(_, _) => None,
//             Self::CannotUnsetRelatesAbstractnessAsItIsASpecialisingRelates(_) => None,
//             Self::CannotChangeRelationTypeSupertypeAsRelatesSpecialiseIsLost(_, _, _, _) => None,
//             Self::AttributeTypeWithoutValueTypeShouldBeAbstract(_) => None,
//             Self::ValueTypeIsNotCompatibleWithRegexAnnotation(_, _) => None,
//             Self::ValueTypeIsNotCompatibleWithRangeAnnotation(_, _) => None,
//             Self::ValueTypeIsNotCompatibleWithValuesAnnotation(_, _) => None,
//             Self::ValueTypeIsNotKeyableForUniqueConstraintOfKeyAnnotationDeclaredOnOwns(_, _, _) => None,
//             Self::ValueTypeIsNotKeyableForUniqueConstraintOfUniqueAnnotationDeclaredOnOwns(_, _, _) => None,
//             Self::CannotSetAnnotationToInterfaceBecauseItsConstraintIsNotNarrowedByItsCapabilityConstraint(_, _) => {
//                 None
//             }
//             Self::CannotSetAnnotationToCapabilityBecauseItsConstraintDoesNotNarrowItsInterfaceConstraint(_, _, _) => {
//                 None
//             }
//             Self::InvalidCardinalityArguments(_) => None,
//             Self::InvalidRegexArguments(_) => None,
//             Self::InvalidRangeArgumentsForValueType(_, _) => None,
//             Self::InvalidValuesArgumentsForValueType(_, _) => None,
//             Self::SubtypeConstraintDoesNotNarrowSupertypeConstraint(_, _, _) => None,
//             Self::CannotUnsetInheritedOwns(_, _) => None,
//             Self::CannotUnsetInheritedPlays(_, _) => None,
//             Self::CannotUnsetInheritedValueType(_, _) => None,
//             Self::ValueTypeNotCompatibleWithInheritedValueType(_, _, _, _) => None,
//             Self::CannotRedeclareInheritedValueTypeWithoutSpecialisation(_, _, _, _) => None,
//             Self::AnnotationIsNotCompatibleWithDeclaredAnnotation(_, _, _) => None,
//             Self::RelationTypeMustRelateAtLeastOneRole(_) => None,
//             Self::CannotRedeclareInheritedCapabilityWithoutSpecialisation(_, _, _, _) => None,
//             Self::CannotRedeclareConstraintOnSubtypeWithoutSpecialisation(_, _, _, _) => None,
//             Self::CapabilityConstraintAlreadyExistsForTheWholeInterfaceType(_, _, _, _) => None,
//             Self::ChangingAttributeSupertypeLeadsToImplicitIndependentAnnotationLossAndUnexpectedDataLoss(_, _, _) => {
//                 None
//             }
//             Self::CannotDeleteTypeWithExistingInstances(_) => None,
//             Self::CannotSetRoleOrderingWithExistingInstances(_) => None,
//             Self::CannotSetOwnsOrderingWithExistingInstances(_, _) => None,
//             Self::CannotUnsetValueTypeWithExistingInstances(_) => None,
//             Self::CannotChangeValueTypeWithExistingInstances(_) => None,
//             Self::CannotUnsetCapabilityWithExistingInstances(_, _, _, _) => None,
//             Self::CannotChangeSupertypeAsCapabilityIsLostWhileHavingHasInstances(_, _, _, _, _) => None,
//             Self::CannotAcquireCapabilityAsExistingInstancesViolateItsConstraint(source) => Some(source),
//             Self::CannotSetAnnotationForCapabilityAsExistingInstancesViolateItsConstraint(source) => Some(source),
//             Self::CannotChangeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(source) => {
//                 Some(source)
//             }
//             Self::CannotChangeInterfaceTypeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(
//                 source,
//             ) => Some(source),
//             Self::CannotUnsetInterfaceTypeSupertypeAsUpdatedCapabilityConstraintIsViolatedByExistingInstances(
//                 source,
//             ) => Some(source),
//             Self::CannotSetAnnotationAsExistingInstancesViolateItsConstraint(source) => Some(source),
//             Self::CannotChangeSupertypeAsUpdatedConstraintIsViolatedByExistingInstances(source) => Some(source),
//         }
//     }
// }
